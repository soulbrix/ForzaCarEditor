# cloner_engine.py
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any, Iterable

ENGINE_VERSION = "v0.2.1"

# NOTE:
# - This engine is intentionally conservative. It reads from MAIN + optional DLC SLTs,
#   but it only WRITES to MAIN.
# - Car cloning: clones Data_Car + Data_CarBody base block + all List_Upgrade* rows (car-scoped)
#   + any other tables with Ordinal/CarID scoped rows, with base-block ID rewrites.
# - Engine cloning: conservative by default (Data_Engine + List_Upgrade* rows referencing EngineID),
#   avoids global Combo_* tables that tend to have UNIQUE constraints.


@dataclass
class CloneReport:
    source_db: Path
    target_db: Path
    extra_source_db: Optional[Path]
    source_car_id: int
    new_car_id: int
    year_marker: int
    source_body_id: int
    new_body_id: int
    old_base: int
    new_base: int
    tables_touched: Dict[str, int]  # table -> rows inserted


# -----------------------------
# Low-level DB helpers
# -----------------------------

def _connect(db: Path, readonly: bool = False) -> sqlite3.Connection:
    """SQLite connect helper with safety: never create new DB files."""
    db = Path(db)
    if not db.exists():
        raise FileNotFoundError(f"DB not found: {db}")

    if readonly:
        uri_path = db.resolve().as_posix()
        con = sqlite3.connect(f"file:{uri_path}?mode=ro", uri=True)
    else:
        con = sqlite3.connect(str(db))
    con.row_factory = sqlite3.Row
    return con


def _list_tables(cur: sqlite3.Cursor) -> List[str]:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    return [r[0] for r in cur.fetchall()]


def _table_exists(cur: sqlite3.Cursor, table: str) -> bool:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? AND name NOT LIKE 'sqlite_%' LIMIT 1",
        (table,),
    )
    return cur.fetchone() is not None


def _table_info(cur: sqlite3.Cursor, table: str) -> List[sqlite3.Row]:
    cur.execute(f"PRAGMA table_info('{table}')")
    return cur.fetchall()


def _cols(info: List[sqlite3.Row]) -> List[str]:
    return [r[1] for r in info]


def _safe_int(x) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None


def _has_single_integer_pk_id(info: List[sqlite3.Row]) -> bool:
    """True if table has exactly one PK column named Id (case-sensitive) of type INTEGER."""
    pk_cols = [r for r in info if r[5]]
    if len(pk_cols) != 1:
        return False
    cid, name, typ, notnull, dflt, pkflag = pk_cols[0]
    return name == "Id" and (typ or "").strip().upper() == "INTEGER"


def _insert_row(cur: sqlite3.Cursor, table: str, cols: List[str], vals: List[Any], auto_drop_id: bool = True) -> None:
    """
    Insert row, optionally dropping Id if it's a single INTEGER PRIMARY KEY (autoinc-ish).
    """
    info = _table_info(cur, table)
    cols_t = _cols(info)

    if auto_drop_id and "Id" in cols and _has_single_integer_pk_id(info) and table not in {"Data_Car", "Data_CarBody", "Data_Engine"}:
        i = cols.index("Id")
        cols = cols[:i] + cols[i + 1 :]
        vals = vals[:i] + vals[i + 1 :]

    # Only keep columns that exist in target table
    cols2, vals2 = [], []
    for c, v in zip(cols, vals):
        if c in cols_t:
            cols2.append(c)
            vals2.append(v)

    placeholders = ",".join(["?"] * len(cols2))
    cols_sql = ",".join([f'"{c}"' for c in cols2])
    cur.execute(f'INSERT INTO "{table}" ({cols_sql}) VALUES ({placeholders})', vals2)


def _row_to_target_shape(
    src_row: sqlite3.Row,
    src_cols: List[str],
    tgt_cols: List[str],
) -> Tuple[List[str], List[Any]]:
    src_map = {c: src_row[c] for c in src_cols if c in src_row.keys()}
    ins_cols = [c for c in tgt_cols if c in src_map]
    ins_vals = [src_map[c] for c in ins_cols]
    return ins_cols, ins_vals


def _rewrite_base_ids_in_place(cols: List[str], vals: List[Any], old_base: int, new_base: int) -> None:
    """
    For any *ID/*Ids column (except Ordinal/CarID/EngineID etc), if value is in old_base..old_base+999,
    shift to new_base + offset.
    """
    for i, c in enumerate(cols):
        cl = c.lower()
        if c in ("Ordinal", "CarID", "CarId", "EngineID", "EngineId", "Engine", "ContentID", "OfferID"):
            continue
        if not (cl.endswith("id") or cl.endswith("ids")):
            continue
        vi = _safe_int(vals[i])
        if vi is None:
            continue
        if old_base <= vi < old_base + 1000:
            vals[i] = new_base + (vi - old_base)


def _clone_rows_from_multiple_sources(
    cursors_s: List[sqlite3.Cursor],
    cur_t: sqlite3.Cursor,
    table: str,
    where_col: str,
    where_val: int,
    rewrites: Dict[str, Any],
    old_base: int,
    new_base: int,
    rewrite_base_ids: bool,
    delete_existing_for_target: Optional[Tuple[str, int]] = None,
    extra_where_sql: str = "",
) -> int:
    """
    Clones rows from ALL sources (MAIN + DLC etc), de-duping identical rows.
    This matches the behavior of the stable cloner and prevents missing deps.

    delete_existing_for_target: (col_name, value) => delete rows in target before insert
    extra_where_sql: appended to WHERE clause
    """
    if not _table_exists(cur_t, table):
        return 0

    info_t = _table_info(cur_t, table)
    cols_t = _cols(info_t)

    # delete existing (avoid duplicates)
    if delete_existing_for_target:
        dcol, dval = delete_existing_for_target
        if dcol in cols_t:
            cur_t.execute(f'DELETE FROM "{table}" WHERE "{dcol}"=?', (dval,))

    total = 0
    seen: set[Tuple[Any, ...]] = set()

    for cur_s in cursors_s:
        if not _table_exists(cur_s, table):
            continue
        info_s = _table_info(cur_s, table)
        cols_s = _cols(info_s)
        if where_col not in cols_s:
            continue

        cur_s.execute(
            f'SELECT * FROM "{table}" WHERE "{where_col}"=?{extra_where_sql}',
            (where_val,),
        )
        rows = cur_s.fetchall()
        if not rows:
            continue

        for r in rows:
            ins_cols, ins_vals = _row_to_target_shape(r, cols_s, cols_t)

            # apply explicit rewrites
            for c, v in rewrites.items():
                if c in ins_cols:
                    ins_vals[ins_cols.index(c)] = v
                else:
                    if c in cols_t:
                        ins_cols.append(c)
                        ins_vals.append(v)

            if rewrite_base_ids:
                _rewrite_base_ids_in_place(ins_cols, ins_vals, old_base, new_base)

            # De-dupe signature (values aligned to target column order)
            sig = []
            for c in cols_t:
                if c in ins_cols:
                    sig.append(ins_vals[ins_cols.index(c)])
                else:
                    sig.append(None)
            sig_t = tuple(sig)
            if sig_t in seen:
                continue
            seen.add(sig_t)

            _insert_row(cur_t, table, ins_cols, ins_vals, auto_drop_id=True)
            total += 1

    return total



# -----------------------------
# Car ID / Engine ID suggestions
# -----------------------------

def _max_int_in_column(cur: sqlite3.Cursor, table: str, col: str) -> Optional[int]:
    if not _table_exists(cur, table):
        return None
    cols = _cols(_table_info(cur, table))
    if col not in cols:
        return None
    cur.execute(f'SELECT MAX(CAST("{col}" AS INTEGER)) AS m FROM "{table}"')
    r = cur.fetchone()
    if not r or r["m"] is None:
        return None
    try:
        return int(r["m"])
    except Exception:
        return None


def suggest_next_car_id(main_db: Path, min_id: int = 2000, aux_sources: Optional[List[Path]] = None) -> int:
    """
    Suggests the next available CarID by scanning MAIN and optional auxiliary SLTs (DLC).
    """
    con_m = _connect(main_db, readonly=True)
    cur_m = con_m.cursor()

    max_id = _max_int_in_column(cur_m, "Data_Car", "Id") or 0

    if aux_sources:
        for p in aux_sources:
            try:
                con = _connect(Path(p), readonly=True)
                cur = con.cursor()
                max_id = max(max_id, _max_int_in_column(cur, "Data_Car", "Id") or 0)
                con.close()
            except Exception:
                continue

    con_m.close()
    return max(min_id, max_id + 1)


def suggest_next_engine_id(main_db: Path, min_id: int = 2000, aux_sources: Optional[List[Path]] = None) -> int:
    con_m = _connect(main_db, readonly=True)
    cur_m = con_m.cursor()
    max_id = _max_int_in_column(cur_m, "Data_Engine", "Id") or _max_int_in_column(cur_m, "Data_Engine", "EngineID") or 0

    if aux_sources:
        for p in aux_sources:
            try:
                con = _connect(Path(p), readonly=True)
                cur = con.cursor()
                max_id = max(max_id,
                             _max_int_in_column(cur, "Data_Engine", "Id") or _max_int_in_column(cur, "Data_Engine", "EngineID") or 0)
                con.close()
            except Exception:
                continue

    con_m.close()
    return max(min_id, max_id + 1)


# -----------------------------
# Car cloning
# -----------------------------

# -----------------------------
# Combo_* scoped cloning helpers
# -----------------------------

def _max_int(cur: sqlite3.Cursor, table: str, col: str) -> int:
    try:
        cur.execute(f'SELECT MAX("{col}") AS m FROM "{table}"')
        r = cur.fetchone()
        if not r or r["m"] is None:
            return 0
        return int(r["m"])
    except Exception:
        return 0


def _clone_combo_rows_for_car(
    cursors_sources: List[sqlite3.Cursor],
    cur_t: sqlite3.Cursor,
    table: str,
    pk_col: str,
    car_scope_col: str,
    source_car_id: int,
    new_car_id: int,
    extra_rewrites: Optional[Dict[str, Any]] = None,
) -> int:
    """Clone per-car rows from a Combo_* table, allocating new unique PK values."""
    if not _table_exists(cur_t, table):
        return 0

    cols_t = _cols(_table_info(cur_t, table))
    if pk_col not in cols_t or car_scope_col not in cols_t:
        return 0

    # fetch donor rows from first source that has them
    donor_rows: List[sqlite3.Row] = []
    src_cols: List[str] = []
    for cur_s in cursors_sources:
        try:
            if not _table_exists(cur_s, table):
                continue
            cols_s = _cols(_table_info(cur_s, table))
            if car_scope_col not in cols_s:
                continue
            cur_s.execute(f'SELECT * FROM "{table}" WHERE "{car_scope_col}"=?', (source_car_id,))
            donor_rows = cur_s.fetchall()
            if donor_rows:
                src_cols = cols_s
                break
        except Exception:
            continue

    if not donor_rows:
        return 0

    # remove existing rows for new car to avoid duplicates
    cur_t.execute(f'DELETE FROM "{table}" WHERE "{car_scope_col}"=?', (new_car_id,))

    next_pk = _max_int(cur_t, table, pk_col) + 1
    n = 0
    for r in donor_rows:
        ins_cols, ins_vals = _row_to_target_shape(r, src_cols, cols_t)

        # rewrite scope
        if car_scope_col in ins_cols:
            ins_vals[ins_cols.index(car_scope_col)] = new_car_id
        else:
            ins_cols.append(car_scope_col)
            ins_vals.append(new_car_id)

        # allocate new PK
        if pk_col in ins_cols:
            ins_vals[ins_cols.index(pk_col)] = next_pk
        else:
            ins_cols.append(pk_col)
            ins_vals.append(next_pk)

        # any extra rewrites (EngineID etc.)
        if extra_rewrites:
            for k, v in extra_rewrites.items():
                if k in cols_t:
                    if k in ins_cols:
                        ins_vals[ins_cols.index(k)] = v
                    else:
                        ins_cols.append(k)
                        ins_vals.append(v)

        _insert_row(cur_t, table, ins_cols, ins_vals, auto_drop_id=False)
        next_pk += 1
        n += 1

    return n


def _find_carbody_id(cur: sqlite3.Cursor, car_id: int) -> Optional[int]:
    if not _table_exists(cur, "Data_CarBody"):
        return None
    cols = _cols(_table_info(cur, "Data_CarBody"))
    if "Id" not in cols:
        return None
    base = car_id * 1000
    cur.execute('SELECT "Id" AS i FROM "Data_CarBody" WHERE "Id">=? AND "Id"<? ORDER BY "Id" LIMIT 1', (base, base + 1000))
    r = cur.fetchone()
    return int(r["i"]) if r and r["i"] is not None else None

def _clone_rows(
    cursors_s: List[sqlite3.Cursor],
    cur_t: sqlite3.Cursor,
    table: str,
    where_col: str,
    where_val: int,
    rewrites: Dict[str, Any],
    old_base: int,
    new_base: int,
    extra_where: str = "",
    delete_existing_for_target: Optional[Tuple[str, int]] = None,
) -> int:
    """
    Clone rows for table where where_col==where_val from the first source cursor that has them,
    insert into target while applying rewrites and shifting base-block IDs.
    """
    if not _table_exists(cur_t, table):
        return 0

    tgt_cols = _cols(_table_info(cur_t, table))

    # delete target rows first to avoid doubled upgrades etc.
    if delete_existing_for_target:
        col, v = delete_existing_for_target
        if col in tgt_cols:
            cur_t.execute(f'DELETE FROM "{table}" WHERE "{col}"=?', (v,))

    rows: List[sqlite3.Row] = []
    src_cols: List[str] = []

    for cur_s in cursors_s:
        if not _table_exists(cur_s, table):
            continue
        info_s = _table_info(cur_s, table)
        src_cols = _cols(info_s)
        if where_col not in src_cols:
            continue

        cur_s.execute(
            f'SELECT * FROM "{table}" WHERE "{where_col}"=?{extra_where}',
            (where_val,),
        )
        rows = cur_s.fetchall()
        if rows:
            break

    if not rows:
        return 0

    n = 0
    for r in rows:
        ic, iv = _row_to_target_shape(r, src_cols, tgt_cols)

        # apply explicit rewrites (Ordinal/CarID/etc.)
        for k, v in rewrites.items():
            if k in ic:
                iv[ic.index(k)] = v
            else:
                ic.append(k)
                iv.append(v)

        # shift base-block IDs (oldCar*1000 -> newCar*1000)
        _rewrite_base_ids_in_place(ic, iv, old_base, new_base)

        _insert_row(cur_t, table, ic, iv, auto_drop_id=True)
        n += 1

    return n


def clone_car_between(
    source_db: Path,
    target_db: Path,
    source_car_id: int,
    new_car_id: int,
    year_marker: int = 6969,
    extra_source_db: Optional[Path] = None,              # compat
    all_source_paths: Optional[List[Path]] = None,        # compat
) -> CloneReport:
    """
    FM4-safe car clone into MAIN (target_db).

    Key goals:
    - Always write to MAIN only.
    - Clone car-scoped upgrade data (List_Upgrade*), plus Combo_Colors / Combo_Engines.
    - Clone the donor Data_CarBody *from whichever SLT actually contains it* (MAIN or DLC).
      This is critical: DLC cars often have their CarBody rows in MAIN, not in the DLC SLT.
    - Special fix: List_UpgradeCarBody -> copy ONLY stock row (IsStock=1 & Level=0) to avoid cockpit/camera issues.
    - Adds ContentOffersMapping row if present (ID/ContentID/OfferID), as requested.

    If the donor Data_CarBody block cannot be found in any source, the clone is aborted to avoid creating
    blank / crashing cars in-game.
    """
    source_db = Path(source_db)
    target_db = Path(target_db)
    extra_source_db = Path(extra_source_db) if extra_source_db else None

    con_s = _connect(source_db, readonly=True)
    cur_s = con_s.cursor()
    con_t = _connect(target_db, readonly=False)
    cur_t = con_t.cursor()

    # Build a "search set" of read-only source cursors:
    # - donor SLT
    # - optional extra_source_db (usually MAIN when cloning DLC)
    # - optional all_source_paths (all discovered SLTs)
    cursors_sources: List[sqlite3.Cursor] = [cur_s]
    extra_cons: List[sqlite3.Connection] = []

    con_x: Optional[sqlite3.Connection] = None
    if extra_source_db and extra_source_db.resolve() != source_db.resolve():
        con_x = _connect(extra_source_db, readonly=True)
        cursors_sources.append(con_x.cursor())

    if all_source_paths:
        for p in all_source_paths:
            try:
                pp = Path(p)
                if pp.resolve() in {source_db.resolve(), target_db.resolve()}:
                    continue
                if extra_source_db and pp.resolve() == extra_source_db.resolve():
                    continue
                con_a = _connect(pp, readonly=True)
                cursors_sources.append(con_a.cursor())
                extra_cons.append(con_a)
            except Exception:
                continue

    if not _table_exists(cur_t, "Data_Car"):
        raise ValueError("MAIN has no Data_Car table.")
    if not _table_exists(cur_s, "Data_Car"):
        raise ValueError(f"{source_db.name} has no Data_Car table.")

    # Ensure new id is free
    cur_t.execute('SELECT 1 FROM "Data_Car" WHERE "Id"=? LIMIT 1', (new_car_id,))
    if cur_t.fetchone():
        raise ValueError(f"MAIN already contains CarID {new_car_id}.")

    # --- clone Data_Car row
    cur_s.execute('SELECT * FROM "Data_Car" WHERE "Id"=?', (source_car_id,))
    donor = cur_s.fetchone()
    if not donor:
        raise ValueError(f"Source CarID {source_car_id} not found in {source_db.name}.")

    cols_s_car = _cols(_table_info(cur_s, "Data_Car"))
    cols_t_car = _cols(_table_info(cur_t, "Data_Car"))
    ic, iv = _row_to_target_shape(donor, cols_s_car, cols_t_car)

    if "Id" in ic:
        iv[ic.index("Id")] = new_car_id
    else:
        ic.append("Id"); iv.append(new_car_id)

    # Year marker: FM4 has both patterns across builds (ModelYear vs Year)
    if "ModelYear" in cols_t_car:
        if "ModelYear" in ic:
            iv[ic.index("ModelYear")] = year_marker
        else:
            ic.append("ModelYear"); iv.append(year_marker)
    elif "Year" in cols_t_car:
        if "Year" in ic:
            iv[ic.index("Year")] = year_marker
        else:
            ic.append("Year"); iv.append(year_marker)

    _insert_row(cur_t, "Data_Car", ic, iv, auto_drop_id=False)

    old_base = source_car_id * 1000
    new_base = new_car_id * 1000

    tables_touched: Dict[str, int] = {"Data_Car": 1}

    # --- clone Data_CarBody base-block (from ANY source that contains it)
    if _table_exists(cur_t, "Data_CarBody"):
        tgt_cols_body = _cols(_table_info(cur_t, "Data_CarBody"))
        if "Id" in tgt_cols_body:
            # clear any pre-existing block
            cur_t.execute('DELETE FROM "Data_CarBody" WHERE "Id">=? AND "Id"<?', (new_base, new_base + 1000))

            body_rows: List[sqlite3.Row] = []
            src_cols_body: List[str] = []
            for cs in cursors_sources:
                if not _table_exists(cs, "Data_CarBody"):
                    continue
                src_cols_body = _cols(_table_info(cs, "Data_CarBody"))
                if "Id" not in src_cols_body:
                    continue
                cs.execute('SELECT * FROM "Data_CarBody" WHERE "Id">=? AND "Id"<? ORDER BY "Id"', (old_base, old_base + 1000))
                body_rows = cs.fetchall()
                if body_rows:
                    break

            if not body_rows:
                # Abort instead of creating a blank car.
                raise ValueError(
                    "Could not find donor Data_CarBody rows in any loaded SLT. "
                    "This will create a blank/crashing car in-game. "
                    "Make sure you selected the correct MAIN + DLC folder and try again."
                )

            for r in body_rows:
                ic2, iv2 = _row_to_target_shape(r, src_cols_body, tgt_cols_body)
                if "Id" in ic2:
                    iv2[ic2.index("Id")] = new_base + (int(r["Id"]) - old_base)
                _rewrite_base_ids_in_place(ic2, iv2, old_base, new_base)
                _insert_row(cur_t, "Data_CarBody", ic2, iv2, auto_drop_id=False)

            tables_touched["Data_CarBody"] = len(body_rows)
    # --- clone List_Upgrade* tables (car- and body-scoped, merged across MAIN+DLC)
    body_cols = ["CarBodyID", "CarBodyId", "CarbodyId"]

    # Discover donor CarBodyID from the stock row in List_UpgradeCarBody when possible
    source_body_id = old_base
    for cs in cursors_sources:
        if not _table_exists(cs, "List_UpgradeCarBody"):
            continue
        cols_ucb = _cols(_table_info(cs, "List_UpgradeCarBody"))
        bc = next((c for c in body_cols if c in cols_ucb), None)
        if not bc:
            continue
        if "Ordinal" in cols_ucb and "Level" in cols_ucb:
            cs.execute(f'SELECT "{bc}" AS b FROM "List_UpgradeCarBody" WHERE "Ordinal"=? AND "Level"=0 LIMIT 1', (source_car_id,))
            rr = cs.fetchone()
            if rr and rr["b"] is not None:
                source_body_id = int(rr["b"])
                break
    new_body_id = new_base

    for table in _list_tables(cur_t):
        tl = table.lower()
        if not tl.startswith("list_upgrade"):
            continue

        cols_tt = _cols(_table_info(cur_t, table))

        # Special-case: List_UpgradeCarBody => ONLY stock row (cockpit/camera stability)
        extra_where = ""
        if tl == "list_upgradecarbody":
            if "IsStock" in cols_tt and "Level" in cols_tt:
                extra_where = ' AND ("IsStock"=1 AND "Level"=0)'
            elif "Level" in cols_tt:
                extra_where = ' AND ("Level"=0)'

        # Prefer cloning by car scope when possible
        scope_col = None
        if "Ordinal" in cols_tt:
            scope_col = "Ordinal"
        elif "CarID" in cols_tt:
            scope_col = "CarID"
        elif "CarId" in cols_tt:
            scope_col = "CarId"

        if scope_col:
            # Rewrite car scope + any CarBodyID columns if present
            rew = {scope_col: new_car_id}
            for bc in body_cols:
                if bc in cols_tt:
                    rew[bc] = new_body_id

            n = _clone_rows_from_multiple_sources(
                cursors_s=cursors_sources,
                cur_t=cur_t,
                table=table,
                where_col=scope_col,
                where_val=source_car_id,
                rewrites=rew,
                old_base=old_base,
                new_base=new_base,
                rewrite_base_ids=True,
                delete_existing_for_target=(scope_col, new_car_id),
                extra_where_sql=extra_where,
            )
            if n:
                tables_touched[table] = tables_touched.get(table, 0) + n
            continue

        # Otherwise clone by CarBodyID when a body scope exists
        bc = next((c for c in body_cols if c in cols_tt), None)
        if bc:
            n = _clone_rows_from_multiple_sources(
                cursors_s=cursors_sources,
                cur_t=cur_t,
                table=table,
                where_col=bc,
                where_val=source_body_id,
                rewrites={bc: new_body_id},
                old_base=old_base,
                new_base=new_base,
                rewrite_base_ids=True,
                delete_existing_for_target=(bc, new_body_id),
                extra_where_sql="",
            )
            if n:
                tables_touched[table] = tables_touched.get(table, 0) + n

    # --- clone other car-scoped Ordinal/CarID tables (non-upgrade dependencies)
    # These are critical for things like AntiSwayPhysics, SpringDamperPhysics, camera/physics blocks, etc.
    # We intentionally skip risky/global tables.
    skip_prefixes = ("event", "combo_")
    skip_exact = {"EventParticipants"}

    for table in _list_tables(cur_t):
        tl = table.lower()
        if tl.startswith(skip_prefixes) or table in skip_exact:
            continue
        if tl.startswith("list_upgrade"):
            continue
        if table in ("Data_Car", "Data_CarBody", "Data_Engine", "ContentOffersMapping"):
            continue

        cols_tt = _cols(_table_info(cur_t, table))

        scope_col = None
        if "Ordinal" in cols_tt:
            scope_col = "Ordinal"
        elif "CarID" in cols_tt:
            scope_col = "CarID"
        elif "CarId" in cols_tt:
            scope_col = "CarId"

        if not scope_col:
            continue

        # Only consider List_* and Data_* tables to reduce risk of cloning global gameplay tables
        if not (tl.startswith("list_") or tl.startswith("data_")):
            continue

        n = _clone_rows_from_multiple_sources(
            cursors_s=cursors_sources,
            cur_t=cur_t,
            table=table,
            where_col=scope_col,
            where_val=source_car_id,
            rewrites={scope_col: new_car_id},
            old_base=old_base,
            new_base=new_base,
            rewrite_base_ids=True,
            delete_existing_for_target=(scope_col, new_car_id),
        )
        if n:
            tables_touched[table] = tables_touched.get(table, 0) + n
    
    # --- explicit per-car dependency tables that do NOT start with List_/Data_
    # These have been observed to be required for the car to be selectable / not blank in-game.
    extra_dep_tables = ["CameraOverrides", "CarExceptions", "CarPartPositions"]
    for tname in extra_dep_tables:
        if not _table_exists(cur_t, tname):
            continue
        cols_tt = _cols(_table_info(cur_t, tname))
        scope_col = None
        if "CarID" in cols_tt:
            scope_col = "CarID"
        elif "CarId" in cols_tt:
            scope_col = "CarId"
        elif "Ordinal" in cols_tt:
            scope_col = "Ordinal"
        if not scope_col:
            continue

        n = _clone_rows_from_multiple_sources(
            cursors_s=cursors_sources,
            cur_t=cur_t,
            table=tname,
            where_col=scope_col,
            where_val=source_car_id,
            rewrites={scope_col: new_car_id},
            old_base=old_base,
            new_base=new_base,
            rewrite_base_ids=True,
            delete_existing_for_target=(scope_col, new_car_id),
            extra_where_sql="",
        )
        if n:
            tables_touched[tname] = tables_touched.get(tname, 0) + n

# --- Combo_Colors (per-car)
    # FM4 pattern: IDs live in the car base-block (CarID*1000 + offset), e.g. 2000001,2000002,...
    if _table_exists(cur_t, "Combo_Colors"):
        cols_tc = _cols(_table_info(cur_t, "Combo_Colors"))
        pkc = "Id" if "Id" in cols_tc else ("ID" if "ID" in cols_tc else None)
        if pkc and "Ordinal" in cols_tc:
            donor_rows: List[sqlite3.Row] = []
            cols_sc: List[str] = []
            for cs in cursors_sources:
                if not _table_exists(cs, "Combo_Colors"):
                    continue
                cols_sc = _cols(_table_info(cs, "Combo_Colors"))
                if "Ordinal" not in cols_sc:
                    continue
                cs.execute('SELECT * FROM "Combo_Colors" WHERE "Ordinal"=? ORDER BY "{}"'.format(pkc), (source_car_id,))
                donor_rows = cs.fetchall()
                if donor_rows:
                    break

            # clear any existing block for this car
            cur_t.execute(f'DELETE FROM "Combo_Colors" WHERE "{pkc}">=? AND "{pkc}"<?', (new_car_id*1000, new_car_id*1000 + 1000))
            cur_t.execute('DELETE FROM "Combo_Colors" WHERE "Ordinal"=?', (new_car_id,))

            for r in donor_rows:
                ic3, iv3 = _row_to_target_shape(r, cols_sc, cols_tc)

                # scope
                if "Ordinal" in ic3:
                    iv3[ic3.index("Ordinal")] = new_car_id
                else:
                    ic3.append("Ordinal"); iv3.append(new_car_id)

                # FM4 base-block PK allocation
                donor_pk = int(r[pkc]) if pkc in r.keys() and r[pkc] is not None else None
                if donor_pk is not None:
                    off = donor_pk - (source_car_id * 1000)
                    if 0 <= off < 1000:
                        newpk = (new_car_id * 1000) + off
                    else:
                        newpk = (new_car_id * 1000) + 1
                else:
                    newpk = (new_car_id * 1000) + 1

                if pkc in ic3:
                    iv3[ic3.index(pkc)] = newpk
                else:
                    ic3.append(pkc); iv3.append(newpk)

                _insert_row(cur_t, "Combo_Colors", ic3, iv3, auto_drop_id=False)

            if donor_rows:
                tables_touched["Combo_Colors"] = len(donor_rows)

    # --- Combo_Engines (per-car) - allocate new PKs (conservative)
    if _table_exists(cur_t, "Combo_Engines"):
        cols_te = _cols(_table_info(cur_t, "Combo_Engines"))
        pke = "EngineComboID" if "EngineComboID" in cols_te else ("Id" if "Id" in cols_te else ("ID" if "ID" in cols_te else None))
        if pke and "Ordinal" in cols_te:
            donor_rows: List[sqlite3.Row] = []
            cols_se: List[str] = []
            for cs in cursors_sources:
                if not _table_exists(cs, "Combo_Engines"):
                    continue
                cols_se = _cols(_table_info(cs, "Combo_Engines"))
                if "Ordinal" not in cols_se:
                    continue
                cs.execute('SELECT * FROM "Combo_Engines" WHERE "Ordinal"=?', (source_car_id,))
                donor_rows = cs.fetchall()
                if donor_rows:
                    break

            for r in donor_rows:
                ic4, iv4 = _row_to_target_shape(r, cols_se, cols_te)
                if "Ordinal" in ic4:
                    iv4[ic4.index("Ordinal")] = new_car_id
                else:
                    ic4.append("Ordinal"); iv4.append(new_car_id)
                cur_t.execute(f'SELECT MAX("{pke}") AS m FROM "Combo_Engines"')
                newpk = int(cur_t.fetchone()["m"] or 0) + 1
                if pke in ic4:
                    iv4[ic4.index(pke)] = newpk
                else:
                    ic4.append(pke); iv4.append(newpk)
                _insert_row(cur_t, "Combo_Engines", ic4, iv4, auto_drop_id=False)

            if donor_rows:
                tables_touched["Combo_Engines"] = len(donor_rows)

    # --- ContentOffersMapping insert
    if _table_exists(cur_t, "ContentOffersMapping"):
        cols_cm = _cols(_table_info(cur_t, "ContentOffersMapping"))
        if {"ID", "ContentID", "OfferID"}.issubset(cols_cm):
            cur_t.execute('DELETE FROM "ContentOffersMapping" WHERE "ID"=?', (new_car_id,))
            _insert_row(
                cur_t,
                "ContentOffersMapping",
                ["ID", "ContentID", "OfferID"],
                [new_car_id, new_car_id, 5571807128695127040],
                auto_drop_id=False,
            )
            tables_touched["ContentOffersMapping"] = 1
        elif {"Id", "ContentId", "OfferId"}.issubset(cols_cm):
            cur_t.execute('DELETE FROM "ContentOffersMapping" WHERE "ContentId"=?', (new_car_id,))
            cols_ins = ["Id", "ContentId", "OfferId"]
            vals_ins = [new_car_id, new_car_id, 5571807128695127040]
            if "ContentType" in cols_cm:
                cols_ins.append("ContentType")
                vals_ins.append(1)
            _insert_row(cur_t, "ContentOffersMapping", cols_ins, vals_ins, auto_drop_id=False)
            tables_touched["ContentOffersMapping"] = 1

    con_t.commit()

    con_s.close()
    if con_x:
        con_x.close()
    for c in extra_cons:
        c.close()
    con_t.close()

    return CloneReport(
        source_db=source_db,
        target_db=target_db,
        extra_source_db=extra_source_db,
        source_car_id=source_car_id,
        new_car_id=new_car_id,
        year_marker=year_marker,
        source_body_id=old_base,
        new_body_id=new_base,
        old_base=old_base,
        new_base=new_base,
        tables_touched=dict(sorted(tables_touched.items(), key=lambda kv: (-kv[1], kv[0]))),
    )



def _clone_torque_curves_for_engine(
    cursors_sources: List[sqlite3.Cursor],
    cur_t: sqlite3.Cursor,
    source_engine_id: int,
    new_engine_id: int,
) -> int:
    """
    Clone torque curves referenced by the donor engine's upgrade rows.

    IMPORTANT:
    We do NOT assume TorqueCurveID = EngineID*100 or *1000.
    Instead:
      1) Scan all List_Upgrade* rows for source_engine_id and collect any TorqueCurve*ID values.
      2) For each referenced curve ID, if it looks like it's in the donor engine block (either *1000 or *100),
         remap it to the new engine's block using the same offset.
      3) Copy the List_TorqueCurve rows for those IDs into MAIN using the remapped ID.
      4) Rewrite torque curve references in the newly-cloned upgrade rows (EngineID = new_engine_id).

    This matches the behavior you manually validated (adding 2000000,2000002,... makes cars work).
    """
    if not _table_exists(cur_t, "List_TorqueCurve"):
        return 0

    # Detect torque curve ID column name in MAIN
    cols_tc_main = _cols(_table_info(cur_t, "List_TorqueCurve"))
    tc_id_col = None
    for cand in ("TorqueCurveID", "TorqueCurveId", "Id", "ID"):
        if cand in cols_tc_main:
            tc_id_col = cand
            break
    if not tc_id_col:
        return 0

    # 1) Collect referenced torque curve IDs from donor engine upgrade rows (across all sources)
    referenced: set[int] = set()

    # We scan ANY List_Upgrade* table that has an engine ref column AND any TorqueCurve*ID column(s)
    engine_ref_cols = ["EngineID", "EngineId", "Engine", "EngineDataID", "Data_EngineID", "Data_EngineId"]

    # Use target table list as a "schema anchor"
    for table in _list_tables(cur_t):
        if not table.lower().startswith("list_upgrade"):
            continue

        # find a source that has this table & columns
        src_cols = None
        eng_col = None
        tc_cols = None

        for cs in cursors_sources:
            if not _table_exists(cs, table):
                continue
            cols_s = _cols(_table_info(cs, table))
            ec = next((c for c in engine_ref_cols if c in cols_s), None)
            if not ec:
                continue
            tcc = [c for c in cols_s if ("torquecurve" in c.lower() and c.lower().endswith("id"))]
            if not tcc:
                continue
            src_cols = cols_s
            eng_col = ec
            tc_cols = tcc
            break

        if not src_cols or not eng_col or not tc_cols:
            continue

        for cs in cursors_sources:
            if not _table_exists(cs, table):
                continue
            cols_s = _cols(_table_info(cs, table))
            if eng_col not in cols_s:
                continue
            # only pick torque curve columns that exist in this cursor's table
            tcc = [c for c in tc_cols if c in cols_s]
            if not tcc:
                continue

            try:
                sel = ",".join([f'"{c}"' for c in tcc])
                cs.execute(
                    f'SELECT {sel} FROM "{table}" WHERE "{eng_col}"=?',
                    (source_engine_id,),
                )
                for r in cs.fetchall():
                    for c in tcc:
                        v = _safe_int(r[c])
                        if v is not None and v > 0:
                            referenced.add(v)
            except Exception:
                continue

    if not referenced:
        return 0

    # 2) Build mapping old_curve_id -> new_curve_id
    # Support both block styles:
    #  - EngineID*1000 + offset (0..999)
    #  - EngineID*100  + offset (0..99)
    map_old_to_new: Dict[int, int] = {}
    for old_id in sorted(referenced):
        off1000 = old_id - (source_engine_id * 1000)
        if 0 <= off1000 < 1000:
            map_old_to_new[old_id] = (new_engine_id * 1000) + off1000
            continue

        off100 = old_id - (source_engine_id * 100)
        if 0 <= off100 < 100:
            map_old_to_new[old_id] = (new_engine_id * 100) + off100
            continue

        # Not in a recognizable donor block; keep as-is (some curves are global/shared)
        map_old_to_new[old_id] = old_id

    # 3) Copy List_TorqueCurve rows for these IDs into MAIN (with remapped IDs)
    # Delete any destination IDs first to avoid UNIQUE constraint issues
    dest_ids = sorted(set(map_old_to_new.values()))
    # Only delete IDs that are within the new engine blocks (avoid deleting global curves)
    delete_ids = [nid for oid, nid in map_old_to_new.items() if nid != oid and (nid // 100 == new_engine_id or nid // 1000 == new_engine_id)]
    if delete_ids:
        # chunked deletes to avoid SQLite parameter limits
        for i in range(0, len(delete_ids), 400):
            chunk = delete_ids[i:i+400]
            ph = ",".join(["?"] * len(chunk))
            cur_t.execute(f'DELETE FROM "List_TorqueCurve" WHERE "{tc_id_col}" IN ({ph})', chunk)

    inserted = 0
    seen_insert: set[int] = set()

    for old_id in sorted(map_old_to_new.keys()):
        new_id = map_old_to_new[old_id]
        if new_id in seen_insert:
            continue

        # find the donor row across sources
        donor_row = None
        donor_cols = None
        for cs in cursors_sources:
            if not _table_exists(cs, "List_TorqueCurve"):
                continue
            cols_s = _cols(_table_info(cs, "List_TorqueCurve"))
            if tc_id_col not in cols_s:
                continue
            try:
                cs.execute(f'SELECT * FROM "List_TorqueCurve" WHERE "{tc_id_col}"=? LIMIT 1', (old_id,))
                rr = cs.fetchone()
                if rr:
                    donor_row = rr
                    donor_cols = cols_s
                    break
            except Exception:
                continue

        if not donor_row or not donor_cols:
            # Missing curve row — this is exactly what crashes the game.
            # We keep going so we can clone what we can, but caller should treat this as critical.
            continue

        ic, iv = _row_to_target_shape(donor_row, donor_cols, cols_tc_main)

        if tc_id_col in ic:
            iv[ic.index(tc_id_col)] = new_id
        else:
            ic.append(tc_id_col)
            iv.append(new_id)

        _insert_row(cur_t, "List_TorqueCurve", ic, iv, auto_drop_id=False)
        seen_insert.add(new_id)
        inserted += 1

    # 4) Rewrite torque curve references in MAIN upgrade rows for the NEW engine only
    # This ensures we don't touch other engines/cars.
    for table in _list_tables(cur_t):
        if not table.lower().startswith("list_upgrade"):
            continue
        cols = _cols(_table_info(cur_t, table))

        eng_col = next((c for c in engine_ref_cols if c in cols), None)
        if not eng_col:
            continue

        tc_cols = [c for c in cols if ("torquecurve" in c.lower() and c.lower().endswith("id"))]
        if not tc_cols:
            continue

        for c in tc_cols:
            # For each mapping pair, update only rows of the newly cloned engine
            for old_id, new_id in map_old_to_new.items():
                if old_id == new_id:
                    continue
                cur_t.execute(
                    f'UPDATE "{table}" SET "{c}"=? WHERE "{eng_col}"=? AND "{c}"=?',
                    (new_id, new_engine_id, old_id),
                )

    return inserted



def clone_engine_to_main(
    source_db: Path,
    main_db: Path,
    source_engine_id: int,
    new_engine_id: int,
    all_source_paths: Optional[List[Path]] = None,
) -> int:
    """
    Conservative engine clone (reduced scope):
    - clones ONLY the Data_Engine row
    - clones ONLY List_Upgrade* rows that reference the donor EngineID
    - rewrites base-block IDs inside those cloned rows (old_base..old_base+999 -> new_base..)
    This avoids global tables like Combo_* that cause UNIQUE constraint failures.
    """
    old_base = source_engine_id * 1000
    new_base = new_engine_id * 1000

    con_s = _connect(Path(source_db), readonly=True)
    con_t = _connect(Path(main_db), readonly=False)
    cur_s = con_s.cursor()
    cur_t = con_t.cursor()

    tables_s = set(_list_tables(cur_s))
    tables_t = set(_list_tables(cur_t))

    if "Data_Engine" not in tables_s:
        con_s.close(); con_t.close()
        raise ValueError(f"{Path(source_db).name} has no Data_Engine table.")
    if "Data_Engine" not in tables_t:
        con_s.close(); con_t.close()
        raise ValueError("MAIN has no Data_Engine table.")

    cols_s = _cols(_table_info(cur_s, "Data_Engine"))
    cols_t = _cols(_table_info(cur_t, "Data_Engine"))

    id_col_s = "Id" if "Id" in cols_s else ("EngineID" if "EngineID" in cols_s else ("EngineId" if "EngineId" in cols_s else None))
    id_col_t = "Id" if "Id" in cols_t else ("EngineID" if "EngineID" in cols_t else ("EngineId" if "EngineId" in cols_t else None))
    if not id_col_s or not id_col_t:
        con_s.close(); con_t.close()
        raise ValueError("Could not find Id/EngineID column in Data_Engine (source or MAIN).")

    cur_s.execute(f'SELECT * FROM "Data_Engine" WHERE "{id_col_s}"=?', (source_engine_id,))
    row = cur_s.fetchone()
    if not row:
        con_s.close(); con_t.close()
        raise ValueError(f"Source engine {source_engine_id} not found in {Path(source_db).name}.")

    cur_t.execute(f'SELECT COUNT(*) AS c FROM "Data_Engine" WHERE "{id_col_t}"=?', (new_engine_id,))
    if int(cur_t.fetchone()["c"] or 0) != 0:
        con_s.close(); con_t.close()
        raise ValueError(f"MAIN already contains EngineID {new_engine_id}.")

    # insert Data_Engine
    src_map = {c: row[c] for c in cols_s if c in row.keys()}
    insert_cols = [c for c in cols_t if c in src_map]
    insert_vals = [src_map[c] for c in insert_cols]
    if id_col_t in insert_cols:
        insert_vals[insert_cols.index(id_col_t)] = new_engine_id
    else:
        insert_cols.append(id_col_t); insert_vals.append(new_engine_id)

    _insert_row(cur_t, "Data_Engine", insert_cols, insert_vals, auto_drop_id=False)

    # Clone ONLY List_Upgrade* rows that reference this engine
    engine_ref_cols = ["EngineID", "EngineId", "Engine", "EngineDataID", "Data_EngineID", "Data_EngineId"]
    # Sources for these rows:
    # - donor SLT
    # - MAIN (some DLC additions live in MAIN already)
    # - optional other SLTs (if provided)
    aux_sources = [cur_s, cur_t]
    extra_cons: List[sqlite3.Connection] = []
    if all_source_paths:
        for p in all_source_paths:
            try:
                pp = Path(p)
                if pp.resolve() in {Path(source_db).resolve(), Path(main_db).resolve()}:
                    continue
                con_a = _connect(pp, readonly=True)
                aux_sources.append(con_a.cursor())
                extra_cons.append(con_a)
            except Exception:
                continue




    for table in sorted(tables_t):

        # Skip global combo tables (they are shared lookups and often have UNIQUE PKs)
        if table.lower().startswith("combo_"):
            continue

        if table.lower() == "list_upgradeengine":
            continue

        if not table.lower().startswith("list_upgrade"):
            continue

        cols = _cols(_table_info(cur_t, table))
        ref_col = next((c for c in engine_ref_cols if c in cols), None)
        if not ref_col:
            continue

        _clone_rows_from_multiple_sources(
            cursors_s=aux_sources,
            cur_t=cur_t,
            table=table,
            where_col=ref_col,
            where_val=source_engine_id,
            rewrites={ref_col: new_engine_id},
            old_base=old_base,
            new_base=new_base,
            rewrite_base_ids=True,
            delete_existing_for_target=(ref_col, new_engine_id),
        )

    # Clone torque curves referenced by the engine upgrade rows we just cloned
    _clone_torque_curves_for_engine(aux_sources, cur_t, source_engine_id, new_engine_id)

    con_t.commit()
    con_s.close()
    for c in extra_cons:
        c.close()
    con_t.close()
    return 1
