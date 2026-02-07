# cloner_engine.py
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

ENGINE_VERSION = "v0.13.3"


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

def _connect(db: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db))
    con.row_factory = sqlite3.Row
    return con


def _list_tables(cur: sqlite3.Cursor) -> List[str]:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    return [r[0] for r in cur.fetchall()]


def _table_info(cur: sqlite3.Cursor, table: str):
    cur.execute(f"PRAGMA table_info('{table}')")
    return cur.fetchall()  # cid, name, type, notnull, dflt_value, pk


def _cols(info) -> List[str]:
    return [r[1] for r in info]


def _safe_int(x) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None


def _has_single_integer_pk_id(info) -> bool:
    pk_cols = [r[1] for r in info if r[5]]
    if len(pk_cols) != 1:
        return False
    pk = pk_cols[0]
    if pk.lower() != "id":
        return False
    for cid, name, typ, notnull, dflt, pkflag in info:
        if pkflag and name == pk:
            return (typ or "").strip().upper() == "INTEGER"
    return False


def _table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    return name in set(_list_tables(cur))


def _insert_row(
    cur_t: sqlite3.Cursor,
    table: str,
    cols_t: List[str],
    vals_t: List,
    auto_pk: bool,
):
    preserve_id_tables = {"Data_Car", "Data_CarBody", "Data_Engine"}

    if auto_pk and "Id" in cols_t and table not in preserve_id_tables:
        i = cols_t.index("Id")
        cols_t = cols_t[:i] + cols_t[i + 1:]
        vals_t = vals_t[:i] + vals_t[i + 1:]

    placeholders = ",".join(["?"] * len(cols_t))
    cols_sql = ",".join([f'"{c}"' for c in cols_t])
    cur_t.execute(f'INSERT INTO "{table}" ({cols_sql}) VALUES ({placeholders})', vals_t)


def _row_to_target_shape(
    source_cols: List[str],
    source_row: sqlite3.Row,
    target_cols: List[str],
    rewrites: Dict[str, Any],
    old_base: int,
    new_base: int,
    rewrite_base_ids: bool,
) -> Tuple[List[str], List]:
    src_map = {c: source_row[c] for c in source_cols if c in source_row.keys()}
    insert_cols = [c for c in target_cols if c in src_map]
    insert_vals = [src_map[c] for c in insert_cols]

    for c, v in rewrites.items():
        if c in insert_cols:
            insert_vals[insert_cols.index(c)] = v

    if rewrite_base_ids:
        for i, c in enumerate(insert_cols):
            if c == "Ordinal":
                continue
            if not (c.lower().endswith("id") or c.lower().endswith("ids")):
                continue
            vi = _safe_int(insert_vals[i])
            if vi is None:
                continue
            if old_base <= vi < old_base + 1000:
                insert_vals[i] = new_base + (vi - old_base)

    return insert_cols, insert_vals


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
) -> int:
    target_tables = set(_list_tables(cur_t))
    if table not in target_tables:
        return 0

    info_t = _table_info(cur_t, table)
    cols_t = _cols(info_t)
    auto_pk = _has_single_integer_pk_id(info_t)

    inserted = 0
    seen: set[Tuple[str, Tuple[Any, ...]]] = set()

    for cur_s in cursors_s:
        source_tables = set(_list_tables(cur_s))
        if table not in source_tables:
            continue

        info_s = _table_info(cur_s, table)
        cols_s = _cols(info_s)
        if where_col not in cols_s:
            continue

        cur_s.execute(f'SELECT * FROM "{table}" WHERE "{where_col}"=?', (where_val,))
        rows = cur_s.fetchall()

        for r in rows:
            insert_cols, insert_vals = _row_to_target_shape(
                cols_s, r, cols_t, rewrites, old_base, new_base, rewrite_base_ids
            )
            if not insert_cols:
                continue

            sig = (",".join(insert_cols), tuple(insert_vals))
            if sig in seen:
                continue
            seen.add(sig)

            _insert_row(cur_t, table, insert_cols, insert_vals, auto_pk=auto_pk)
            inserted += 1

    return inserted


# -----------------------------
# CarBody discovery + allocation
# -----------------------------

def _discover_carbody_ref_column(cur: sqlite3.Cursor) -> Optional[str]:
    tables = set(_list_tables(cur))
    if "Data_Car" not in tables:
        return None
    cols = _cols(_table_info(cur, "Data_Car"))

    candidates = [
        "CarBodyID", "CarBodyId", "CarbodyId", "CarBody",
        "BodyID", "BodyId",
        "CarBodyDataID", "CarBodyDataId",
        "Data_CarBodyID", "Data_CarBodyId",
        "CarModelID", "CarModelId",
    ]
    for c in candidates:
        if c in cols:
            return c
    return None


def get_car_body_id(db_path: Path, car_id: int) -> int:
    con = _connect(db_path)
    cur = con.cursor()

    ref_col = _discover_carbody_ref_column(cur)
    body_id = None
    if ref_col:
        try:
            cur.execute(f'SELECT "{ref_col}" AS bid FROM "Data_Car" WHERE "Id"=?', (car_id,))
            r = cur.fetchone()
            if r:
                body_id = _safe_int(r["bid"])
        except Exception:
            body_id = None

    con.close()
    return body_id if body_id is not None else (car_id * 1000)


def _find_free_body_id_in_block(cur_t: sqlite3.Cursor, start: int, block: int = 1000) -> int:
    if "Data_CarBody" not in set(_list_tables(cur_t)):
        return start
    for cand in range(start, start + block):
        cur_t.execute('SELECT 1 FROM "Data_CarBody" WHERE "Id"=? LIMIT 1', (cand,))
        if not cur_t.fetchone():
            return cand
    return start + block


# -----------------------------
# Cars: listing + cloning
# -----------------------------

def suggest_next_car_id(db_path: Path, min_id: int = 2000) -> int:
    con = _connect(db_path)
    cur = con.cursor()
    cur.execute("SELECT MAX(Id) AS mx FROM Data_Car")
    mx = int(cur.fetchone()["mx"] or 0)
    con.close()
    return max(min_id, mx + 1)


def list_cars(db_path: Path) -> List[Tuple[int, int, str]]:
    con = _connect(db_path)
    cur = con.cursor()
    cur.execute('SELECT Id, Year, MediaName FROM Data_Car ORDER BY Id')
    rows = cur.fetchall()
    con.close()

    out: List[Tuple[int, int, str]] = []
    for r in rows:
        cid = int(r["Id"])
        year = int(r["Year"] or 0)
        media = r["MediaName"] if r["MediaName"] else "(no MediaName)"
        out.append((cid, year, str(media)))
    return out
    
def _clone_stock_rows_only_from_multiple_sources(
    cursors_s: List[sqlite3.Cursor],
    cur_t: sqlite3.Cursor,
    table: str,
    where_col: str,
    where_val: int,
    rewrites: Dict[str, Any],
) -> int:
    """
    Clone ONLY stock rows (Level=0 and IsStock=1) from `table` for where_col=where_val.
    If Level/IsStock columns don't exist, we fallback to cloning all rows (still with rewrites).
    """
    target_tables = set(_list_tables(cur_t))
    if table not in target_tables:
        return 0

    info_t = _table_info(cur_t, table)
    cols_t = _cols(info_t)
    auto_pk = _has_single_integer_pk_id(info_t)

    inserted = 0
    seen: set[Tuple[str, Tuple[Any, ...]]] = set()

    # Build filter clause depending on available columns
    def _stock_filter(cols: List[str]) -> tuple[str, tuple]:
        has_level = "Level" in cols
        has_isstock = "IsStock" in cols
        if has_level and has_isstock:
            return ' AND "Level"=0 AND "IsStock"=1', tuple()
        if has_level:
            return ' AND "Level"=0', tuple()
        if has_isstock:
            return ' AND "IsStock"=1', tuple()
        return "", tuple()

    for cur_s in cursors_s:
        source_tables = set(_list_tables(cur_s))
        if table not in source_tables:
            continue

        info_s = _table_info(cur_s, table)
        cols_s = _cols(info_s)
        if where_col not in cols_s:
            continue

        extra_where, _ = _stock_filter(cols_s)
        sql = f'SELECT * FROM "{table}" WHERE "{where_col}"=?{extra_where}'
        cur_s.execute(sql, (where_val,))
        rows = cur_s.fetchall()

        for r in rows:
            src_cols = cols_s
            src_map = {c: r[c] for c in src_cols if c in r.keys()}

            insert_cols = [c for c in cols_t if c in src_map]
            insert_vals = [src_map[c] for c in insert_cols]

            # Apply rewrites
            for c, v in rewrites.items():
                if c in insert_cols:
                    insert_vals[insert_cols.index(c)] = v

            if not insert_cols:
                continue

            sig = (",".join(insert_cols), tuple(insert_vals))
            if sig in seen:
                continue
            seen.add(sig)

            _insert_row(cur_t, table, insert_cols, insert_vals, auto_pk=auto_pk)
            inserted += 1

    return inserted

def clone_car_between(
    source_db: Path,
    target_db: Path,
    source_car_id: int,
    new_car_id: int,
    year_marker: int = 6969,
    extra_source_db: Optional[Path] = None,
) -> CloneReport:
    old_base = source_car_id * 1000
    new_base = new_car_id * 1000

    con_s = _connect(source_db)
    con_t = _connect(target_db)
    cur_s = con_s.cursor()
    cur_t = con_t.cursor()

    con_x = None
    cur_x = None
    if extra_source_db is not None:
        con_x = _connect(extra_source_db)
        cur_x = con_x.cursor()

    touched: Dict[str, int] = {}

    def touch(t: str, n: int):
        if n:
            touched[t] = touched.get(t, 0) + n

    cur_s.execute('SELECT COUNT(*) AS c FROM Data_Car WHERE Id=?', (source_car_id,))
    if int(cur_s.fetchone()["c"] or 0) == 0:
        con_s.close()
        con_t.close()
        if con_x:
            con_x.close()
        raise ValueError(f"Source car {source_car_id} not found in {source_db.name}")

    cur_t.execute('SELECT COUNT(*) AS c FROM Data_Car WHERE Id=?', (new_car_id,))
    if int(cur_t.fetchone()["c"] or 0) != 0:
        con_s.close()
        con_t.close()
        if con_x:
            con_x.close()
        raise ValueError(f"Target already contains CarID {new_car_id}. Pick another.")

    source_body_id = get_car_body_id(source_db, source_car_id)

    if old_base <= source_body_id < old_base + 1000:
        desired_new_body_id = new_base + (source_body_id - old_base)
    else:
        desired_new_body_id = new_base
    new_body_id = _find_free_body_id_in_block(cur_t, desired_new_body_id, block=1000)

    aux_sources: List[sqlite3.Cursor] = [cur_s]
    if cur_x is not None:
        aux_sources.append(cur_x)

    ref_col_t = _discover_carbody_ref_column(cur_t)
    rew_car: Dict[str, Any] = {"Id": new_car_id, "Year": year_marker}
    if ref_col_t:
        rew_car[ref_col_t] = new_body_id

    touch("Data_Car", _clone_rows_from_multiple_sources(
        cursors_s=[cur_s],
        cur_t=cur_t,
        table="Data_Car",
        where_col="Id",
        where_val=source_car_id,
        rewrites=rew_car,
        old_base=old_base,
        new_base=new_base,
        rewrite_base_ids=False
    ))

    body_inserted = _clone_rows_from_multiple_sources(
        cursors_s=aux_sources,
        cur_t=cur_t,
        table="Data_CarBody",
        where_col="Id",
        where_val=source_body_id,
        rewrites={"Id": new_body_id},
        old_base=old_base,
        new_base=new_base,
        rewrite_base_ids=False
    )
    touch("Data_CarBody", body_inserted)

    if body_inserted == 0:
        con_s.close()
        con_t.close()
        if con_x:
            con_x.close()
        raise ValueError(
            f"Missing Data_CarBody.Id={source_body_id} in source/extra sources. "
            "This clone will be blank/crash. Ensure the SLT containing this car also contains its body row."
        )

    target_tables = set(_list_tables(cur_t))
    all_tables = sorted(target_tables)

    body_cols = ["CarBodyID", "CarBodyId", "CarbodyId"]
    # We'll handle List_UpgradeCarBody explicitly (stock row only) to avoid cockpit camera issues.
    upgrade_tables = [
        t for t in all_tables
        if t.lower().startswith("list_upgrade") and t.lower() != "list_upgradecarbody"
]


    for t in upgrade_tables:
        cols_t = _cols(_table_info(cur_t, t))
        if "Ordinal" in cols_t:
            rew: Dict[str, Any] = {"Ordinal": new_car_id}
            for bc in body_cols:
                if bc in cols_t:
                    rew[bc] = new_body_id

            n = _clone_rows_from_multiple_sources(
                cursors_s=aux_sources,
                cur_t=cur_t,
                table=t,
                where_col="Ordinal",
                where_val=source_car_id,
                rewrites=rew,
                old_base=old_base,
                new_base=new_base,
                rewrite_base_ids=True
            )
            touch(t, n)
        else:
            bc = next((c for c in body_cols if c in cols_t), None)
            if bc:
                n = _clone_rows_from_multiple_sources(
                    cursors_s=aux_sources,
                    cur_t=cur_t,
                    table=t,
                    where_col=bc,
                    where_val=source_body_id,
                    rewrites={bc: new_body_id},
                    old_base=old_base,
                    new_base=new_base,
                    rewrite_base_ids=True
                )
                touch(t, n)
                
            # --- Special case: List_UpgradeCarBody (stock row only) ---
    if "List_UpgradeCarBody" in target_tables:
        cols_ucb_t = _cols(_table_info(cur_t, "List_UpgradeCarBody"))

        # Identify key columns
        ordinal_col = "Ordinal" if "Ordinal" in cols_ucb_t else ("CarId" if "CarId" in cols_ucb_t else ("CarID" if "CarID" in cols_ucb_t else None))
        if ordinal_col:
            # Always wipe any existing rows for the new car first (prevents duplicates / cockpit issues)
            cur_t.execute(f'DELETE FROM "List_UpgradeCarBody" WHERE "{ordinal_col}"=?', (new_car_id,))

            # We must also force the body ID column if it exists
            body_col = None
            for bc in ["CarBodyID", "CarBodyId", "CarbodyId"]:
                if bc in cols_ucb_t:
                    body_col = bc
                    break

            rew_ucb = {ordinal_col: new_car_id}
            if body_col:
                rew_ucb[body_col] = new_body_id

            # Clone ONLY stock row(s) from donor -> clone
            n_ucb = _clone_stock_rows_only_from_multiple_sources(
                cursors_s=aux_sources,
                cur_t=cur_t,
                table="List_UpgradeCarBody",
                where_col=ordinal_col,
                where_val=source_car_id,
                rewrites=rew_ucb,
            )
            touch("List_UpgradeCarBody", n_ucb)


    def _is_baseblock_ordinal_table(table: str) -> bool:
        cols_t = _cols(_table_info(cur_t, table))
        if "Ordinal" not in cols_t:
            return False
        id_cols = [c for c in cols_t if c != "Ordinal" and (c.lower().endswith("id") or c.lower().endswith("ids"))]
        if not id_cols:
            return False

        for cur_src in aux_sources:
            if table not in set(_list_tables(cur_src)):
                continue
            cols_s = _cols(_table_info(cur_src, table))
            if "Ordinal" not in cols_s:
                continue
            for c in id_cols[:6]:
                if c not in cols_s:
                    continue
                cur_src.execute(f'SELECT "{c}" AS v FROM "{table}" WHERE "Ordinal"=? LIMIT 80', (source_car_id,))
                for rr in cur_src.fetchall():
                    vi = _safe_int(rr["v"])
                    if vi is not None and old_base <= vi < old_base + 1000:
                        return True
        return False

    ordinal_tables = [
        t for t in all_tables
        if not t.lower().startswith("list_upgrade") and _is_baseblock_ordinal_table(t)
    ]

    for t in ordinal_tables:
        cols_t = _cols(_table_info(cur_t, t))
        rew: Dict[str, Any] = {"Ordinal": new_car_id}
        for bc in body_cols:
            if bc in cols_t:
                rew[bc] = new_body_id

        n = _clone_rows_from_multiple_sources(
            cursors_s=aux_sources,
            cur_t=cur_t,
            table=t,
            where_col="Ordinal",
            where_val=source_car_id,
            rewrites=rew,
            old_base=old_base,
            new_base=new_base,
            rewrite_base_ids=True
        )
        touch(t, n)

    for t in all_tables:
        if t.lower().startswith("list_upgrade"):
            continue
        cols_t = _cols(_table_info(cur_t, t))
        bc = next((c for c in body_cols if c in cols_t), None)
        if not bc:
            continue
        n = _clone_rows_from_multiple_sources(
            cursors_s=aux_sources,
            cur_t=cur_t,
            table=t,
            where_col=bc,
            where_val=source_body_id,
            rewrites={bc: new_body_id},
            old_base=old_base,
            new_base=new_base,
            rewrite_base_ids=True
        )
        touch(t, n)

    for t in all_tables:
        cols_t = _cols(_table_info(cur_t, t))
        if "CarId" in cols_t:
            n = _clone_rows_from_multiple_sources(
                cursors_s=aux_sources,
                cur_t=cur_t,
                table=t,
                where_col="CarId",
                where_val=source_car_id,
                rewrites={"CarId": new_car_id},
                old_base=old_base,
                new_base=new_base,
                rewrite_base_ids=False
            )
            touch(t, n)
        if "CarID" in cols_t:
            n = _clone_rows_from_multiple_sources(
                cursors_s=aux_sources,
                cur_t=cur_t,
                table=t,
                where_col="CarID",
                where_val=source_car_id,
                rewrites={"CarID": new_car_id},
                old_base=old_base,
                new_base=new_base,
                rewrite_base_ids=False
            )
            touch(t, n)

    con_t.commit()
    con_s.close()
    con_t.close()
    if con_x:
        con_x.close()

    return CloneReport(
        source_db=source_db,
        target_db=target_db,
        extra_source_db=extra_source_db,
        source_car_id=source_car_id,
        new_car_id=new_car_id,
        year_marker=year_marker,
        source_body_id=source_body_id,
        new_body_id=new_body_id,
        old_base=old_base,
        new_base=new_base,
        tables_touched=dict(sorted(touched.items(), key=lambda kv: (-kv[1], kv[0]))),
    )


# -----------------------------
# Engine discovery + assignment
# -----------------------------

def list_engines_across_sources(source_paths: List[Path]) -> List[Tuple[int, str, Path]]:
    engines: Dict[int, Tuple[str, Path]] = {}
    preferred_label_cols = ["EngineName", "MediaName", "Name", "DisplayName", "StringKey", "StringID", "Description"]

    for p in source_paths:
        try:
            con = _connect(p)
            cur = con.cursor()
            tables = set(_list_tables(cur))
            if "Data_Engine" not in tables:
                con.close()
                continue

            info = _table_info(cur, "Data_Engine")
            cols = _cols(info)

            id_col = "Id" if "Id" in cols else ("EngineID" if "EngineID" in cols else ("EngineId" if "EngineId" in cols else None))
            if not id_col:
                con.close()
                continue

            label_col = next((c for c in preferred_label_cols if c in cols), None)

            if label_col:
                cur.execute(f'SELECT "{id_col}" AS eid, "{label_col}" AS lbl FROM "Data_Engine"')
            else:
                cur.execute(f'SELECT "{id_col}" AS eid FROM "Data_Engine"')

            for r in cur.fetchall():
                eid = _safe_int(r["eid"])
                if eid is None or eid in engines:
                    continue
                lbl = r["lbl"] if (label_col and "lbl" in r.keys()) else None
                label = str(lbl) if lbl not in (None, "") else f"Engine {eid}"
                engines[eid] = (label, p)

            con.close()
        except Exception:
            try:
                con.close()
            except Exception:
                pass
            continue

    out = [(eid, engines[eid][0], engines[eid][1]) for eid in engines.keys()]
    out.sort(key=lambda x: (x[1].lower(), x[0]))
    return out


def list_engine_medianames_across_sources(source_paths: List[Path]) -> List[Tuple[str, Path]]:
    out: Dict[str, Path] = {}

    for p in source_paths:
        try:
            con = _connect(p)
            cur = con.cursor()
            tables = set(_list_tables(cur))
            if "Data_Engine" not in tables:
                con.close()
                continue

            cols = _cols(_table_info(cur, "Data_Engine"))
            if "MediaName" not in cols:
                con.close()
                continue

            cur.execute('SELECT "MediaName" AS mn FROM "Data_Engine"')
            for r in cur.fetchall():
                mn = r["mn"]
                if mn is None:
                    continue
                mn = str(mn).strip()
                if not mn:
                    continue
                if mn not in out:
                    out[mn] = p

            con.close()
        except Exception:
            try:
                con.close()
            except Exception:
                pass
            continue

    items = [(mn, out[mn]) for mn in out.keys()]
    items.sort(key=lambda x: x[0].lower())
    return items


def set_stock_engine(main_db: Path, car_id: int, engine_id: int) -> int:
    con = _connect(main_db)
    cur = con.cursor()
    tables = set(_list_tables(cur))
    if "List_UpgradeEngine" not in tables:
        con.close()
        raise ValueError("MAIN is missing List_UpgradeEngine table.")

    info = _table_info(cur, "List_UpgradeEngine")
    cols = _cols(info)

    for c in ["Ordinal", "IsStock", "Level"]:
        if c not in cols:
            con.close()
            raise ValueError(f'List_UpgradeEngine is missing required column "{c}".')

    engine_id_candidates = ["EngineID", "EngineId", "Engine", "EngineDataID", "Data_EngineID"]
    eng_col = next((c for c in engine_id_candidates if c in cols), None)
    if not eng_col:
        con.close()
        raise ValueError("Could not find an engine id column in List_UpgradeEngine (e.g., EngineID).")

    cur.execute(
        'SELECT COUNT(*) AS c FROM "List_UpgradeEngine" '
        'WHERE "Ordinal"=? AND "IsStock"=1 AND "Level"=0',
        (car_id,)
    )
    count = int(cur.fetchone()["c"] or 0)
    if count == 0:
        con.close()
        raise ValueError(
            f"No stock engine row found for CarID {car_id} "
            f'(expected Ordinal={car_id}, IsStock=1, Level=0).'
        )

    cur.execute(
        f'UPDATE "List_UpgradeEngine" SET "{eng_col}"=? '
        f'WHERE "Ordinal"=? AND "IsStock"=1 AND "Level"=0',
        (engine_id, car_id)
    )
    updated = cur.rowcount

    con.commit()
    con.close()
    return updated


# -----------------------------
# Engine creator: clone + edit
# -----------------------------

# -----------------------------
# Engine creator: clone + edit
# -----------------------------

def suggest_next_engine_id(main_db: Path, min_id: int = 2000, aux_sources: Optional[List[Path]] = None) -> int:
    """
    Suggest next safe EngineID >= min_id.
    Scans MAIN + optional auxiliary sources (DLC SLTs) to avoid collisions.
    aux_sources should be a list of Paths to SLT files (same style as self.sources in app.py).
    """
    def _scan_db(db_path: Path) -> int:
        con = _connect(db_path)
        cur = con.cursor()
        tables = set(_list_tables(cur))
        if "Data_Engine" not in tables:
            con.close()
            return 0

        cols = _cols(_table_info(cur, "Data_Engine"))
        id_col = (
            "Id" if "Id" in cols else
            "EngineID" if "EngineID" in cols else
            "EngineId" if "EngineId" in cols else
            None
        )
        if not id_col:
            con.close()
            return 0

        cur.execute(f'SELECT MAX("{id_col}") AS mx FROM "Data_Engine"')
        mx = cur.fetchone()["mx"]
        con.close()
        return int(mx) if mx is not None else 0

    max_id = _scan_db(main_db)

    if aux_sources:
        for p in aux_sources:
            if not p:
                continue
            pp = Path(p)
            # avoid double-scanning MAIN if it's included
            if pp.resolve() == Path(main_db).resolve():
                continue
            max_id = max(max_id, _scan_db(pp))

    return max(min_id, max_id + 1)

def _insert_row_preserve_id(cur_t: sqlite3.Cursor, table: str, cols_t: List[str], vals_t: List):
    placeholders = ",".join(["?"] * len(cols_t))
    cols_sql = ",".join([f'"{c}"' for c in cols_t])
    cur_t.execute(f'INSERT INTO "{table}" ({cols_sql}) VALUES ({placeholders})', vals_t)


def _clone_rows_preserve_id(
    cursors_s: List[sqlite3.Cursor],
    cur_t: sqlite3.Cursor,
    table: str,
    where_col: str,
    where_val: int,
    rewrites: Dict[str, Any],
    old_base: int,
    new_base: int,
    rewrite_base_ids: bool,
) -> int:
    target_tables = set(_list_tables(cur_t))
    if table not in target_tables:
        return 0

    info_t = _table_info(cur_t, table)
    cols_t = _cols(info_t)

    inserted = 0
    seen: set[Tuple[str, Tuple[Any, ...]]] = set()

    for cur_s in cursors_s:
        if table not in set(_list_tables(cur_s)):
            continue
        info_s = _table_info(cur_s, table)
        cols_s = _cols(info_s)
        if where_col not in cols_s:
            continue

        cur_s.execute(f'SELECT * FROM "{table}" WHERE "{where_col}"=?', (where_val,))
        rows = cur_s.fetchall()

        for r in rows:
            insert_cols, insert_vals = _row_to_target_shape(
                cols_s, r, cols_t, rewrites, old_base, new_base, rewrite_base_ids
            )
            if not insert_cols:
                continue

            sig = (",".join(insert_cols), tuple(insert_vals))
            if sig in seen:
                continue
            seen.add(sig)

            _insert_row_preserve_id(cur_t, table, insert_cols, insert_vals)
            inserted += 1

    return inserted


def clone_engine_to_main(
    source_db: Path,
    main_db: Path,
    source_engine_id: int,
    new_engine_id: int,
    all_source_paths: Optional[List[Path]] = None,
) -> int:
    """
    Conservative engine clone (safe scope, FM4-friendly):

    - Clone ONLY Data_Engine row (source_engine_id -> new_engine_id)
    - Clone ONLY List_UpgradeEngine* tables (engine-specific upgrade tables)
      EXCEPT List_UpgradeEngine (car<->engine mapping; do NOT clone globally)
    - Clone referenced torque curves (List_TorqueCurve) so 2000xxx curve IDs exist.
    """
    old_base = source_engine_id * 1000
    new_base = new_engine_id * 1000

    con_s = _connect(source_db)
    con_t = _connect(main_db)
    cur_s = con_s.cursor()
    cur_t = con_t.cursor()

    tables_s = set(_list_tables(cur_s))
    tables_t = set(_list_tables(cur_t))

    if "Data_Engine" not in tables_s:
        con_s.close(); con_t.close()
        raise ValueError(f"{source_db.name} has no Data_Engine table.")
    if "Data_Engine" not in tables_t:
        con_s.close(); con_t.close()
        raise ValueError("MAIN has no Data_Engine table.")

    cols_s = _cols(_table_info(cur_s, "Data_Engine"))
    cols_t = _cols(_table_info(cur_t, "Data_Engine"))

    id_col_s = (
        "Id" if "Id" in cols_s else
        "EngineID" if "EngineID" in cols_s else
        "EngineId" if "EngineId" in cols_s else
        None
    )
    id_col_t = (
        "Id" if "Id" in cols_t else
        "EngineID" if "EngineID" in cols_t else
        "EngineId" if "EngineId" in cols_t else
        None
    )
    if not id_col_s or not id_col_t:
        con_s.close(); con_t.close()
        raise ValueError("Could not find Id/EngineID column in Data_Engine (source or MAIN).")

    # Read donor engine row
    cur_s.execute(f'SELECT * FROM "Data_Engine" WHERE "{id_col_s}"=?', (source_engine_id,))
    row = cur_s.fetchone()
    if not row:
        con_s.close(); con_t.close()
        raise ValueError(f"Source engine {source_engine_id} not found in {source_db.name}.")

    # Ensure new id is free in MAIN
    cur_t.execute(f'SELECT COUNT(*) AS c FROM "Data_Engine" WHERE "{id_col_t}"=?', (new_engine_id,))
    if int(cur_t.fetchone()["c"] or 0) != 0:
        con_s.close(); con_t.close()
        raise ValueError(f"MAIN already contains EngineID {new_engine_id}.")

    # Insert Data_Engine row (intersection of columns)
    src_map = {c: row[c] for c in cols_s if c in row.keys()}
    insert_cols = [c for c in cols_t if c in src_map]
    insert_vals = [src_map[c] for c in insert_cols]

    if id_col_t in insert_cols:
        insert_vals[insert_cols.index(id_col_t)] = new_engine_id
    else:
        insert_cols.append(id_col_t)
        insert_vals.append(new_engine_id)

    placeholders = ",".join(["?"] * len(insert_cols))
    cols_sql = ",".join([f'"{c}"' for c in insert_cols])
    cur_t.execute(f'INSERT INTO "Data_Engine" ({cols_sql}) VALUES ({placeholders})', insert_vals)

    # We’ll clone from donor source, but also allow MAIN to act as a source if you’ve added rows there before
    aux_sources = [cur_s, cur_t]

    # Clone ONLY engine-specific upgrade tables, not car<->engine mapping table.
    # List_UpgradeEngine is NOT an engine dependency table.
    engine_ref_cols = [
        "EngineID", "EngineId", "Engine",
        "EngineDataID", "Data_EngineID", "Data_EngineId"
    ]

    # Collect torque curve IDs used by donor engine upgrade rows (before rewrite)
    # We then clone those curves into the new_base range.
    donor_torque_curve_ids: set[int] = set()

    def _collect_torque_curve_ids_from_table(table: str):
        if table not in tables_s:
            return
        cols = _cols(_table_info(cur_s, table))
        # Look for columns like TorqueCurve...ID
        tc_cols = [c for c in cols if "torquecurve" in c.lower() and c.lower().endswith("id")]
        if not tc_cols:
            return

        # Find engine reference column
        ref_col = next((c for c in engine_ref_cols if c in cols), None)
        if not ref_col:
            return

        tc_cols_sql = ",".join(f'"{c}"' for c in tc_cols)
        cur_s.execute(
        f'SELECT {tc_cols_sql} FROM "{table}" WHERE "{ref_col}"=?',
        (source_engine_id,)
)

        for rr in cur_s.fetchall():
            for c in tc_cols:
                try:
                    v = int(rr[c])
                except Exception:
                    continue
                if old_base <= v < old_base + 1000:
                    donor_torque_curve_ids.add(v)

    # Clone the engine upgrade tables and gather torque curve ids along the way
    for table in sorted(tables_t):
        tl = table.lower()
        if not tl.startswith("list_upgradeengine"):
            continue
        if tl == "list_upgradeengine":
            # EXCLUDE car<->engine mapping table
            continue

        # Pull torque curve ids from donor before cloning (safe for any engine upgrade table)
        _collect_torque_curve_ids_from_table(table)

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
        )

    # Now clone required torque curves into MAIN so the rewritten 2000xxx IDs exist.
    # Table name and ID column may vary slightly; detect common patterns.
    if "List_TorqueCurve" in tables_t and donor_torque_curve_ids:
        cols_tc_t = _cols(_table_info(cur_t, "List_TorqueCurve"))
        cols_tc_s = _cols(_table_info(cur_s, "List_TorqueCurve")) if "List_TorqueCurve" in tables_s else []

        # Identify torque curve ID column in target
        tc_id_col_t = (
            "TorqueCurveID" if "TorqueCurveID" in cols_tc_t else
            "TorqueCurveId" if "TorqueCurveId" in cols_tc_t else
            "Id" if "Id" in cols_tc_t else
            "ID" if "ID" in cols_tc_t else
            None
        )
        tc_id_col_s = (
            "TorqueCurveID" if "TorqueCurveID" in cols_tc_s else
            "TorqueCurveId" if "TorqueCurveId" in cols_tc_s else
            "Id" if "Id" in cols_tc_s else
            "ID" if "ID" in cols_tc_s else
            None
        )

        if tc_id_col_t and tc_id_col_s and "List_TorqueCurve" in tables_s:
            # Determine whether we must preserve the PK on insert
            info_tc_t = _table_info(cur_t, "List_TorqueCurve")
            # If it's a single integer PK, _insert_row may drop it unless we tell it not to.
            # We'll insert manually, preserving the ID.
            for old_tc_id in sorted(donor_torque_curve_ids):
                new_tc_id = new_base + (old_tc_id - old_base)

                # Skip if already exists
                cur_t.execute(f'SELECT 1 FROM "List_TorqueCurve" WHERE "{tc_id_col_t}"=? LIMIT 1', (new_tc_id,))
                if cur_t.fetchone():
                    continue

                # Read donor curve row
                cur_s.execute(f'SELECT * FROM "List_TorqueCurve" WHERE "{tc_id_col_s}"=?', (old_tc_id,))
                tc_row = cur_s.fetchone()
                if not tc_row:
                    # If donor missing curve row, that's a hard problem; better to fail loudly
                    con_s.close(); con_t.close()
                    raise ValueError(f"Missing donor torque curve {old_tc_id} in List_TorqueCurve; cannot clone engine safely.")

                src_tc_map = {c: tc_row[c] for c in cols_tc_s if c in tc_row.keys()}

                insert_tc_cols = [c for c in cols_tc_t if c in src_tc_map]
                insert_tc_vals = [src_tc_map[c] for c in insert_tc_cols]

                # Force new torque curve ID
                if tc_id_col_t in insert_tc_cols:
                    insert_tc_vals[insert_tc_cols.index(tc_id_col_t)] = new_tc_id
                else:
                    insert_tc_cols.append(tc_id_col_t)
                    insert_tc_vals.append(new_tc_id)

                placeholders = ",".join(["?"] * len(insert_tc_cols))
                cols_sql = ",".join([f'"{c}"' for c in insert_tc_cols])
                cur_t.execute(f'INSERT INTO "List_TorqueCurve" ({cols_sql}) VALUES ({placeholders})', insert_tc_vals)

    con_t.commit()
    con_s.close()
    con_t.close()
    return 1


def _lookup_table_best_effort(main_db: Path, table_candidates: List[str], label_candidates: List[str]) -> List[Tuple[int, str]]:
    con = _connect(main_db)
    cur = con.cursor()
    tables = set(_list_tables(cur))

    table = next((t for t in table_candidates if t in tables), None)
    if not table:
        con.close()
        return []

    cols = _cols(_table_info(cur, table))
    label_col = next((c for c in label_candidates if c in cols), None)
    if not label_col:
        con.close()
        return []

    if "Id" in cols:
        id_col = "Id"
    else:
        id_col = next((c for c in cols if c.lower().endswith("id")), None)
    if not id_col:
        con.close()
        return []

    cur.execute(f'SELECT "{id_col}" AS i, "{label_col}" AS l FROM "{table}" ORDER BY "{id_col}"')
    out = []
    for r in cur.fetchall():
        i = _safe_int(r["i"])
        if i is None:
            continue
        out.append((i, str(r["l"])))
    con.close()
    return out


def get_engine_editor_options(main_db: Path) -> Dict[str, List[Tuple[int, str]]]:
    return {
        "aspiration": _lookup_table_best_effort(main_db, ["List_Aspiration"], ["Aspiration"]),
        "cylinder": _lookup_table_best_effort(
            main_db,
            ["List_Cylinder", "List_cylinder", "List_Cylinders", "List_cylinders"],
            ["Number"],
        ),
        "config": _lookup_table_best_effort(
            main_db,
            ["List_EngineConfig", "List_Engineconfig", "List_Engineconfig"],
            ["EngineConfig"],
        ),
        "vtiming": _lookup_table_best_effort(
            main_db,
            ["List_Variabletiming", "List_VariableTiming"],
            ["VariableTimingType"],
        ),
    }


def get_engine_fields_from_db(db_path: Path, engine_id: int) -> Dict[str, Any]:
    """
    Read engine fields from a specific SLT (MAIN or DLC).
    This is used by the UI so EngineName/MediaName etc load correctly even when the
    selected engine originates from a DLC SLT.
    """
    con = _connect(db_path)
    cur = con.cursor()
    if "Data_Engine" not in set(_list_tables(cur)):
        con.close()
        raise ValueError(f"{db_path.name} missing Data_Engine.")

    cols = _cols(_table_info(cur, "Data_Engine"))
    id_col = "Id" if "Id" in cols else ("EngineID" if "EngineID" in cols else ("EngineId" if "EngineId" in cols else None))
    if not id_col:
        con.close()
        raise ValueError(f"No Id/EngineID column in {db_path.name} Data_Engine.")

    cur.execute(f'SELECT * FROM "Data_Engine" WHERE "{id_col}"=?', (engine_id,))
    r = cur.fetchone()
    if not r:
        con.close()
        raise ValueError(f"Engine {engine_id} not found in {db_path.name}.")

    def pick(*names: str) -> Optional[str]:
        return next((n for n in names if n in cols), None)

    out: Dict[str, Any] = {"EngineID": engine_id}

    c_asp = pick("AspirationID_Stock", "AspirationIDStock", "AspirationID")
    c_cyl = pick("CylinderID")
    c_cfg = pick("ConfigID", "EngineConfigID")
    c_vt = pick("VariableTimingID", "VariableTimingId", "VariableTiming")
    c_com = pick("Compression")
    c_boo = pick("StockBoost-bar", "StockBoost_bar", "StockBoostBar", "StockBoost")

    c_carb = pick("Carbureted", "IsCarbureted")
    c_diesel = pick("Diesel", "IsDiesel")
    c_rot = pick("Rotary", "IsRotary")

    # For your case EngineName is the real familiar name, so we prioritize it
    c_mn = pick("MediaName", "EngineMedia", "Media", "Media_Name")
    c_en = pick("EngineName", "Name", "Engine_Name", "EngineNameKey", "EngineName_Key")

    for key, col in [
        ("AspirationID_Stock", c_asp),
        ("CylinderID", c_cyl),
        ("ConfigID", c_cfg),
        ("VariableTimingID", c_vt),
        ("Compression", c_com),
        ("StockBoost-bar", c_boo),
        ("Carbureted", c_carb),
        ("Diesel", c_diesel),
        ("Rotary", c_rot),
        ("MediaName", c_mn),
        ("EngineName", c_en),
    ]:
        if col:
            out[key] = r[col]

    con.close()
    return out


def get_engine_fields(main_db: Path, engine_id: int) -> Dict[str, Any]:
    # Backwards-compatible wrapper (MAIN only)
    return get_engine_fields_from_db(main_db, engine_id)


def update_engine_fields(main_db: Path, engine_id: int, changes: Dict[str, Any]) -> int:
    con = _connect(main_db)
    cur = con.cursor()
    if "Data_Engine" not in set(_list_tables(cur)):
        con.close()
        raise ValueError("MAIN missing Data_Engine.")

    cols = _cols(_table_info(cur, "Data_Engine"))
    id_col = "Id" if "Id" in cols else ("EngineID" if "EngineID" in cols else ("EngineId" if "EngineId" in cols else None))
    if not id_col:
        con.close()
        raise ValueError("No Id/EngineID column in MAIN Data_Engine.")

    def pick(*names: str) -> Optional[str]:
        return next((n for n in names if n in cols), None)

    colmap = {
        "AspirationID_Stock": pick("AspirationID_Stock", "AspirationIDStock", "AspirationID"),
        "CylinderID": pick("CylinderID"),
        "ConfigID": pick("ConfigID", "EngineConfigID"),
        "VariableTimingID": pick("VariableTimingID", "VariableTimingId", "VariableTiming"),
        "Compression": pick("Compression"),
        "StockBoost-bar": pick("StockBoost-bar", "StockBoost_bar", "StockBoostBar", "StockBoost"),
        "Carbureted": pick("Carbureted", "IsCarbureted"),
        "Diesel": pick("Diesel", "IsDiesel"),
        "Rotary": pick("Rotary", "IsRotary"),
        "MediaName": pick("MediaName", "EngineMedia", "Media", "Media_Name"),
        "EngineName": pick("EngineName", "Name", "Engine_Name", "EngineNameKey", "EngineName_Key"),
    }

    sets = []
    vals = []
    for k, v in changes.items():
        col = colmap.get(k)
        if not col:
            continue
        sets.append(f'"{col}"=?')
        vals.append(v)

    if not sets:
        con.close()
        return 0

    vals.append(engine_id)
    cur.execute(f'UPDATE "Data_Engine" SET {", ".join(sets)} WHERE "{id_col}"=?', vals)
    n = cur.rowcount
    con.commit()
    con.close()
    return n


# -----------------------------
# Car editor lookup options (for Quick Tweaks dropdowns)
# -----------------------------

def lookup_options(main_db: Path, table_candidates: List[str], label_candidates: List[str]) -> List[Tuple[int, str]]:
    con = _connect(main_db)
    cur = con.cursor()

    chosen_table = next((t for t in table_candidates if _table_exists(cur, t)), None)
    if not chosen_table:
        con.close()
        return []

    cols = _cols(_table_info(cur, chosen_table))
    label_col = next((c for c in label_candidates if c in cols), None)
    if not label_col:
        con.close()
        return []

    if "Id" in cols:
        id_col = "Id"
    else:
        id_col = next((c for c in cols if c.lower().endswith("id")), None)

    if not id_col:
        con.close()
        return []

    cur.execute(f'SELECT "{id_col}" AS i, "{label_col}" AS l FROM "{chosen_table}" ORDER BY "{id_col}"')
    out: List[Tuple[int, str]] = []
    for r in cur.fetchall():
        i = _safe_int(r["i"])
        if i is None:
            continue
        out.append((i, str(r["l"])))
    con.close()
    return out


def get_car_editor_options(main_db: Path) -> Dict[str, List[Tuple[int, str]]]:
    return {
        "EnginePlacementID": lookup_options(main_db, ["List_EnginePlacement"], ["EnginePlacement"]),
        "MaterialTypeID": lookup_options(main_db, ["List_MaterialType"], ["Material"]),
        "EngineConfigID": lookup_options(main_db, ["List_EngineConfig", "List_Engineconfig"], ["EngineConfig"]),
        "DriveTypeID": lookup_options(main_db, ["List_DriveType", "List_Drivetype"], ["DriveType"]),
    }


# -----------------------------
# Quick Tweaks (car) — FIXED rear/back name variants
# -----------------------------

_BODY_FIELD_CANDIDATES: Dict[str, List[str]] = {
    "ModelFrontStockRideHeight": ["ModelFrontStockRideHeight"],
    "ModelBackStockRideHeight": ["ModelBackStockRideHeight", "ModelRearStockRideHeight"],
    "ModelFrontTrackOuter": ["ModelFrontTrackOuter"],
    "ModelBackTrackOuter": ["ModelBackTrackOuter", "ModelRearTrackOuter"],
}


def _pick_existing_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in cols:
            return c
    return None


def get_quick_tweaks(db_path: Path, car_id: int) -> Dict[str, Any]:
    con = _connect(db_path)
    cur = con.cursor()

    out: Dict[str, Any] = {}

    cur.execute('SELECT * FROM Data_Car WHERE Id=?', (car_id,))
    r = cur.fetchone()
    if r:
        for k in [
            "CarTypeID", "ClassID", "EnginePlacementID", "MaterialTypeID",
            "CurbWeight", "WeightDistribution", "EngineConfigID", "DriveTypeID",
            "Year"
        ]:
            if k in r.keys():
                out[f"Data_Car.{k}"] = r[k]

    body_id = get_car_body_id(db_path, car_id)
    if "Data_CarBody" in set(_list_tables(cur)):
        cur.execute('SELECT * FROM Data_CarBody WHERE Id=?', (body_id,))
        b = cur.fetchone()
        if b:
            cols_b = list(b.keys())
            for canonical, candidates in _BODY_FIELD_CANDIDATES.items():
                col = _pick_existing_col(cols_b, candidates)
                if col:
                    out[f"Data_CarBody.{canonical}"] = b[col]

    con.close()
    return out


def apply_quick_tweaks(db_path: Path, car_id: int, changes: Dict[str, Any]) -> Dict[str, int]:
    con = _connect(db_path)
    cur = con.cursor()

    updated: Dict[str, int] = {"Data_Car": 0, "Data_CarBody": 0}

    car_changes = {k.split(".", 1)[1]: v for k, v in changes.items() if k.startswith("Data_Car.")}
    body_changes_raw = {k.split(".", 1)[1]: v for k, v in changes.items() if k.startswith("Data_CarBody.")}

    if car_changes:
        sets = ", ".join([f'"{k}"=?' for k in car_changes.keys()])
        vals = list(car_changes.values()) + [car_id]
        cur.execute(f'UPDATE "Data_Car" SET {sets} WHERE "Id"=?', vals)
        updated["Data_Car"] = cur.rowcount

    if body_changes_raw and "Data_CarBody" in set(_list_tables(cur)):
        body_id = get_car_body_id(db_path, car_id)

        info = _table_info(cur, "Data_CarBody")
        cols_body = _cols(info)

        body_changes: Dict[str, Any] = {}
        for canonical_key, v in body_changes_raw.items():
            if canonical_key in _BODY_FIELD_CANDIDATES:
                actual_col = _pick_existing_col(cols_body, _BODY_FIELD_CANDIDATES[canonical_key])
                if actual_col:
                    body_changes[actual_col] = v
            else:
                if canonical_key in cols_body:
                    body_changes[canonical_key] = v

        if body_changes:
            sets = ", ".join([f'"{k}"=?' for k in body_changes.keys()])
            vals = list(body_changes.values()) + [body_id]
            cur.execute(f'UPDATE "Data_CarBody" SET {sets} WHERE "Id"=?', vals)
            updated["Data_CarBody"] = cur.rowcount

    con.commit()
    con.close()
    return updated


# -----------------------------
# Integrity Checker
# -----------------------------

def integrity_check(db_path: Path, year_marker: Optional[int] = None, min_id: int = 2000) -> List[str]:
    con = _connect(db_path)
    cur = con.cursor()
    tables = set(_list_tables(cur))

    issues: List[str] = []

    if "Data_Car" not in tables:
        con.close()
        return ["Missing Data_Car table."]

    if year_marker is None:
        cur.execute('SELECT Id, MediaName, Year FROM Data_Car WHERE Id>=? ORDER BY Id', (min_id,))
    else:
        cur.execute('SELECT Id, MediaName, Year FROM Data_Car WHERE Id>=? AND Year=? ORDER BY Id', (min_id, year_marker))
    cars = cur.fetchall()

    upgrade_tables = [t for t in tables if t.lower().startswith("list_upgrade")]

    def cols_of(t: str) -> List[str]:
        return _cols(_table_info(cur, t))

    for r in cars:
        cid = int(r["Id"])
        media = r["MediaName"] if r["MediaName"] else "(no MediaName)"

        body_id = get_car_body_id(db_path, cid)

        if "Data_CarBody" in tables:
            cur.execute('SELECT COUNT(*) AS c FROM Data_CarBody WHERE Id=?', (body_id,))
            if int(cur.fetchone()["c"] or 0) == 0:
                issues.append(f"Car {cid} ({media}): Missing Data_CarBody.Id={body_id} (blank/crash risk)")

        found_any_upgrade = False
        for t in upgrade_tables:
            cols = cols_of(t)
            if "Ordinal" in cols:
                cur.execute(f'SELECT 1 FROM "{t}" WHERE "Ordinal"=? LIMIT 1', (cid,))
                if cur.fetchone():
                    found_any_upgrade = True
                    break
            else:
                body_cols = [c for c in ["CarBodyID", "CarBodyId", "CarbodyId"] if c in cols]
                if body_cols:
                    bc = body_cols[0]
                    cur.execute(f'SELECT 1 FROM "{t}" WHERE "{bc}"=? LIMIT 1', (body_id,))
                    if cur.fetchone():
                        found_any_upgrade = True
                        break

        if not found_any_upgrade and upgrade_tables:
            issues.append(f"Car {cid} ({media}): No rows found in any List_Upgrade* table (may be OK, but unusual)")

    con.close()
    return issues


# -----------------------------
# Delete helpers
# -----------------------------

def _delete_where(cur: sqlite3.Cursor, table: str, where_sql: str, params: tuple) -> int:
    cur.execute(f'DELETE FROM "{table}" WHERE {where_sql}', params)
    return cur.rowcount


def delete_car(db_path: Path, car_id: int) -> Dict[str, int]:
    con = _connect(db_path)
    cur = con.cursor()

    tables = _list_tables(cur)
    deleted: Dict[str, int] = {}

    def record(t: str, n: int):
        if n:
            deleted[t] = deleted.get(t, 0) + n

    body_cols = ["CarBodyID", "CarBodyId", "CarbodyId"]
    upgrade_tables = [t for t in tables if t.lower().startswith("list_upgrade")]

    body_id = get_car_body_id(db_path, car_id)

    for t in upgrade_tables:
        cols_t = _cols(_table_info(cur, t))
        if "Ordinal" in cols_t:
            record(t, _delete_where(cur, t, '"Ordinal"=?', (car_id,)))
        else:
            bc = next((c for c in body_cols if c in cols_t), None)
            if bc:
                record(t, _delete_where(cur, t, f'"{bc}"=?', (body_id,)))

    for t in tables:
        if t in upgrade_tables:
            continue
        cols_t = _cols(_table_info(cur, t))
        if "Ordinal" in cols_t:
            record(t, _delete_where(cur, t, '"Ordinal"=?', (car_id,)))

    for t in tables:
        if t in upgrade_tables:
            continue
        cols_t = _cols(_table_info(cur, t))
        bc = next((c for c in body_cols if c in cols_t), None)
        if bc:
            record(t, _delete_where(cur, t, f'"{bc}"=?', (body_id,)))

    for t in tables:
        cols_t = _cols(_table_info(cur, t))
        if "CarId" in cols_t:
            record(t, _delete_where(cur, t, '"CarId"=?', (car_id,)))
        if "CarID" in cols_t:
            record(t, _delete_where(cur, t, '"CarID"=?', (car_id,)))

    if "Data_CarBody" in set(tables):
        record("Data_CarBody", _delete_where(cur, "Data_CarBody", '"Id"=?', (body_id,)))
    record("Data_Car", _delete_where(cur, "Data_Car", '"Id"=?', (car_id,)))

    con.commit()
    con.close()
    return dict(sorted(deleted.items(), key=lambda kv: (-kv[1], kv[0])))


def get_car_ids_by_year(db_path: Path, year: int) -> List[int]:
    con = _connect(db_path)
    cur = con.cursor()
    cur.execute('SELECT Id FROM Data_Car WHERE Year=?', (year,))
    ids = [int(r["Id"]) for r in cur.fetchall()]
    con.close()
    return ids


def get_cloned_car_ids(db_path: Path, min_id: int = 2000) -> List[int]:
    con = _connect(db_path)
    cur = con.cursor()
    cur.execute('SELECT Id FROM Data_Car WHERE Id>=? ORDER BY Id', (min_id,))
    ids = [int(r["Id"]) for r in cur.fetchall()]
    con.close()
    return ids
