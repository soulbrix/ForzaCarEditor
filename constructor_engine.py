# constructor_engine.py
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

CONSTRUCTOR_VERSION = "v0.2.1"


@dataclass
class ApplyReport:
    target_car_id: int
    donor_car_id: int
    donor_source: Path
    subsystem: str
    level: int
    rows_written: Dict[str, int]
    notes: List[str]


# -----------------------------
# DB helpers
# -----------------------------

def _connect(p: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(p))
    con.row_factory = sqlite3.Row
    return con


def _list_tables(cur: sqlite3.Cursor) -> List[str]:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    return [r[0] for r in cur.fetchall()]

def _pick_existing_table(cur: sqlite3.Cursor, candidates: List[str]) -> Optional[str]:
    """
    Return the first table name that exists in the DB, matching case-insensitively.
    Example: candidates ["Data_Engine", "Data_engine"].
    """
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    existing = [r[0] for r in cur.fetchall()]
    lower_map = {name.lower(): name for name in existing}
    for c in candidates:
        if c in existing:
            return c
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None

def _table_info(cur: sqlite3.Cursor, table: str):
    cur.execute(f"PRAGMA table_info('{table}')")
    return cur.fetchall()


def _cols(info) -> List[str]:
    return [r[1] for r in info]


def _pk_col(info) -> Optional[str]:
    for cid, name, typ, notnull, dflt, pkflag in info:
        if pkflag:
            return name
    return None


def _safe_int(x) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None


def _table_exists(cur: sqlite3.Cursor, table: str) -> bool:
    return table in set(_list_tables(cur))


def _find_first_table(cur: sqlite3.Cursor, candidates: List[str]) -> Optional[str]:
    tables = set(_list_tables(cur))
    for t in candidates:
        if t in tables:
            return t
    return None


def _row_exists(cur: sqlite3.Cursor, table: str, pk: str, pk_val: int) -> bool:
    cur.execute(f'SELECT 1 FROM "{table}" WHERE "{pk}"=? LIMIT 1', (pk_val,))
    return cur.fetchone() is not None


def _alloc_id_in_block(cur: sqlite3.Cursor, table: str, pk: str, start: int, block: int = 1000) -> int:
    """
    Finds a free PK value in [start, start+block). If full, returns start+block.
    """
    for cand in range(start, start + block):
        if not _row_exists(cur, table, pk, cand):
            return cand
    return start + block


def _insert_row(cur: sqlite3.Cursor, table: str, cols: List[str], vals: List[Any], auto_drop_id: bool):
    info = _table_info(cur, table)
    cols_t = _cols(info)
    pk = _pk_col(info)
    auto_pk = False
    if pk and pk.lower() == "id":
        # if there is exactly one PK and it's Id INTEGER, treat as auto
        pk_cols = [r[1] for r in info if r[5]]
        if len(pk_cols) == 1:
            for cid, name, typ, notnull, dflt, pkflag in info:
                if pkflag and name == pk and (typ or "").strip().upper() == "INTEGER":
                    auto_pk = True

    preserve_id_tables = {"Data_Car", "Data_CarBody", "Data_Engine"}
    if auto_pk and auto_drop_id and "Id" in cols and table not in preserve_id_tables:
        i = cols.index("Id")
        cols = cols[:i] + cols[i + 1 :]
        vals = vals[:i] + vals[i + 1 :]

    placeholders = ",".join(["?"] * len(cols))
    cols_sql = ",".join([f'"{c}"' for c in cols])
    cur.execute(f'INSERT INTO "{table}" ({cols_sql}) VALUES ({placeholders})', vals)


def _copy_row_by_pk(
    cur_src: sqlite3.Cursor,
    cur_dst: sqlite3.Cursor,
    table: str,
    pk_col: str,
    old_id: int,
    new_id: int,
    id_rewrites: Dict[str, int],
    base_old: int,
    base_new: int,
    rewrite_base_ids: bool,
) -> bool:
    """
    Copies row table(pk=old_id) -> table(pk=new_id).
    Also rewrites known columns + optionally rewrites any *ID-like cols in base block.
    """
    if not _table_exists(cur_dst, table):
        return False
    if not _table_exists(cur_src, table):
        return False

    info_s = _table_info(cur_src, table)
    info_d = _table_info(cur_dst, table)
    cols_s = _cols(info_s)
    cols_d = _cols(info_d)

    cur_src.execute(f'SELECT * FROM "{table}" WHERE "{pk_col}"=?', (old_id,))
    r = cur_src.fetchone()
    if not r:
        return False

    src_map = {c: r[c] for c in cols_s if c in r.keys()}
    ins_cols = [c for c in cols_d if c in src_map]
    ins_vals = [src_map[c] for c in ins_cols]

    if pk_col in ins_cols:
        ins_vals[ins_cols.index(pk_col)] = new_id

    # explicit rewrites
    for c, v in id_rewrites.items():
        if c in ins_cols:
            ins_vals[ins_cols.index(c)] = v

    # base-block rewrite for ID-like columns
    if rewrite_base_ids:
        for i, c in enumerate(ins_cols):
            if c == "Ordinal":
                continue
            if not (c.lower().endswith("id") or c.lower().endswith("ids")):
                continue
            vi = _safe_int(ins_vals[i])
            if vi is None:
                continue
            if base_old <= vi < base_old + 1000:
                ins_vals[i] = base_new + (vi - base_old)

    # if destination row already exists, replace
    cur_dst.execute(f'DELETE FROM "{table}" WHERE "{pk_col}"=?', (new_id,))
    _insert_row(cur_dst, table, ins_cols, ins_vals, auto_drop_id=True)
    return True


# -----------------------------
# Subsystem detection
# -----------------------------

_SUBSYSTEMS = [
    ("Engine", ["List_UpgradesEngine"]),
    ("SpringDamper", ["List_UpgradeSpringDamper", "List_UpgradesSpringDamper"]),
    ("Transmission", ["List_UpgradeTransmission", "List_UpgradesTransmission"]),
    ("Differential", ["List_UpgradeDifferential", "List_UpgradesDifferential"]),
    ("Brakes", ["List_UpgradeBrakes", "List_UpgradesBrakes"]),
    ("Tires", ["List_UpgradeTires", "List_UpgradesTires"]),
    ("Aero", ["List_UpgradeAero", "List_UpgradesAero"]),
]


def list_supported_subsystems(main_db: Path) -> List[str]:
    con = _connect(main_db)
    cur = con.cursor()
    tables = set(_list_tables(cur))
    con.close()

    out = []
    for name, candidates in _SUBSYSTEMS:
        if any(t in tables for t in candidates):
            out.append(name)
    return out


def _pick_upgrade_table(cur: sqlite3.Cursor, subsystem: str) -> Optional[str]:
    for name, candidates in _SUBSYSTEMS:
        if name.lower() == subsystem.lower():
            t = _find_first_table(cur, candidates)
            return t
    # fallback: try fuzzy match
    tables = _list_tables(cur)
    want = subsystem.lower()
    for t in tables:
        if t.lower().startswith("list_upgrade") and want in t.lower():
            return t
    return None


def _find_level_row(
    cur_src: sqlite3.Cursor,
    upgrade_table: str,
    donor_car_id: int,
    level: int,
) -> Optional[sqlite3.Row]:
    cols = _cols(_table_info(cur_src, upgrade_table))
    if "Ordinal" not in cols:
        return None

    level_col = "Level" if "Level" in cols else ("level" if "level" in cols else None)
    if not level_col:
        # no Level column? choose first row
        cur_src.execute(f'SELECT * FROM "{upgrade_table}" WHERE "Ordinal"=? LIMIT 1', (donor_car_id,))
        return cur_src.fetchone()

    # Prefer exact level row; if none, fall back to IsStock=1 and Level=0
    cur_src.execute(
        f'SELECT * FROM "{upgrade_table}" WHERE "Ordinal"=? AND "{level_col}"=? LIMIT 1',
        (donor_car_id, level),
    )
    r = cur_src.fetchone()
    if r:
        return r

    # fallback stock
    if "IsStock" in cols:
        cur_src.execute(
            f'SELECT * FROM "{upgrade_table}" WHERE "Ordinal"=? AND "IsStock"=1 AND "{level_col}"=0 LIMIT 1',
            (donor_car_id,),
        )
        return cur_src.fetchone()

    cur_src.execute(
        f'SELECT * FROM "{upgrade_table}" WHERE "Ordinal"=? ORDER BY "{level_col}" LIMIT 1',
        (donor_car_id,),
    )
    return cur_src.fetchone()


def _write_level_row(
    cur_dst: sqlite3.Cursor,
    upgrade_table: str,
    target_car_id: int,
    src_row: sqlite3.Row,
    desired_level: int,
) -> int:
    """
    Writes/overwrites the level row for target in upgrade_table based on src_row.
    """
    info = _table_info(cur_dst, upgrade_table)
    cols_d = _cols(info)

    # Source map
    src_map = {k: src_row[k] for k in src_row.keys()}

    # Build insert
    ins_cols = [c for c in cols_d if c in src_map]
    ins_vals = [src_map[c] for c in ins_cols]

    if "Ordinal" in ins_cols:
        ins_vals[ins_cols.index("Ordinal")] = target_car_id

    level_col = "Level" if "Level" in cols_d else ("level" if "level" in cols_d else None)
    if level_col and level_col in ins_cols:
        ins_vals[ins_cols.index(level_col)] = desired_level

    # IsStock behavior: for level 0, set IsStock=1 if it exists; else 0
    if "IsStock" in cols_d and "IsStock" in ins_cols:
        ins_vals[ins_cols.index("IsStock")] = 1 if desired_level == 0 else 0

    # Delete existing target row for that level (or all if no level col)
    if level_col:
        cur_dst.execute(
            f'DELETE FROM "{upgrade_table}" WHERE "Ordinal"=? AND "{level_col}"=?',
            (target_car_id, desired_level),
        )
    else:
        cur_dst.execute(f'DELETE FROM "{upgrade_table}" WHERE "Ordinal"=?', (target_car_id,))

    _insert_row(cur_dst, upgrade_table, ins_cols, ins_vals, auto_drop_id=True)
    return 1


# -----------------------------
# Physics cloning (base-block)
# -----------------------------

def _clone_physics_ids_from_upgrade_row(
    cur_src: sqlite3.Cursor,
    cur_dst: sqlite3.Cursor,
    src_row: sqlite3.Row,
    donor_car_id: int,
    target_car_id: int,
    notes: List[str],
) -> Dict[str, int]:
    """
    Best-effort:
    - For any column ending with 'PhysicsID' (or containing 'PhysicsID'), if its value
      is within donor base block, we clone the row from the matching physics table (if found)
      into target base block and rewrite the ID in the target upgrade row.
    Returns mapping of column->new_id to rewrite in upgrade row.
    """
    rewrites: Dict[str, int] = {}
    donor_base = donor_car_id * 1000
    target_base = target_car_id * 1000

    src_cols = list(src_row.keys())
    phys_cols = [c for c in src_cols if "physicsid" in c.lower()]

    if not phys_cols:
        return rewrites

    # physics table candidates by known patterns
    physics_tables = {
        "SpringDamperPhysics": ["List_SpringDamperPhysics"],
        "AntiSwayPhysics": ["List_AntiSwayPhysics"],
        "TransmissionPhysics": ["List_TransmissionPhysics"],
        "DifferentialPhysics": ["List_DifferentialPhysics"],
        "BrakePhysics": ["List_BrakePhysics"],
        "AeroPhysics": ["List_AeroPhysics"],
        "TireCompound": ["List_TireCompound"],  # not always PhysicsID, but leave it here
    }

    dst_tables = set(_list_tables(cur_dst))

    for c in phys_cols:
        vi = _safe_int(src_row[c])
        if vi is None:
            continue
        if not (donor_base <= vi < donor_base + 1000):
            # likely global/shared id; do not clone
            continue

        # choose new id (same offset) then ensure free
        desired_new = target_base + (vi - donor_base)

        # pick the best physics table to clone from, based on column name
        cname = c.lower()
        table_candidates: List[str] = []
        if "springdamper" in cname:
            table_candidates = physics_tables["SpringDamperPhysics"]
        elif "antisway" in cname:
            table_candidates = physics_tables["AntiSwayPhysics"]
        elif "transmission" in cname:
            table_candidates = physics_tables["TransmissionPhysics"]
        elif "differential" in cname:
            table_candidates = physics_tables["DifferentialPhysics"]
        elif "brake" in cname:
            table_candidates = physics_tables["BrakePhysics"]
        elif "aero" in cname:
            table_candidates = physics_tables["AeroPhysics"]
        elif "tire" in cname:
            table_candidates = physics_tables["TireCompound"]
        else:
            # heuristic: look for any List_*Physics table
            table_candidates = [t for t in dst_tables if t.lower().startswith("list_") and t.lower().endswith("physics")]

        # find existing table
        phys_table = None
        for t in table_candidates:
            if t in dst_tables:
                phys_table = t
                break

        if not phys_table:
            notes.append(f'Could not find physics table for column "{c}" (value {vi}). Kept original ID.')
            continue

        info_dst = _table_info(cur_dst, phys_table)
        pk = _pk_col(info_dst)
        if not pk:
            notes.append(f'Physics table "{phys_table}" has no PK. Skipped cloning for {c}.')
            continue

        # allocate actual new id in target block
        new_id = _alloc_id_in_block(cur_dst, phys_table, pk, desired_new, block=1000)

        ok = _copy_row_by_pk(
            cur_src=cur_src,
            cur_dst=cur_dst,
            table=phys_table,
            pk_col=pk,
            old_id=vi,
            new_id=new_id,
            id_rewrites={},  # base rewrite handles inner references
            base_old=donor_base,
            base_new=target_base,
            rewrite_base_ids=True,
        )
        if ok:
            rewrites[c] = new_id
        else:
            notes.append(f'Failed to clone "{phys_table}" row {vi} for column "{c}". Kept original ID.')

    return rewrites


# -----------------------------
# Public: apply subsystem
# -----------------------------

def apply_subsystem_from_donor(
    main_db: Path,
    donor_source_db: Path,
    target_car_id: int,
    donor_car_id: int,
    subsystem: str,
    level: int,
) -> ApplyReport:
    """
    Applies one subsystem at chosen level from donor car to target car.
    Only edits MAIN.
    """
    con_dst = _connect(main_db)
    cur_dst = con_dst.cursor()

    con_src = _connect(donor_source_db)
    cur_src = con_src.cursor()

    rows_written: Dict[str, int] = {}
    notes: List[str] = []

    # Validate target exists in MAIN
    cur_dst.execute('SELECT 1 FROM "Data_Car" WHERE "Id"=? LIMIT 1', (target_car_id,))
    if not cur_dst.fetchone():
        con_src.close()
        con_dst.close()
        raise ValueError(f"Target CarID {target_car_id} not found in MAIN.")

    upgrade_table = _pick_upgrade_table(cur_dst, subsystem)
    if not upgrade_table:
        con_src.close()
        con_dst.close()
        raise ValueError(f"No upgrade table found in MAIN for subsystem {subsystem}.")

    # Make sure donor table exists in donor source
    if not _table_exists(cur_src, upgrade_table):
        con_src.close()
        con_dst.close()
        raise ValueError(f'Donor source "{donor_source_db.name}" does not contain table "{upgrade_table}".')

    src_row = _find_level_row(cur_src, upgrade_table, donor_car_id, level)
    if not src_row:
        con_src.close()
        con_dst.close()
        raise ValueError(f"No donor row found for {upgrade_table} Ordinal={donor_car_id} Level={level}.")

    # Special case: Engine uses List_UpgradesEngine and references Data_Engine but no physics cloning required.
    # We still copy the level row and let existing stock engine assignment feature work.
    # For other subsystems, we attempt to clone linked PhysicsIDs if they are base-block.
    rewrites = _clone_physics_ids_from_upgrade_row(cur_src, cur_dst, src_row, donor_car_id, target_car_id, notes)

    # Write the upgrade row (overwriting target)
    n = _write_level_row(cur_dst, upgrade_table, target_car_id, src_row, desired_level=level)
    rows_written[upgrade_table] = rows_written.get(upgrade_table, 0) + n

    # Apply rewrites to the just-written row (if any)
    if rewrites:
        cols_d = _cols(_table_info(cur_dst, upgrade_table))
        level_col = "Level" if "Level" in cols_d else ("level" if "level" in cols_d else None)

        sets = []
        vals: List[Any] = []
        for c, v in rewrites.items():
            if c in cols_d:
                sets.append(f'"{c}"=?')
                vals.append(v)

        if sets:
            if level_col:
                vals.extend([target_car_id, level])
                cur_dst.execute(
                    f'UPDATE "{upgrade_table}" SET {", ".join(sets)} WHERE "Ordinal"=? AND "{level_col}"=?',
                    vals,
                )
            else:
                vals.append(target_car_id)
                cur_dst.execute(
                    f'UPDATE "{upgrade_table}" SET {", ".join(sets)} WHERE "Ordinal"=?',
                    vals,
                )
            rows_written[f"{upgrade_table} (rewrites)"] = cur_dst.rowcount

    con_dst.commit()
    con_src.close()
    con_dst.close()

    return ApplyReport(
        target_car_id=target_car_id,
        donor_car_id=donor_car_id,
        donor_source=donor_source_db,
        subsystem=subsystem,
        level=level,
        rows_written=dict(sorted(rows_written.items(), key=lambda kv: (-kv[1], kv[0]))),
        notes=notes,
    )


# -----------------------------
# Spicy camber (stock suspension row)
# -----------------------------

def apply_spicy_camber(
    main_db: Path,
    target_car_id: int,
    front_camber: float = -2.5,
    rear_camber: float = -4.0,
) -> Dict[str, int]:
    """
    Applies camber by editing List_SpringDamperPhysics.StaticCamber
    for the target car's STOCK spring/damper row (IsStock=1, Level=0).
    """
    con = _connect(main_db)
    cur = con.cursor()
    tables = set(_list_tables(cur))

    if "List_UpgradeSpringDamper" not in tables:
        con.close()
        raise ValueError("MAIN missing List_UpgradeSpringDamper.")
    if "List_SpringDamperPhysics" not in tables:
        con.close()
        raise ValueError("MAIN missing List_SpringDamperPhysics.")

    cols_u = _cols(_table_info(cur, "List_UpgradeSpringDamper"))
    needed = ["Ordinal", "IsStock", "Level"]
    for c in needed:
        if c not in cols_u:
            con.close()
            raise ValueError(f'List_UpgradeSpringDamper missing "{c}".')

    front_col = next((c for c in cols_u if c.lower() == "frontspringdamperphysicsid"), None)
    rear_col = next((c for c in cols_u if c.lower() == "rearspringdamperphysicsid"), None)
    if not front_col or not rear_col:
        con.close()
        raise ValueError("List_UpgradeSpringDamper missing Front/RearSpringDamperPhysicsID columns.")

    cur.execute(
        'SELECT * FROM "List_UpgradeSpringDamper" WHERE "Ordinal"=? AND "IsStock"=1 AND "Level"=0 LIMIT 1',
        (target_car_id,),
    )
    r = cur.fetchone()
    if not r:
        con.close()
        raise ValueError(f"No stock spring/damper row found for CarID {target_car_id} (IsStock=1 Level=0).")

    f_id = _safe_int(r[front_col])
    b_id = _safe_int(r[rear_col])
    if f_id is None or b_id is None:
        con.close()
        raise ValueError("Front/RearSpringDamperPhysicsID values are null/invalid.")

    cols_p = _cols(_table_info(cur, "List_SpringDamperPhysics"))
    pk = _pk_col(_table_info(cur, "List_SpringDamperPhysics"))
    if not pk:
        con.close()
        raise ValueError("List_SpringDamperPhysics has no primary key.")

    cam_col = next((c for c in cols_p if c.lower() == "staticcamber"), None)
    if not cam_col:
        con.close()
        raise ValueError("List_SpringDamperPhysics missing StaticCamber column.")

    out = {"front": 0, "rear": 0}

    cur.execute(f'UPDATE "List_SpringDamperPhysics" SET "{cam_col}"=? WHERE "{pk}"=?', (float(front_camber), f_id))
    out["front"] = cur.rowcount
    cur.execute(f'UPDATE "List_SpringDamperPhysics" SET "{cam_col}"=? WHERE "{pk}"=?', (float(rear_camber), b_id))
    out["rear"] = cur.rowcount

    con.commit()
    con.close()
    return out
    
    # -----------------------------
# Constructor Studio APIs (UI helpers)
# -----------------------------
import shutil
from datetime import datetime


def backup_db(db: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = db.with_name(f"{db.stem}_backup_{ts}{db.suffix}")
    shutil.copy2(db, out)
    return out


def build_source_list(main_db: Path, dlc_folder: Optional[Path]) -> List[Path]:
    sources = [Path(main_db)]

    if dlc_folder and Path(dlc_folder).exists():
        dlc_root = Path(dlc_folder)

        found = []
        for p in dlc_root.rglob("*"):
            if p.is_file() and p.suffix.lower() == ".slt":
                if p.resolve() == Path(main_db).resolve():
                    continue
                found.append(p)

        # stable order: by filename then by full path
        found = sorted(found, key=lambda x: (x.name.lower(), str(x).lower()))
        sources.extend(found)

    return sources


def _first_existing_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in cols:
            return c
    return None


def list_cars_all_sources(sources: List[Path]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[Tuple[int, str]] = set()

    for src in sources:
        con = _connect(src)
        cur = con.cursor()
        if "Data_Car" not in set(_list_tables(cur)):
            con.close()
            continue

        cols = _cols(_table_info(cur, "Data_Car"))
        car_id_col = _first_existing_col(cols, ["CarID", "CarId", "Id"])
        media_col = _first_existing_col(cols, ["MediaName", "CarName", "Name"])
        year_col = _first_existing_col(cols, ["ModelYear", "Year", "ReleaseYear"])

        if not car_id_col:
            con.close()
            continue

        sel_cols = [car_id_col]
        if media_col:
            sel_cols.append(media_col)
        if year_col:
            sel_cols.append(year_col)

        sel_sql = ",".join(f'"{c}"' for c in sel_cols)
        cur.execute(f'SELECT {sel_sql} FROM "Data_Car"')
        rows = cur.fetchall()

        for r in rows:
            car_id = int(r[car_id_col])
            media = r[media_col] if media_col and media_col in r.keys() else ""
            year = r[year_col] if year_col and year_col in r.keys() else None
            key = (car_id, src.name)
            if key in seen:
                continue
            seen.add(key)
            out.append({"CarID": car_id, "MediaName": media or "", "Year": year, "Source": str(src)})

        con.close()

    return out


def list_engines_all_sources(sources: List[Path]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[Tuple[int, str]] = set()

    for src in sources:
        con = _connect(src)
        cur = con.cursor()
        if "Data_Engine" not in set(_list_tables(cur)):
            con.close()
            continue

        cols = _cols(_table_info(cur, "Data_Engine"))
        id_col = _first_existing_col(cols, ["EngineID", "EngineId", "Id"])
        name_col = _first_existing_col(cols, ["EngineName", "Name"])
        media_col = _first_existing_col(cols, ["MediaName"])

        if not id_col:
            con.close()
            continue

        sel_cols = [id_col]
        if name_col:
            sel_cols.append(name_col)
        if media_col:
            sel_cols.append(media_col)

        sel_sql = ",".join(f'"{c}"' for c in sel_cols)
        cur.execute(f'SELECT {sel_sql} FROM "Data_Engine"')
        rows = cur.fetchall()

        for r in rows:
            eid = int(r[id_col])
            en = r[name_col] if name_col and name_col in r.keys() else ""
            mn = r[media_col] if media_col and media_col in r.keys() else ""
            key = (eid, src.name)
            if key in seen:
                continue
            seen.add(key)
            out.append({"EngineID": eid, "EngineName": en or "", "MediaName": mn or "", "Source": str(src)})

        con.close()

    return out

def build_lookup_cache(main_db: Path, sources: List[Path]) -> Dict[str, Dict[int, str]]:
    """
    Builds lookups from MAIN first; falls back to DLC for display if missing.
    Returned format: cache[TableName][id_int] = display_str
    """
    cache: Dict[str, Dict[int, str]] = {}

    def load_table(src: Path, table: str, id_col: str, name_col: str):
        con = _connect(src)
        cur = con.cursor()
        if table not in set(_list_tables(cur)):
            con.close()
            return
        cols = _cols(_table_info(cur, table))
        if id_col not in cols or name_col not in cols:
            con.close()
            return
        cur.execute(f'SELECT "{id_col}" AS _id, "{name_col}" AS _name FROM "{table}"')
        m = cache.setdefault(table, {})
        for r in cur.fetchall():
            try:
                rid = int(r["_id"])
            except Exception:
                continue
            if rid not in m:
                m[rid] = "" if r["_name"] is None else str(r["_name"])
        con.close()

    # MAIN first, then DLCs fill missing ids (do not overwrite)
    primary = [Path(main_db)] + [p for p in sources if Path(p).resolve() != Path(main_db).resolve()]

    for src in primary:
        # Data_Car dropdown lookups
        # EnginePlacement: integer is in column EnginePlacement, display in DisplayName
        load_table(src, "List_EnginePlacement", "ID", "EnginePlacement")
        
        # MaterialType: integer is MaterialTypeID, display in Material
        load_table(src, "List_MaterialType", "MaterialTypeID", "Material")

        # Engine editor dropdown lookups
        # ConfigID: integer is EngineConfig, display in DisplayName
        load_table(src, "List_EngineConfig", "ConfigID", "EngineConfig")

        # CylinderID: integer is CylinderID, display in Number
        # (user said List_Cylinders; in some SLTs it may be List_Cylinder or List_Cylinders)
        load_table(src, "List_Cylinders", "CylinderID", "Number")
        load_table(src, "List_Cylinder", "CylinderID", "Number")

        # VariableTimingID: integer is VariableTimingID, display in VariableTimingType
        load_table(src, "List_VariableTiming", "VariableTimingID", "VariableTimingType")

        # Tire compound: integer is TireCompoundID, display in DisplayName
        load_table(src, "List_TireCompound", "TireCompoundID", "DisplayName")

        # Drive type: integer is ID, display in DriveType (for drivetrain special resolver)
        load_table(src, "List_DriveType", "ID", "DriveType")

    return cache


def get_data_car(main_db: Path, car_id: int) -> Optional[Dict[str, Any]]:
    con = _connect(main_db)
    cur = con.cursor()
    if "Data_Car" not in set(_list_tables(cur)):
        con.close()
        return None
    cols = _cols(_table_info(cur, "Data_Car"))
    pk = _first_existing_col(cols, ["CarID", "CarId", "Id"])
    if not pk:
        con.close()
        return None
    cur.execute(f'SELECT * FROM "Data_Car" WHERE "{pk}"=?', (car_id,))
    r = cur.fetchone()
    con.close()
    return dict(r) if r else None


def update_data_car(main_db: Path, car_id: int, updates: Dict[str, Any]) -> None:
    con = _connect(main_db)
    cur = con.cursor()
    cols = _cols(_table_info(cur, "Data_Car"))
    pk = _first_existing_col(cols, ["CarID", "CarId", "Id"])
    if not pk:
        con.close()
        raise ValueError("Data_Car has no CarID/Id column.")
    # only update existing columns
    upd = {k: v for k, v in updates.items() if k in cols and k != pk}
    if not upd:
        con.close()
        return
    sets = ", ".join([f'"{k}"=?' for k in upd.keys()])
    cur.execute(f'UPDATE "Data_Car" SET {sets} WHERE "{pk}"=?', (*upd.values(), car_id))
    con.commit()
    con.close()


def get_data_carbody_for_car(main_db: Path, car_id: int) -> Optional[Dict[str, Any]]:
    """
    FM4 common scheme: Data_CarBody.Id lives in car base-block (car_id*1000..+999).
    We pick the first row in that block.
    """
    con = _connect(main_db)
    cur = con.cursor()
    if "Data_CarBody" not in set(_list_tables(cur)):
        con.close()
        return None
    cols = _cols(_table_info(cur, "Data_CarBody"))
    if "Id" not in cols:
        con.close()
        return None
    base = car_id * 1000
    cur.execute('SELECT * FROM "Data_CarBody" WHERE "Id">=? AND "Id"<? ORDER BY "Id" LIMIT 1', (base, base + 1000))
    r = cur.fetchone()
    con.close()
    return dict(r) if r else None


def update_data_carbody(main_db: Path, carbody_id: int, updates: Dict[str, Any]) -> None:
    con = _connect(main_db)
    cur = con.cursor()
    cols = _cols(_table_info(cur, "Data_CarBody"))
    if "Id" not in cols:
        con.close()
        raise ValueError("Data_CarBody has no Id.")
    upd = {k: v for k, v in updates.items() if k in cols and k != "Id"}
    if not upd:
        con.close()
        return
    sets = ", ".join([f'"{k}"=?' for k in upd.keys()])
    cur.execute(f'UPDATE "Data_CarBody" SET {sets} WHERE "Id"=?', (*upd.values(), carbody_id))
    con.commit()
    con.close()


def get_data_engine(main_db: Path, engine_id: int) -> Optional[Dict[str, Any]]:
    con = _connect(main_db)
    cur = con.cursor()
    if "Data_Engine" not in set(_list_tables(cur)):
        con.close()
        return None
    cols = _cols(_table_info(cur, "Data_Engine"))
    pk = _first_existing_col(cols, ["EngineID", "EngineId", "Id"])
    if not pk:
        con.close()
        return None
    cur.execute(f'SELECT * FROM "Data_Engine" WHERE "{pk}"=?', (engine_id,))
    r = cur.fetchone()
    con.close()
    return dict(r) if r else None


def update_data_engine(main_db: Path, engine_id: int, updates: Dict[str, Any]) -> None:
    con = _connect(main_db)
    cur = con.cursor()
    cols = _cols(_table_info(cur, "Data_Engine"))
    pk = _first_existing_col(cols, ["EngineID", "EngineId", "Id"])
    if not pk:
        con.close()
        raise ValueError("Data_Engine has no EngineID/Id column.")
    upd = {k: v for k, v in updates.items() if k in cols and k != pk}
    if not upd:
        con.close()
        return
    sets = ", ".join([f'"{k}"=?' for k in upd.keys()])
    cur.execute(f'UPDATE "Data_Engine" SET {sets} WHERE "{pk}"=?', (*upd.values(), engine_id))
    con.commit()
    con.close()


def engine_exists_in_main(main_db: Path, engine_id: int) -> bool:
    con = _connect(main_db)
    cur = con.cursor()
    if "Data_Engine" not in set(_list_tables(cur)):
        con.close()
        return False
    cols = _cols(_table_info(cur, "Data_Engine"))
    pk = _first_existing_col(cols, ["EngineID", "EngineId", "Id"])
    if not pk:
        con.close()
        return False
    cur.execute(f'SELECT 1 FROM "Data_Engine" WHERE "{pk}"=? LIMIT 1', (engine_id,))
    ok = cur.fetchone() is not None
    con.close()
    return ok


def resolve_engine_name(sources: List[Path], engine_id: int) -> str:
    for src in sources:
        con = _connect(src)
        cur = con.cursor()
        if "Data_Engine" not in set(_list_tables(cur)):
            con.close()
            continue
        cols = _cols(_table_info(cur, "Data_Engine"))
        pk = _first_existing_col(cols, ["EngineID", "EngineId", "Id"])
        name_col = _first_existing_col(cols, ["EngineName", "Name"])
        if not pk or not name_col:
            con.close()
            continue
        cur.execute(f'SELECT "{name_col}" AS n FROM "Data_Engine" WHERE "{pk}"=?', (engine_id,))
        r = cur.fetchone()
        con.close()
        if r and r["n"] is not None:
            return str(r["n"])
    return ""

def get_stock_drivetrain_id_for_car(main_db: Path, car_id: int) -> Optional[int]:
    """
    Tries to resolve the drivetrain/powertrain ID for a car from MAIN.

    Priority:
    1) List_UpgradeDrivetrain stock row (IsStock=1, Level=0) -> PowertrainID/DrivetrainID column
    2) Any row in List_UpgradeDrivetrain for this car
    3) Data_Car.PowertrainID (fallback)
    """
    con = _connect(main_db)
    cur = con.cursor()
    tables = set(_list_tables(cur))

    # 1/2) Prefer List_UpgradeDrivetrain
    if "List_UpgradeDrivetrain" in tables:
        cols = _cols(_table_info(cur, "List_UpgradeDrivetrain"))
        if "Ordinal" in cols:
            id_col = _first_existing_col(cols, ["PowertrainID", "PowertrainId", "DrivetrainID", "DrivetrainId"])
            if id_col:
                has_isstock = "IsStock" in cols
                has_level = "Level" in cols

                if has_isstock and has_level:
                    cur.execute(
                        f'SELECT "{id_col}" AS v FROM "List_UpgradeDrivetrain" '
                        f'WHERE "Ordinal"=? AND "IsStock"=1 AND "Level"=0 LIMIT 1',
                        (car_id,),
                    )
                    r = cur.fetchone()
                    if r and r["v"] is not None:
                        con.close()
                        return int(r["v"])

                # fallback: first row for this car
                cur.execute(
                    f'SELECT "{id_col}" AS v FROM "List_UpgradeDrivetrain" WHERE "Ordinal"=? LIMIT 1',
                    (car_id,),
                )
                r = cur.fetchone()
                if r and r["v"] is not None:
                    con.close()
                    return int(r["v"])

    # 3) Fallback to Data_Car.PowertrainID if exists
    if "Data_Car" in tables:
        cols = _cols(_table_info(cur, "Data_Car"))
        if "Id" in cols and "PowertrainID" in cols:
            cur.execute('SELECT "PowertrainID" AS v FROM "Data_Car" WHERE "Id"=? LIMIT 1', (car_id,))
            r = cur.fetchone()
            if r and r["v"] is not None:
                con.close()
                return int(r["v"])

    con.close()
    return None


def get_stock_engine_for_car(main_db: Path, car_id: int) -> Optional[Dict[str, Any]]:
    con = _connect(main_db)
    cur = con.cursor()
    if "List_UpgradeEngine" not in set(_list_tables(cur)):
        con.close()
        return None
    cols = _cols(_table_info(cur, "List_UpgradeEngine"))
    if "Ordinal" not in cols:
        con.close()
        return None
    level_col = "Level" if "Level" in cols else None
    isstock_col = "IsStock" if "IsStock" in cols else None
    engine_col = _first_existing_col(cols, ["EngineID", "EngineId", "Engine"])
    if not engine_col:
        con.close()
        return None

    # Prefer IsStock=1 & Level=0 when present
    if level_col and isstock_col:
        cur.execute(f'SELECT * FROM "List_UpgradeEngine" WHERE "Ordinal"=? AND "{isstock_col}"=1 AND "{level_col}"=0 LIMIT 1', (car_id,))
        r = cur.fetchone()
        con.close()
        return dict(r) if r else None

    # fallback first row
    cur.execute(f'SELECT * FROM "List_UpgradeEngine" WHERE "Ordinal"=? LIMIT 1', (car_id,))
    r = cur.fetchone()
    con.close()
    return dict(r) if r else None


def set_stock_engine_for_car(main_db: Path, car_id: int, engine_id: int) -> None:
    """
    Ensures exactly one stock engine row exists for the car:
    - removes any rows for Ordinal=car_id where IsStock=1 and Level=0 (if columns exist)
    - inserts or updates a single row with EngineID=engine_id, IsStock=1, Level=0
    """
    con = _connect(main_db)
    cur = con.cursor()
    if "List_UpgradeEngine" not in set(_list_tables(cur)):
        con.close()
        raise ValueError("List_UpgradeEngine does not exist in MAIN.")

    cols = _cols(_table_info(cur, "List_UpgradeEngine"))
    if "Ordinal" not in cols:
        con.close()
        raise ValueError("List_UpgradeEngine has no Ordinal column.")

    level_col = "Level" if "Level" in cols else None
    isstock_col = "IsStock" if "IsStock" in cols else None
    engine_col = _first_existing_col(cols, ["EngineID", "EngineId", "Engine"])
    if not engine_col:
        con.close()
        raise ValueError("List_UpgradeEngine has no EngineID column.")

    if level_col and isstock_col:
        cur.execute(
            f'DELETE FROM "List_UpgradeEngine" WHERE "Ordinal"=? AND "{isstock_col}"=1 AND "{level_col}"=0',
            (car_id,),
        )

        cur.execute(f'SELECT * FROM "List_UpgradeEngine" WHERE "Ordinal"=? LIMIT 1', (car_id,))
        base = cur.fetchone()

        if base:
            row = dict(base)
            row["Ordinal"] = car_id
            row[engine_col] = engine_id
            row[isstock_col] = 1
            row[level_col] = 0

            # Do NOT insert primary key columns (they are UNIQUE and will collide)
            cols_ins = [c for c in cols if c in row and c not in ("Id", "ID")]
            vals_ins = [row[c] for c in cols_ins]

            cols_sql = ",".join(f'"{c}"' for c in cols_ins)
            placeholders = ",".join(["?"] * len(cols_ins))
            cur.execute(f'INSERT INTO "List_UpgradeEngine" ({cols_sql}) VALUES ({placeholders})', vals_ins)
        else:
            cols_ins = ["Ordinal", engine_col]
            vals_ins = [car_id, engine_id]
            cols_ins.append(isstock_col); vals_ins.append(1)
            cols_ins.append(level_col); vals_ins.append(0)

            cols_sql = ",".join(f'"{c}"' for c in cols_ins)
            placeholders = ",".join(["?"] * len(cols_ins))
            cur.execute(f'INSERT INTO "List_UpgradeEngine" ({cols_sql}) VALUES ({placeholders})', vals_ins)

        con.commit()
        con.close()
        return

    # fallback: update first row
    cur.execute(f'SELECT rowid AS rid FROM "List_UpgradeEngine" WHERE "Ordinal"=? LIMIT 1', (car_id,))
    r = cur.fetchone()
    if r:
        rid = int(r["rid"])
        cur.execute(f'UPDATE "List_UpgradeEngine" SET "{engine_col}"=? WHERE rowid=?', (engine_id, rid))
    else:
        cur.execute(f'INSERT INTO "List_UpgradeEngine" ("Ordinal","{engine_col}") VALUES (?,?)', (car_id, engine_id))

    con.commit()
    con.close()



def list_car_related_tables(main_db: Path) -> List[str]:
    """
    Tables for the editor:
    - All List_Upgrade* tables (even if they don't use Ordinal)
    - Any other table that has Ordinal (car-scoped)
    """
    con = _connect(main_db)
    cur = con.cursor()
    tables = _list_tables(cur)

    out = []
    for t in tables:
        tl = t.lower()
        if tl.startswith("list_upgrade"):
            out.append(t)
            continue
        cols = _cols(_table_info(cur, t))
        if "Ordinal" in cols:
            out.append(t)

    con.close()

    # upgrades first
    out = sorted(set(out), key=lambda x: (0 if x.lower().startswith("list_upgrade") else 1, x.lower()))
    return out

def _has_col(cols: List[str], name: str) -> bool:
    return any(c == name for c in cols)

def detect_scope_for_table(cur: sqlite3.Cursor, table: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (scope_kind, scope_col)
      scope_kind: "car" | "engine" | "carbody" | None
    """
    if table not in set(_list_tables(cur)):
        return (None, None)

    cols = _cols(_table_info(cur, table))

    # Most common
    if "Ordinal" in cols:
        return ("car", "Ordinal")

    # Some tables use explicit CarID columns
    for c in ["CarID", "CarId"]:
        if c in cols:
            return ("car", c)

    # Engine-scoped upgrade tables
    for c in ["EngineID", "EngineId", "Engine"]:
        if c in cols:
            return ("engine", c)

    # CarBody-scoped tables
    for c in ["CarBodyID", "CarBodyId"]:
        if c in cols:
            return ("carbody", c)

    # Drivetrain / powertrain-scoped upgrade tables (clutch, diff, driveline, transmission, etc.)
    for c in ["PowertrainID", "PowertrainId", "DrivetrainID", "DrivetrainId"]:
        if c in cols:
            return ("drivetrain", c)

    return (None, None)


def list_rows_scoped(
    main_db: Path,
    table: str,
    car_id: int,
    engine_id: Optional[int],
    carbody_id: Optional[int],
    drivetrain_id: Optional[int],
) -> Tuple[List[Dict[str, Any]], Optional[str], Optional[str], Optional[int]]:
    """
    Loads rows for a table using the appropriate scope column.
    Returns: (rows, scope_kind, scope_col, scope_value)
    """
    con = _connect(main_db)
    cur = con.cursor()

    if table not in set(_list_tables(cur)):
        con.close()
        return ([], None, None, None)

    scope_kind, scope_col = detect_scope_for_table(cur, table)
    if not scope_kind or not scope_col:
        con.close()
        return ([], None, None, None)

    if scope_kind == "car":
        scope_val = car_id
    elif scope_kind == "engine":
        if engine_id is None:
            con.close()
            return ([], "engine", scope_col, None)
        scope_val = int(engine_id)
    elif scope_kind == "carbody":
        if carbody_id is None:
            con.close()
            return ([], "carbody", scope_col, None)
        scope_val = int(carbody_id)
    elif scope_kind == "drivetrain":
        if drivetrain_id is None:
            con.close()
            return ([], "drivetrain", scope_col, None)
        scope_val = int(drivetrain_id)
    else:
        con.close()
        return ([], None, None, None)


    cur.execute(
        f'SELECT rowid AS "__rowid__", * FROM "{table}" WHERE "{scope_col}"=? ORDER BY rowid',
        (scope_val,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return (rows, scope_kind, scope_col, scope_val)


def list_rows_by_ordinal(main_db: Path, table: str, car_id: int) -> List[Dict[str, Any]]:
    con = _connect(main_db)
    cur = con.cursor()
    if table not in set(_list_tables(cur)):
        con.close()
        return []
    cols = _cols(_table_info(cur, table))
    if "Ordinal" not in cols:
        con.close()
        return []
    cur.execute(f'SELECT rowid AS "__rowid__", * FROM "{table}" WHERE "Ordinal"=? ORDER BY rowid', (car_id,))
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def get_row_by_rowid(main_db: Path, table: str, rowid: int) -> Optional[Dict[str, Any]]:
    con = _connect(main_db)
    cur = con.cursor()
    if table not in set(_list_tables(cur)):
        con.close()
        return None
    cur.execute(f'SELECT rowid AS "__rowid__", * FROM "{table}" WHERE rowid=?', (rowid,))
    r = cur.fetchone()
    con.close()
    return dict(r) if r else None


def update_row_by_rowid(main_db: Path, table: str, rowid: int, updates: Dict[str, Any]) -> None:
    con = _connect(main_db)
    cur = con.cursor()
    if table not in set(_list_tables(cur)):
        con.close()
        raise ValueError(f"Table not found: {table}")

    cols = _cols(_table_info(cur, table))
    upd = {k: v for k, v in updates.items() if k in cols}
    if not upd:
        con.close()
        return

    sets = ", ".join([f'"{k}"=?' for k in upd.keys()])
    cur.execute(f'UPDATE "{table}" SET {sets} WHERE rowid=?', (*upd.values(), rowid))
    con.commit()
    con.close()


def delete_row_by_rowid(main_db: Path, table: str, rowid: int) -> None:
    con = _connect(main_db)
    cur = con.cursor()
    cur.execute(f'DELETE FROM "{table}" WHERE rowid=?', (rowid,))
    con.commit()
    con.close()


def insert_row(main_db: Path, table: str, values: Dict[str, Any]) -> None:
    con = _connect(main_db)
    cur = con.cursor()
    if table not in set(_list_tables(cur)):
        con.close()
        raise ValueError(f"Table not found: {table}")

    cols = _cols(_table_info(cur, table))
    vals = {k: v for k, v in values.items() if k in cols}
    if not vals:
        con.close()
        return

    keys = list(vals.keys())
    placeholders = ",".join(["?"] * len(keys))
    cols_sql = ",".join(f'"{k}"' for k in keys)

    cur.execute(f'INSERT INTO "{table}" ({cols_sql}) VALUES ({placeholders})', [vals[k] for k in keys])
    con.commit()
    con.close()

def apply_subsystem_from_donor(
    main_db: Path,
    donor_db: Path,
    target_car_id: int,
    donor_car_id: int,
    subsystem: str,
    level: int = 0,
):
    """
    Bridges your existing constructor logic into a single call for the new UI.
    Uses your existing _pick_upgrade_table / _find_level_row / _write_level_row and physics cloning behavior.
    """
    con_t = _connect(main_db)
    cur_t = con_t.cursor()

    con_s = _connect(donor_db)
    cur_s = con_s.cursor()

    upgrade_table = _pick_upgrade_table(cur_s, subsystem)
    if not upgrade_table:
        con_s.close(); con_t.close()
        raise ValueError(f"Could not find upgrade table for subsystem '{subsystem}' in {donor_db.name}")

    src_row = _find_level_row(cur_s, upgrade_table, donor_car_id, level)
    if not src_row:
        con_s.close(); con_t.close()
        raise ValueError(f"No donor row found for {subsystem} level {level} (car {donor_car_id})")

    # Write level row into MAIN (target)
    written = _write_level_row(cur_t, upgrade_table, target_car_id, src_row, level)

    # Clone linked physics IDs when present (existing logic below in your file)
    # If your file already calls physics cloning inside _write_level_row or elsewhere, this is extra-safe but optional.
    # We'll attempt to reuse your private function if present.
    notes = []
    if "_clone_physics_ids_from_upgrade_row" in globals():
        try:
            # base-block concept
            base_old = donor_car_id * 1000
            base_new = target_car_id * 1000
            globals()["_clone_physics_ids_from_upgrade_row"](cur_s, cur_t, upgrade_table, src_row, base_old, base_new)
            notes.append("Physics IDs cloned (best-effort).")
        except Exception as e:
            notes.append(f"Physics clone warning: {e}")

    con_t.commit()
    con_s.close(); con_t.close()

    return f"Applied {subsystem} from donor CarID {donor_car_id} ({donor_db.name}) to target CarID {target_car_id} in {upgrade_table} (Level {level}). Rows written: {written}. Notes: {', '.join(notes) if notes else 'none'}"

def list_distinct_engine_medianames(sources: List[Path]) -> List[str]:
    out: set[str] = set()
    for src in sources:
        con = _connect(src)
        cur = con.cursor()
        if "Data_Engine" not in set(_list_tables(cur)):
            con.close()
            continue
        cols = _cols(_table_info(cur, "Data_Engine"))
        if "MediaName" not in cols:
            con.close()
            continue
        cur.execute('SELECT DISTINCT "MediaName" AS m FROM "Data_Engine"')
        for r in cur.fetchall():
            if r["m"] is not None and str(r["m"]).strip():
                out.add(str(r["m"]))
        con.close()
    return sorted(out)
    
def build_powertrain_options(sources: List[Path], lookup_cache: Dict[str, Dict[int, str]]) -> List[Tuple[int, str]]:
    """
    PowertrainID in List_UpgradeDrivetrain appears to refer to Data_Drivetrain.DrivetrainID.
    We build labels using:
      - DrivetrainID
      - DrivetypeID -> List_DriveType (ID -> DriveType)
      - EngineMountingDirection mapped:
          0 Longitudinal, 1 Transverse, 2 Reversed longitudinal
    Also tries to find which CarID block this DrivetrainID belongs to (DrivetrainID//1000 heuristic).
    """
    mount_map = {
        0: "Longitudinal",
        1: "Transverse",
        2: "Reversed longitudinal",
    }
    drive_types = lookup_cache.get("List_DriveType", {})

    items: Dict[int, str] = {}

    for src in sources:
        con = _connect(src)
        cur = con.cursor()
        if "Data_Drivetrain" not in set(_list_tables(cur)):
            con.close()
            continue
        cols = _cols(_table_info(cur, "Data_Drivetrain"))
        if "DrivetrainID" not in cols:
            con.close()
            continue

        dtid_col = "DrivetrainID"
        drivetype_col = "DrivetypeID" if "DrivetypeID" in cols else None
        mount_col = "EngineMountingDirection" if "EngineMountingDirection" in cols else None

        sel = [dtid_col]
        if drivetype_col:
            sel.append(drivetype_col)
        if mount_col:
            sel.append(mount_col)

        sel_sql = ",".join(f'"{c}"' for c in sel)
        cur.execute(f'SELECT {sel_sql} FROM "Data_Drivetrain"')
        for r in cur.fetchall():
            try:
                did = int(r[dtid_col])
            except Exception:
                continue

            if did in items:
                continue

            dt = ""
            if drivetype_col and r[drivetype_col] is not None:
                try:
                    dtid = int(r[drivetype_col])
                    dt = drive_types.get(dtid, str(dtid))
                except Exception:
                    dt = str(r[drivetype_col])

            md = ""
            if mount_col and r[mount_col] is not None:
                try:
                    md = mount_map.get(int(r[mount_col]), str(r[mount_col]))
                except Exception:
                    md = str(r[mount_col])

            car_guess = did // 1000
            parts = []
            if dt:
                parts.append(dt)
            if md:
                parts.append(md)
            parts.append(f"CarBlock~{car_guess}")
            label = " / ".join(parts)

            items[did] = label

        con.close()

    return sorted(items.items(), key=lambda x: x[0])


def basic_validate_car(main_db: Path, car_id: int) -> List[str]:
    """
    Lightweight validator (not a full integrity checker).
    Checks existence of core rows and common crash causes.
    """
    issues: List[str] = []
    car = get_data_car(main_db, car_id)
    if not car:
        issues.append("Data_Car row missing in MAIN.")

    body = get_data_carbody_for_car(main_db, car_id)
    if not body:
        issues.append("Data_CarBody row missing in MAIN (base-block lookup).")

    stock = get_stock_engine_for_car(main_db, car_id)
    if not stock:
        issues.append("Stock engine row missing in List_UpgradeEngine for this car.")
    else:
        eid = stock.get("EngineID") or stock.get("EngineId") or stock.get("Engine")
        if eid is not None:
            try:
                eid_i = int(eid)
                if not engine_exists_in_main(main_db, eid_i):
                    issues.append(f"Stock EngineID {eid_i} not present in MAIN Data_Engine (clone/assign needed).")
            except Exception:
                pass

    return issues

