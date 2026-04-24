"""
deploy.py — Deploy a dbt project to Snowflake (dbt Projects on Snowflake)
================================================================================
What this script does, per invocation (single project):

    PHASE 1  Upload project files → internal Snowflake stage
             • CREATE SCHEMA / CREATE STAGE if missing
             • REMOVE files that exist on-stage but not locally (stale cleanup)
             • PUT every local file (incl. dbt_packages/) to the stage
             • ALTER STAGE ... REFRESH so DBT PROJECT sees the new directory

    PHASE 2  Register the DBT PROJECT object
             • CREATE DBT PROJECT IF NOT EXISTS <name> FROM '@stage'
             • ALTER DBT PROJECT <name> REFRESH  (picks up the uploaded code)

    PHASE 3  Execute dbt inside Snowflake
             • EXECUTE DBT PROJECT <name> ARGS='<dbt_args>'  (default: 'build')

Connection context (role / warehouse / database) is derived from --target and
--db-type using the pattern documented on `build_target_cfg`.

Usage examples:
    python deploy.py --target dvlp --suffix 25_04 \\
                     --project-name dbt_package --project-dir ./dbt_package
    python deploy.py --target prod \\
                     --project-name dbt_package --project-dir ./dbt_package \\
                     --dbt-args "run --select tag:daily"

Prerequisites:
    pip install mufg_snowflakeconn snowflake-snowpark-python

Design notes:
    - No `dbt deps` runs inside Snowflake (no external network access from the
      compute). `dbt_packages/` is committed to the repo and uploaded to the
      stage alongside the rest of the project.
    - Shared across multiple dbt projects — the PowerShell wrapper
      (`deploy_all.ps1`) loops and invokes this script once per project.
"""
import os
import sys
import time
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

# Upload tuning — concurrent PUT calls. Each file is one PUT; every file is
# dispatched to a worker thread. Bump this up if the agent has bandwidth.
UPLOAD_WORKERS = 16

# ─── Fixed configuration ─────────────────────────────────────────────────────
DBT_SCHEMA     = 'DBT'                  # schema holding the stage + DBT PROJECT object
DEFAULT_DBTYPE = 'RAPTOR'               # override via --db-type
VALID_TARGETS  = ['dvlp', 'test', 'rlse', 'prod']

# Files/dirs NOT uploaded to the stage (local-only artefacts)
UPLOAD_EXCLUDE_DIRS  = {'.git', '.venv', 'venv', 'target', 'logs',
                       '__pycache__', '.vscode', '.idea'}
UPLOAD_EXCLUDE_NAMES = {'deploy.py', '.DS_Store', '.gitignore'}


def build_target_cfg(target: str, db_type: str, suffix: str) -> dict:
    """
    Build the Snowflake connection context from --target, --db-type, --suffix.

    Pattern:
        env       = target                           e.g. 'dvlp'
        role      = <TARGET>_<DBTYPE>_OWNER          e.g. 'DVLP_RAPTOR_OWNER'
        warehouse = <TARGET>_<DBTYPE>_WH_M           e.g. 'DVLP_RAPTOR_WH_M'
        database  = <TARGET>_<DBTYPE>[_<SUFFIX>]     e.g. 'DVLP_RAPTOR_25_04'

    Suffix is appended for every target except prod.
    """
    t = target.upper()
    d = db_type.upper()
    cfg = {
        'env':       target,
        'role':      f'{t}_{d}_OWNER',
        'warehouse': f'{t}_{d}_WH_M',
        'database':  f'{t}_{d}',
    }
    if target != 'prod' and suffix:
        cfg['database'] = f"{cfg['database']}_{suffix}"
    return cfg


# ─── Pretty-print helpers ────────────────────────────────────────────────────

def _ts() -> str:
    """Timestamp prefix for phase banners."""
    return datetime.now().strftime('%H:%M:%S')


def _banner(text: str, char: str = '=', width: int = 72) -> None:
    print(char * width)
    print(f"  {text}")
    print(char * width)


def _phase(step: int, total: int, title: str) -> None:
    """Print a phase header like '[PHASE 1/3] 10:15:23 — Upload ...'."""
    print()
    print("─" * 72)
    print(f"  [PHASE {step}/{total}] {_ts()}  —  {title}")
    print("─" * 72)


# ─── Connection ──────────────────────────────────────────────────────────────

def get_session(target_cfg):
    """Create a Snowpark session using the MUFG connector."""
    from mufg_snowflakeconn import sfconnection as m_sf
    mufgconn = m_sf.MufgSnowflakeConn(target_cfg['env'], 'apd_raptor_sfk_depl@mufgsecurities.com')
    session = mufgconn.get_snowflake_session()
    session.use_role(target_cfg['role'])
    session.use_warehouse(target_cfg['warehouse'])
    session.use_database(target_cfg['database'])
    return session


# ─── Upload project files to stage ───────────────────────────────────────────

def iter_project_files(project_dir: Path):
    """Yield (absolute_local_path, relative_posix_path) for every file to upload."""
    for path in project_dir.rglob('*'):
        if not path.is_file():
            continue
        rel_parts = path.relative_to(project_dir).parts
        if any(part in UPLOAD_EXCLUDE_DIRS for part in rel_parts):
            continue
        if path.name in UPLOAD_EXCLUDE_NAMES:
            continue
        yield path, path.relative_to(project_dir).as_posix()


def upload_project_to_stage(session, stage_fqn, project_dir: Path):
    """Create the stage if needed, remove stale files, PUT every project file."""
    print(f"\n  📦 Ensuring stage {stage_fqn}...")
    try:
        session.sql(
            f"CREATE SCHEMA IF NOT EXISTS {DBT_SCHEMA}"
        ).collect()
        session.sql(
            f"CREATE STAGE IF NOT EXISTS {stage_fqn} "
            f"ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') "
            f"DIRECTORY = (ENABLE = TRUE)"
        ).collect()
        print(f"     ✅ Stage ready")
    except Exception as e:
        print(f"     ❌ Stage creation failed: {e}")
        return False

    files = list(iter_project_files(project_dir))

    # ── Wipe the stage clean before re-uploading ─────────────────────────
    print(f"\n  🧹 Clearing stage {stage_fqn}...")
    try:
        session.sql(f"REMOVE @{stage_fqn}").collect()
        print(f"     ✅ Stage cleared")
    except Exception as e:
        print(f"     ⚠️  Stage cleanup warning: {str(e).splitlines()[0][:160]}")

    # ── Upload ───────────────────────────────────────────────────────────
    # Flat parallel upload: every file is a separate PUT, dispatched to a
    # thread pool. Fastest pattern for deep trees with many small files.
    print(f"\n  📤 Uploading {len(files)} files (workers={UPLOAD_WORKERS})...")

    def upload_one(item):
        local_path, rel = item
        subdir     = os.path.dirname(rel)
        stage_path = f'@{stage_fqn}/{subdir}' if subdir else f'@{stage_fqn}'
        try:
            session.file.put(
                str(local_path).replace('\\', '/'), stage_path,
                auto_compress=False, overwrite=True,
            )
            return None
        except Exception as e:
            return (rel, str(e).splitlines()[0][:160])

    uploaded = 0
    errors   = []
    with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as ex:
        for result in ex.map(upload_one, files):
            if result is None:
                uploaded += 1
            else:
                rel, msg = result
                print(f"     ❌ {rel}: {msg}")
                errors.append(rel)

    # ── Verify everything landed on the stage ────────────────────────────
    print(f"\n  🔎 Verifying stage contents...")
    try:
        staged = session.sql(f"LIST @{stage_fqn}").collect()
        stage_count = len(staged)
    except Exception as e:
        stage_count = -1
        print(f"     ⚠️  Could not LIST stage: {str(e).splitlines()[0][:160]}")

    if errors:
        print(f"  ⚠️ {len(errors)} file(s) failed to upload (see above)")
        return False
    if stage_count != -1 and stage_count != len(files):
        print(f"  ⚠️ Stage count {stage_count} does not match local count {len(files)}")
        return False
    print(f"     ✅ Uploaded {uploaded}/{len(files)} files — stage holds {stage_count}")

    # Refresh directory so DBT PROJECT sees new files
    try:
        session.sql(f"ALTER STAGE {stage_fqn} REFRESH").collect()
    except Exception:
        pass

    return True


# ─── Register & execute the dbt project ──────────────────────────────────────

def register_dbt_project(session, project_fqn, stage_fqn):
    """Create (or refresh) the DBT PROJECT object pointing at the stage."""
    print(f"\n  📚 Registering DBT PROJECT {project_fqn}...")
    create_sql = f"CREATE DBT PROJECT IF NOT EXISTS {project_fqn} FROM '@{stage_fqn}'"
    try:
        session.sql(create_sql).collect()
    except Exception as e:
        print(f"     ❌ CREATE failed. SQL:\n        {create_sql}")
        print(f"     ❌ Error:\n{e}")
        return False

    refresh_sql = f"ALTER DBT PROJECT {project_fqn} REFRESH"
    try:
        session.sql(refresh_sql).collect()
        print(f"     ✅ Project registered & refreshed")
        return True
    except Exception as e:
        print(f"     ❌ REFRESH failed. SQL:\n        {refresh_sql}")
        print(f"     ❌ Error:\n{e}")
        return False


def execute_dbt_project(session, project_fqn, dbt_args):
    """Run `EXECUTE DBT PROJECT ... ARGS='...'` and surface the output."""
    print(f"\n  🚀 Executing: dbt {dbt_args}")
    safe_args = dbt_args.replace("'", "''")
    try:
        rows = session.sql(
            f"EXECUTE DBT PROJECT {project_fqn} ARGS='{safe_args}'"
        ).collect()
        for row in rows[:50]:
            print(f"     {row}")
        print(f"     ✅ dbt execution finished")
        return True
    except Exception as e:
        print(f"     ❌ dbt execution failed: {str(e).split(chr(10))[0][:200]}")
        return False


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Deploy dbt project to Snowflake (dbt Projects on Snowflake)'
    )
    parser.add_argument('--target', required=True, choices=VALID_TARGETS,
                        help='Environment: dvlp / test / rlse / prod')
    parser.add_argument('--project-name', required=True,
                        help='dbt project identifier. Drives the Snowflake DBT PROJECT '
                             'object name and the stage name.')
    parser.add_argument('--project-dir', required=True,
                        help='Path to the dbt project folder (contains dbt_project.yml).')
    parser.add_argument('--db-type', default=DEFAULT_DBTYPE,
                        help=f"DB type component used in role/warehouse/database names "
                             f"(default: {DEFAULT_DBTYPE} → <ENV>_{DEFAULT_DBTYPE}_OWNER etc.)")
    parser.add_argument('--suffix',
                        help="Suffix appended to the database as <DB>_<suffix>. "
                             "Required for every target except prod.")
    parser.add_argument('--dbt-args', default='build',
                        help="Args passed to dbt (default: 'build'). "
                             "Examples: 'run --select tag:daily', 'test', 'seed'")
    parser.add_argument('--upload-only', action='store_true',
                        help='Upload files + register project; skip execute')
    parser.add_argument('--execute-only', action='store_true',
                        help='Skip upload; just execute')
    args = parser.parse_args()

    project_dir = Path(args.project_dir).resolve()
    if not (project_dir / 'dbt_project.yml').is_file():
        print(f"  ❌ No dbt_project.yml in {project_dir}")
        sys.exit(2)

    project_name_sf = args.project_name.strip().upper().replace('-', '_')
    stage_name      = f"{project_name_sf}_STAGE"

    suffix = (args.suffix or '').strip().strip('_')
    if args.target != 'prod' and not suffix:
        print(f"  ❌ --suffix is required for target '{args.target}' (only 'prod' may omit it)")
        sys.exit(2)

    target_cfg  = build_target_cfg(args.target, args.db_type, suffix)
    stage_fqn   = f"{target_cfg['database']}.{DBT_SCHEMA}.{stage_name}"
    project_fqn = f"{target_cfg['database']}.{DBT_SCHEMA}.{project_name_sf}"

    # Work out which phases will actually run so PHASE headers are numbered
    # correctly (e.g. "[PHASE 1/2]" when --upload-only skips execution).
    phases = []
    if not args.execute_only:
        phases += ['upload', 'register']
    if not args.upload_only:
        phases += ['execute']
    total_phases = len(phases)
    phase_idx    = {name: i + 1 for i, name in enumerate(phases)}

    # ── Header banner ────────────────────────────────────────────────────
    _banner(f"dbt Project Deployment — {project_name_sf}")
    print(f"  Started    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Source     : {project_dir}")
    print(f"  Target     : {args.target}   (db-type: {args.db_type.upper()})")
    if suffix:
        print(f"  Suffix     : {suffix}")
    print(f"  Role       : {target_cfg['role']}")
    print(f"  Warehouse  : {target_cfg['warehouse']}")
    print(f"  Database   : {target_cfg['database']}")
    print(f"  Stage      : {stage_fqn}")
    print(f"  DBT PROJECT: {project_fqn}")
    print(f"  dbt args   : {args.dbt_args}")
    print("=" * 72)

    t0 = time.monotonic()

    # ── Connect ──────────────────────────────────────────────────────────
    print(f"\n  🔌 [{_ts()}] Connecting to Snowflake...")
    try:
        session = get_session(target_cfg)
        ctx = session.sql(
            "SELECT CURRENT_ROLE() AS R, CURRENT_WAREHOUSE() AS W, CURRENT_DATABASE() AS D"
        ).collect()[0]
        print(f"     ✅ Connected — Role: {ctx['R']}, Warehouse: {ctx['W']}, Database: {ctx['D']}")
    except Exception as e:
        print(f"     ❌ Connection failed: {e}")
        sys.exit(1)

    success = True

    # ── PHASE: Upload + register ─────────────────────────────────────────
    if not args.execute_only:
        _phase(phase_idx['upload'], total_phases, "Upload project files to Snowflake stage")
        if not upload_project_to_stage(session, stage_fqn, project_dir):
            success = False

        if success:
            _phase(phase_idx['register'], total_phases, "Register DBT PROJECT object")
            if not register_dbt_project(session, project_fqn, stage_fqn):
                success = False

    # ── PHASE: Execute ───────────────────────────────────────────────────
    if not args.upload_only and success:
        _phase(phase_idx['execute'], total_phases, f"Execute dbt — 'dbt {args.dbt_args}'")
        if not execute_dbt_project(session, project_fqn, args.dbt_args):
            success = False

    # ── Summary ──────────────────────────────────────────────────────────
    elapsed = time.monotonic() - t0
    mm, ss  = divmod(int(elapsed), 60)
    print()
    _banner(
        f"{'✅ DEPLOYMENT COMPLETE' if success else '⚠️  DEPLOYMENT COMPLETE WITH ERRORS'}"
        f"  —  elapsed {mm:02d}:{ss:02d}"
    )

    session.close()
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
