"""
deploy.py — Deploy this dbt project to Snowflake (dbt Projects on Snowflake)
================================================================================
Usage:
    python deploy.py --target test                         # upload + register + `dbt build`
    python deploy.py --target prod --dbt-args "run --select tag:daily"
    python deploy.py --target dev --upload-only            # upload + refresh only
    python deploy.py --target test --execute-only          # skip upload; just execute

Prerequisites:
    pip install mufg_snowflakeconn snowflake-snowpark-python

Design notes:
    - No `dbt deps` runs inside Snowflake (no external network). `dbt_packages/`
      is committed to the repo and uploaded to the stage alongside the rest of
      the project.
    - One database per environment (dev / test / release / prod). --target
      selects the connection context.
    - Each dbt project in the repo gets its own copy of this script (or its
      own constants block) — the two projects deploy independently.
"""
import os
import sys
import argparse
from pathlib import Path

# ─── Per-project configuration ───────────────────────────────────────────────
# Edit this block when copying the script to the second dbt project.
PROJECT_DIR      = Path(__file__).parent
DBT_PROJECT_NAME = 'DBT_PACKAGE'           # Snowflake DBT PROJECT object name
DBT_SCHEMA       = 'DBT'                   # schema holding stage + project object
DBT_STAGE_NAME   = 'DBT_PACKAGE_STAGE'     # stage under that schema

# Map --target → connection context. Adjust role/warehouse/database per env.
TARGETS = {
    'dev': {
        'env':       'dvlp',
        'role':      'DVLP_RAPTOR_OWNER',
        'warehouse': 'DVLP_RAVEN_WH_M',
        'database':  'DVLP_RAPTOR_ANALYTICS',
    },
    'test': {
        'env':       'dvlp',
        'role':      'DVLP_RAPTOR_OWNER',
        'warehouse': 'DVLP_RAVEN_WH_M',
        'database':  'DVLP_RAPTOR_ANALYTICS_TEST',
    },
    'release': {
        'env':       'uat',
        'role':      'UAT_RAPTOR_OWNER',
        'warehouse': 'UAT_RAVEN_WH_M',
        'database':  'UAT_RAPTOR_ANALYTICS',
    },
    'prod': {
        'env':       'prod',
        'role':      'PROD_RAPTOR_OWNER',
        'warehouse': 'PROD_RAVEN_WH_M',
        'database':  'PROD_RAPTOR_ANALYTICS',
    },
}

# Files/dirs NOT uploaded to the stage (local-only artefacts)
UPLOAD_EXCLUDE_DIRS  = {'.git', '.venv', 'venv', 'target', 'logs', '__pycache__', '.vscode', '.idea'}
UPLOAD_EXCLUDE_NAMES = {'deploy.py', '.DS_Store', '.gitignore'}


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


def upload_project_to_stage(session, stage_fqn):
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

    files = list(iter_project_files(PROJECT_DIR))
    expected = {rel.lower() for _, rel in files}

    # ── Remove stale files (files deleted locally) ───────────────────────
    print(f"\n  🧹 Checking for stale files on stage...")
    try:
        staged = session.sql(f"LIST @{stage_fqn}").collect()
        removed = 0
        for row in staged:
            raw = row['name']
            slash_idx = raw.find('/')
            rel_path = raw[slash_idx + 1:] if slash_idx != -1 else raw
            rel_cmp = rel_path[:-3].lower() if rel_path.lower().endswith('.gz') else rel_path.lower()
            if rel_cmp not in expected:
                try:
                    session.sql(f"REMOVE @{stage_fqn}/{rel_path}").collect()
                    print(f"     🗑️  Removed stale: {rel_path}")
                    removed += 1
                except Exception as rm_err:
                    print(f"     ⚠️  Could not remove {rel_path}: {rm_err}")
        if removed == 0:
            print(f"     ✅ No stale files found")
    except Exception as e:
        print(f"     ⚠️  Stage cleanup warning: {e}")

    # ── Upload ───────────────────────────────────────────────────────────
    print(f"\n  📤 Uploading {len(files)} files...")
    errors = 0
    for local_path, rel in files:
        subdir = os.path.dirname(rel)
        stage_path = f'@{stage_fqn}/{subdir}' if subdir else f'@{stage_fqn}'
        try:
            session.file.put(
                str(local_path).replace('\\', '/'),
                stage_path,
                auto_compress=False,
                overwrite=True,
            )
            print(f"     ✅ {rel}")
        except Exception as e:
            print(f"     ❌ {rel}: {e}")
            errors += 1

    if errors:
        print(f"  ⚠️ {errors} files failed to upload")
        return False

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
    try:
        session.sql(
            f"CREATE DBT PROJECT IF NOT EXISTS {project_fqn} FROM '@{stage_fqn}'"
        ).collect()
    except Exception as e:
        print(f"     ❌ Register failed: {e}")
        return False

    try:
        session.sql(f"ALTER DBT PROJECT {project_fqn} REFRESH").collect()
        print(f"     ✅ Project registered & refreshed")
    except Exception as e:
        print(f"     ⚠️  Refresh note: {str(e).split(chr(10))[0][:150]}")
    return True


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
    parser.add_argument('--target', required=True, choices=list(TARGETS.keys()),
                        help='Environment: dev / test / release / prod')
    parser.add_argument('--dbt-args', default='build',
                        help="Args passed to dbt (default: 'build'). "
                             "Examples: 'run --select tag:daily', 'test', 'seed'")
    parser.add_argument('--upload-only', action='store_true',
                        help='Upload files + register project; skip execute')
    parser.add_argument('--execute-only', action='store_true',
                        help='Skip upload; just execute')
    args = parser.parse_args()

    target_cfg  = TARGETS[args.target]
    stage_fqn   = f"{target_cfg['database']}.{DBT_SCHEMA}.{DBT_STAGE_NAME}"
    project_fqn = f"{target_cfg['database']}.{DBT_SCHEMA}.{DBT_PROJECT_NAME}"

    print("=" * 64)
    print(f"  dbt Project Deployment — {DBT_PROJECT_NAME}")
    print(f"  Target:   {args.target}")
    print(f"  Database: {target_cfg['database']}")
    print(f"  Stage:    {stage_fqn}")
    print(f"  Project:  {project_fqn}")
    print("=" * 64)

    # ── Connect ──────────────────────────────────────────────────────────
    print("\n  🔌 Connecting to Snowflake...")
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

    # ── Upload + register ────────────────────────────────────────────────
    if not args.execute_only:
        print("\n" + "─" * 64)
        print("  PHASE 1: Upload project files to stage")
        print("─" * 64)
        if not upload_project_to_stage(session, stage_fqn):
            success = False

        if success:
            print("\n" + "─" * 64)
            print("  PHASE 2: Register DBT PROJECT")
            print("─" * 64)
            if not register_dbt_project(session, project_fqn, stage_fqn):
                success = False

    # ── Execute ──────────────────────────────────────────────────────────
    if not args.upload_only and success:
        print("\n" + "─" * 64)
        print(f"  PHASE 3: Execute dbt — args: {args.dbt_args}")
        print("─" * 64)
        if not execute_dbt_project(session, project_fqn, args.dbt_args):
            success = False

    # ── Summary ──────────────────────────────────────────────────────────
    print("\n" + "=" * 64)
    if success:
        print("  ✅ DEPLOYMENT COMPLETE")
    else:
        print("  ⚠️  DEPLOYMENT COMPLETE WITH ERRORS — Review above.")
    print("=" * 64)

    session.close()
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
