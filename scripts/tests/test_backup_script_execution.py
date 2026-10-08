from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BACKUP_SCRIPT = ROOT / "scripts" / "bridgeops-backup-postgres.sh"
CONFIG_HELPER = ROOT / "scripts" / "bridgeops-postgres-config.py"
KEY_TABLES = (
    "channels",
    "channel_cost_states",
    "channel_funding_events",
    "gateway_downstream_keys",
    "gateway_upstream_keys",
    "gateway_request_logs",
    "gateway_request_aggregates",
    "system_metadata",
)
REAL_COMMANDS = "awk basename bash cat chmod cmp cp date dirname env find getent grep install mkdir mktemp mv python3 readlink rm sha256sum sh sort stat tar zstd".split()


def process_start_identity(pid: int) -> str:
    process_stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    fields = process_stat[process_stat.rfind(")") + 1 :].split()
    boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(
        encoding="utf-8"
    ).strip()
    return hashlib.sha256(f"{boot_id}\0{fields[19]}".encode()).hexdigest()


class PostgreSQLBackupExecutionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        missing = [
            name
            for name in ("bwrap", "unshare", *REAL_COMMANDS)
            if shutil.which(name) is None
        ]
        if missing:
            raise unittest.SkipTest(f"missing sandbox commands: {', '.join(missing)}")
        probe = subprocess.run(
            ["unshare", "--user", "--map-root-user", "id", "-u"],
            check=False,
            capture_output=True,
            text=True,
        )
        if probe.returncode != 0 or probe.stdout.strip() != "0":
            raise unittest.SkipTest("unprivileged user namespaces are unavailable")

    def test_real_script_reaches_fake_pg_dump_and_creates_new_pair(self) -> None:
        result, trusted = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((trusted / "pg-dump-called").is_file())
        archives = list((trusted / "output").glob("BridgeOps-backup-*.tar.zst"))
        self.assertEqual(len(archives), 1)
        sidecar = Path(str(archives[0]) + ".sha256")
        self.assertTrue(sidecar.is_file())
        expected_hash, expected_name = sidecar.read_text(encoding="utf-8").split()
        self.assertEqual(expected_name, archives[0].name)
        self.assertEqual(hashlib.sha256(archives[0].read_bytes()).hexdigest(), expected_hash)

    def test_real_script_rejects_inactive_and_stale_runtime_before_dump(self) -> None:
        for options in ({"service_active": False}, {"stale_runtime": True}):
            with self.subTest(options=options):
                result, trusted = self.run_script(**options)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((trusted / "pg-dump-called").exists())

    def test_real_script_rejects_helper_under_writable_parent(self) -> None:
        result, trusted = self.run_script(repo_mode=0o777)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((trusted / "pg-dump-called").exists())

    def run_script(
        self,
        *,
        service_active: bool = True,
        stale_runtime: bool = False,
        repo_mode: int = 0o700,
    ) -> tuple[subprocess.CompletedProcess[str], Path]:
        temporary = tempfile.TemporaryDirectory(prefix="bridgeops-script-contract-")
        self.addCleanup(temporary.cleanup)
        base = Path(temporary.name)
        trusted = base / "trusted"
        repository = trusted / "repo"
        scripts = repository / "scripts"
        migrations = repository / "backend" / "migrations"
        output = trusted / "output"
        fake_bin = base / "fake-bin"
        for directory in (scripts, migrations, output, fake_bin):
            directory.mkdir(parents=True, exist_ok=True)
        trusted.chmod(0o700)
        repository.chmod(repo_mode)
        scripts.chmod(0o700)
        migrations.parent.chmod(0o700)
        migrations.chmod(0o700)
        output.chmod(0o700)

        shutil.copy2(BACKUP_SCRIPT, scripts / BACKUP_SCRIPT.name)
        shutil.copy2(CONFIG_HELPER, scripts / CONFIG_HELPER.name)
        (scripts / BACKUP_SCRIPT.name).chmod(0o755)
        (scripts / CONFIG_HELPER.name).chmod(0o755)
        migration = migrations / "000001_contract.sql"
        migration.write_text("CREATE TABLE contract (id bigint);\n", encoding="utf-8")
        migration.chmod(0o644)
        migration_hash = hashlib.sha256(migration.read_bytes()).hexdigest()

        config = trusted / "config.yaml"
        config.write_text(
            textwrap.dedent(
                """
                database:
                  driver: postgres
                  host: 127.0.0.1
                  port: 5432
                  user: bridgeops
                  password: ignored
                  name: bridgeops
                  timeZone: UTC
                  sslMode: disable
                """
            ).lstrip(),
            encoding="utf-8",
        )
        config.chmod(0o600)
        postgres_env = trusted / "postgres.env"
        postgres_env.write_text(
            "DATABASE_DRIVER=postgres\nDATABASE_HOST=127.0.0.1\nDATABASE_PORT=5432\n"
            "DATABASE_USER=bridgeops\nDATABASE_NAME=bridgeops\nDATABASE_TIMEZONE=UTC\n"
            "DATABASE_SSLMODE=disable\nDATABASE_PASSWORD=contract-password\n",
            encoding="utf-8",
        )
        postgres_env.chmod(0o600)
        runtime_status = trusted / "runtime-status.json"
        runtime_status.write_text(
            json.dumps(
                {
                    "schema_version": 3,
                    "pid": os.getpid(),
                    "process_start_id": "a" * 64
                    if stale_runtime
                    else process_start_identity(os.getpid()),
                    "database_driver": "postgres",
                    "bridgeops_version": "0.0.16",
                    "started_at": "2026-07-20T00:00:00Z",
                }
            ),
            encoding="utf-8",
        )
        runtime_status.chmod(0o600)

        for command in REAL_COMMANDS:
            host_command = Path(shutil.which(command) or "").resolve(strict=True)
            self.assertEqual(host_command.parent, Path("/usr/bin"))
            (fake_bin / command).symlink_to(f"/hostbin/{host_command.name}")
        self.write_fakes(fake_bin, migration_hash, service_active)

        environment = {
            "HOME": "/root",
            "PATH": "/usr/bin",
            "BRIDGEOPS_BACKUP_DIR": "/trusted/output",
            "BRIDGEOPS_REPO_DIR": "/trusted/repo",
            "BRIDGEOPS_CONFIG_FILE": "/trusted/config.yaml",
            "BRIDGEOPS_POSTGRES_ENV_FILE": "/trusted/postgres.env",
            "BRIDGEOPS_MIGRATIONS_DIR": "/trusted/repo/backend/migrations",
            "BRIDGEOPS_CONFIG_HELPER": "/trusted/repo/scripts/bridgeops-postgres-config.py",
            "BRIDGEOPS_RUNTIME_STATUS_FILE": "/trusted/runtime-status.json",
            "BRIDGEOPS_SERVICE_NAME": "bridgeops.service",
        }
        command = [
            shutil.which("unshare") or "unshare",
            "--user",
            "--map-root-user",
            shutil.which("bwrap") or "bwrap",
            "--tmpfs",
            "/",
            "--proc",
            "/proc",
            "--dev",
            "/dev",
            "--dir",
            "/usr",
            "--bind",
            str(fake_bin),
            "/usr/bin",
            "--ro-bind",
            "/usr/bin",
            "/hostbin",
            "--ro-bind",
            "/usr/lib",
            "/usr/lib",
            "--ro-bind",
            "/usr/lib64",
            "/usr/lib64",
            "--symlink",
            "usr/bin",
            "/bin",
            "--symlink",
            "usr/lib",
            "/lib",
            "--symlink",
            "usr/lib64",
            "/lib64",
            "--dir",
            "/usr/sbin",
            "--symlink",
            "usr/sbin",
            "/sbin",
            "--ro-bind",
            "/etc",
            "/etc",
            "--dir",
            "/root",
            "--tmpfs",
            "/tmp",
            "--bind",
            str(trusted),
            "/trusted",
            "--chdir",
            "/trusted/repo",
            "/trusted/repo/scripts/bridgeops-backup-postgres.sh",
            "--output",
            "/trusted/output",
        ]
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            env=environment,
            timeout=30,
        )
        return result, trusted

    def write_fakes(self, fake_bin: Path, migration_hash: str, active: bool) -> None:
        active_exit = 0 if active else 3
        self.write_executable(
            fake_bin / "systemctl",
            f"""#!/bin/sh
if [ "$1" = is-active ] && [ "$2" = --quiet ]; then exit {active_exit}; fi
if [ "$1" = show ] && [ "$2" = bridgeops.service ] && [ "$3" = -p ] && [ "$5" = --value ]; then
case "$4" in
LoadState) printf 'loaded\\n' ;;
ExecStart) printf '%s\\n' '{{ path=/usr/local/bin/bridgeops ; argv[]=/usr/local/bin/bridgeops -config /trusted/config.yaml ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }}' ;;
EnvironmentFiles) printf '/trusted/postgres.env (ignore_errors=no)\\n' ;;
Environment) printf 'APP_ENV=production\\n' ;;
PassEnvironment) printf '\\n' ;;
MainPID) printf '{os.getpid()}\\n' ;;
*) exit 2 ;;
esac
exit 0
fi
exit 2
""",
        )
        table_rows = "\\n".join(f"{table}\\tr" for table in KEY_TABLES)
        column_rows = "\\n".join(
            f"{table}\\t1\\tid\\tbigint\\t1\\t\\t" for table in KEY_TABLES
        )
        self.write_executable(
            fake_bin / "psql",
            f"""#!/bin/sh
while [ "$#" -gt 0 ]; do
  if [ "$1" = -c ]; then query=$2; break; fi
  shift
done
case "$query" in
*"SELECT current_database()"*) printf 'bridgeops\\tbridgeops\\t127.0.0.1\\t5432\\tUTC\\t160000\\n' ;;
*"SELECT version, lower(checksum)"*) printf '1\\t{migration_hash}\\n' ;;
*"SELECT value FROM public.system_metadata"*) printf 'UTC\\n' ;;
*"SELECT c.relname, c.relkind"*) printf '%b\\n' '{table_rows}' ;;
*"SELECT c.relname, a.attnum"*) printf '%b\\n' '{column_rows}' ;;
*) exit 80 ;;
esac
""",
        )
        self.write_executable(fake_bin / "pg_isready", "#!/bin/sh\nexit 0\n")
        self.write_executable(
            fake_bin / "pg_dump",
            """#!/bin/sh
for argument in "$@"; do
  case "$argument" in
  --file=*) output=${argument#--file=} ;;
  --dbname=*) database=${argument#--dbname=} ;;
  --format=custom) format=true ;;
  --no-owner) owner=true ;;
  --no-acl) acl=true ;;
  --schema=public) schema=true ;;
  esac
done
[ -n "$output" ] && [ "$database" = bridgeops ] && [ "$format" = true ] && [ "$owner" = true ] && [ "$acl" = true ] && [ "$schema" = true ] || exit 81
printf 'contract dump' > "$output"
printf 'called\n' > /trusted/pg-dump-called
""",
        )
        toc = []
        for index, table in enumerate(KEY_TABLES, start=1):
            toc.append(f"{index * 2 - 1}; 0 0 TABLE public {table} bridgeops")
            toc.append(f"{index * 2}; 0 0 TABLE DATA public {table} bridgeops")
        self.write_executable(
            fake_bin / "pg_restore",
            "#!/bin/sh\n[ \"$1\" = --list ] || exit 82\ncat <<'EOF'\n"
            + "\n".join(toc)
            + "\nEOF\n",
        )

    @staticmethod
    def write_executable(path: Path, body: str) -> None:
        path.write_text(body, encoding="utf-8")
        path.chmod(0o755)


if __name__ == "__main__":
    unittest.main()
