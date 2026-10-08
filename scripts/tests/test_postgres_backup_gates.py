from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CONFIG_HELPER = ROOT / "scripts" / "bridgeops-postgres-config.py"
BACKUP_SCRIPT = ROOT / "scripts" / "bridgeops-backup-postgres.sh"
RESTORE_SCRIPT = ROOT / "scripts" / "bridgeops-restore-postgres.sh"
CLEAN_ENV_MARKER = "__bridgeops_internal_clean_env_v1__"
CONFIG_PATH = "/etc/bridgeops/config.yaml"
POSTGRES_ENV_PATH = "/etc/bridgeops/postgres.env"
EXPECTED_EXEC_START = (
    "{ path=/usr/local/bin/bridgeops ; "
    f"argv[]=/usr/local/bin/bridgeops -config {CONFIG_PATH} ; "
    "ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; "
    "pid=0 ; code=(null) ; status=0/0 }"
)


def process_start_identity(pid: int) -> str:
    process_stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    fields = process_stat[process_stat.rfind(")") + 1 :].split()
    boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    return hashlib.sha256(f"{boot_id}\0{fields[19]}".encode()).hexdigest()


BACKUP_GATE_HARNESS = r"""
source "$1"
SERVICE_NAME=bridgeops.service
CONFIG_FILE=/etc/bridgeops/config.yaml
POSTGRES_ENV_FILE=/etc/bridgeops/postgres.env
CONFIG_HELPER=/trusted/bridgeops-postgres-config.py
RUNTIME_STATUS_FILE=/run/bridgeops/runtime-status.json
systemctl() {
  if [[ "$1" == is-active && "$2" == --quiet && "$3" == "$SERVICE_NAME" ]]; then
    [[ "${TEST_SERVICE_ACTIVE:-0}" == 1 ]]
    return
  fi
  [[ $# -eq 5 && "$1" == show && "$2" == "$SERVICE_NAME" && "$3" == -p && "$5" == --value ]] || return 90
  [[ "${TEST_SYSTEMCTL_FAILURE:-}" != "$4" ]] || return 1
  case "$4" in
    LoadState) printf '%s\n' "$TEST_LOAD_STATE" ;;
    ExecStart) printf '%s\n' "$TEST_EXEC_START" ;;
    EnvironmentFiles) printf '%s\n' "$TEST_ENVIRONMENT_FILES" ;;
    Environment) printf '%s\n' "$TEST_ENVIRONMENT" ;;
    PassEnvironment) printf '%s\n' "$TEST_PASS_ENVIRONMENT" ;;
    MainPID) printf '%s\n' "$TEST_MAIN_PID" ;;
    *) return 91 ;;
  esac
}
python3() {
  [[ $# -eq 8 ]] || return 92
  [[ "$1" == "$CONFIG_HELPER" && "$2" == --config && "$3" == "$CONFIG_FILE" && "$4" == --runtime-status && "$5" == "$RUNTIME_STATUS_FILE" && "$6" == --driver-only && "$7" == --expected-pid && "$8" == "$TEST_MAIN_PID" ]] || return 93
  [[ "${TEST_HELPER_FAILURE:-0}" != 1 ]] || return 1
  printf '%s\n' "$TEST_DRIVER"
}
bridgeops_validate_postgres_service_gate
"""


RESTORE_UNIT_HARNESS = r"""
source "$1"
SERVICE_NAME=bridgeops.service
TARGET_CONFIG=/etc/bridgeops/config.yaml
TARGET_ENV=/etc/bridgeops/postgres.env
systemctl() {
  [[ $# -eq 5 && "$1" == show && "$2" == "$SERVICE_NAME" && "$3" == -p && "$5" == --value ]] || return 90
  [[ "${TEST_SYSTEMCTL_FAILURE:-}" != "$4" ]] || return 1
  case "$4" in
    LoadState) printf '%s\n' "$TEST_LOAD_STATE" ;;
    ExecStart) printf '%s\n' "$TEST_EXEC_START" ;;
    EnvironmentFiles) printf '%s\n' "$TEST_ENVIRONMENT_FILES" ;;
    Environment) printf '%s\n' "$TEST_ENVIRONMENT" ;;
    PassEnvironment) printf '%s\n' "$TEST_PASS_ENVIRONMENT" ;;
    *) return 91 ;;
  esac
}
bridgeops_validate_postgres_restore_unit
"""


RESTORED_RUNTIME_HARNESS = r"""
source "$1"
SERVICE_NAME=bridgeops.service
TARGET_CONFIG=/etc/bridgeops/config.yaml
CONFIG_HELPER=/trusted/bridgeops-postgres-config.py
RUNTIME_STATUS_FILE=/run/bridgeops/runtime-status.json
systemctl() {
  if [[ "$1" == is-active && "$2" == --quiet && "$3" == "$SERVICE_NAME" ]]; then
    [[ "${TEST_SERVICE_ACTIVE:-0}" == 1 ]]
    return
  fi
  [[ $# -eq 5 && "$1" == show && "$2" == "$SERVICE_NAME" && "$3" == -p && "$4" == MainPID && "$5" == --value ]] || return 90
  printf '%s\n' "$TEST_MAIN_PID"
}
python3() {
  [[ $# -eq 8 ]] || return 92
  [[ "$1" == "$CONFIG_HELPER" && "$2" == --config && "$3" == "$TARGET_CONFIG" && "$4" == --runtime-status && "$5" == "$RUNTIME_STATUS_FILE" && "$6" == --driver-only && "$7" == --expected-pid && "$8" == "$TEST_MAIN_PID" ]] || return 93
  [[ "${TEST_HELPER_FAILURE:-0}" != 1 ]] || return 1
  printf '%s\n' "$TEST_DRIVER"
}
bridgeops_validate_postgres_restored_runtime
"""


class ConfigHelperTests(unittest.TestCase):
    def run_helper(self, config: str, **runtime_overrides: object) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory(prefix="bridgeops-config-gate-") as temp_dir:
            base = Path(temp_dir)
            config_path = base / "config.yaml"
            config_path.write_text(textwrap.dedent(config).lstrip(), encoding="utf-8")
            runtime_path = base / "runtime-status.json"
            runtime = {
                "schema_version": 3,
                "pid": os.getpid(),
                "process_start_id": process_start_identity(os.getpid()),
                "database_driver": "postgres",
                "bridgeops_version": "test",
                "started_at": "2026-07-22T00:00:00Z",
            }
            runtime.update(runtime_overrides)
            use_symlink = bool(runtime.pop("symlink", False))
            mode = int(runtime.pop("mode", 0o600))
            target = base / "runtime-target.json" if use_symlink else runtime_path
            target.write_text(json.dumps(runtime), encoding="utf-8")
            target.chmod(mode)
            if target != runtime_path:
                runtime_path.symlink_to(target)
            return subprocess.run(
                [sys.executable, str(CONFIG_HELPER), "--config", str(config_path), "--runtime-status", str(runtime_path), "--driver-only", "--expected-pid", str(os.getpid())],
                check=False,
                capture_output=True,
                text=True,
            )

    def test_postgres_runtime_matches_active_process(self) -> None:
        result = self.run_helper("database:\n  driver: postgres\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "postgres\n")

    def test_non_postgres_duplicate_and_non_string_drivers_fail_closed(self) -> None:
        for config in (
            "database:\n  driver: unsupported\n",
            "database:\n  driver: true\n",
            "database:\n  driver: postgres\n  driver: postgres\n",
        ):
            with self.subTest(config=config):
                result = self.run_helper(config)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")

    def test_runtime_driver_pid_identity_and_file_safety_are_verified(self) -> None:
        cases = (
            {"database_driver": "unsupported"},
            {"pid": 999999},
            {"process_start_id": "a" * 64},
            {"mode": 0o660},
            {"symlink": True},
        )
        for overrides in cases:
            with self.subTest(overrides=overrides):
                result = self.run_helper("database:\n  driver: postgres\n", **overrides)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")


class ConfigEnvironmentTests(unittest.TestCase):
    CONFIG = """
        database:
          driver: postgres
          host: 127.0.0.1
          port: 5432
          user: bridgeops
          name: bridgeops
          timeZone: UTC
          sslMode: disable
    """
    ENV = """
        DATABASE_DRIVER=postgres
        DATABASE_HOST=127.0.0.1
        DATABASE_PORT=5432
        DATABASE_USER=bridgeops
        DATABASE_NAME=bridgeops
        DATABASE_TIMEZONE=UTC
        DATABASE_SSLMODE=disable
        DATABASE_PASSWORD=SECRET_SENTINEL
    """

    def run_inspect(self, config: str, environment: str, connection: bool = False) -> subprocess.CompletedProcess[bytes]:
        with tempfile.TemporaryDirectory(prefix="bridgeops-env-gate-") as temp_dir:
            base = Path(temp_dir)
            config_path = base / "config.yaml"
            env_path = base / "postgres.env"
            config_path.write_text(textwrap.dedent(config).lstrip(), encoding="utf-8")
            env_path.write_text(textwrap.dedent(environment).lstrip(), encoding="utf-8")
            arguments = [sys.executable, str(CONFIG_HELPER), "--config", str(config_path), "--env", str(env_path)]
            if connection:
                arguments.append("--connection")
            return subprocess.run(arguments, check=False, capture_output=True)

    def test_inspection_outputs_reviewed_fields_without_password(self) -> None:
        result = self.run_inspect(self.CONFIG, self.ENV)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(result.stdout.split(b"\0")[:-1], [b"postgres", b"127.0.0.1", b"5432", b"bridgeops", b"bridgeops", b"UTC", b"disable"])
        self.assertNotIn(b"SECRET_SENTINEL", result.stdout)
        self.assertNotIn(b"SECRET_SENTINEL", result.stderr)

    def test_mismatch_duplicate_and_missing_credentials_fail_without_secret_output(self) -> None:
        for environment in (
            self.ENV.replace("DATABASE_PORT=5432", "DATABASE_PORT=5433"),
            self.ENV + "DATABASE_HOST=127.0.0.1\n",
            self.ENV.replace("DATABASE_PASSWORD=SECRET_SENTINEL\n", ""),
        ):
            with self.subTest(environment=environment):
                result = self.run_inspect(self.CONFIG, environment)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, b"")
                self.assertNotIn(b"SECRET_SENTINEL", result.stderr)


class GateHarnessTests(unittest.TestCase):
    def base_environment(self) -> dict[str, str]:
        environment = os.environ.copy()
        environment.update({
            "TEST_LOAD_STATE": "loaded",
            "TEST_EXEC_START": EXPECTED_EXEC_START,
            "TEST_ENVIRONMENT_FILES": f"{POSTGRES_ENV_PATH} (ignore_errors=no)",
            "TEST_ENVIRONMENT": "APP_ENV=production",
            "TEST_PASS_ENVIRONMENT": "",
            "TEST_SYSTEMCTL_FAILURE": "",
            "TEST_SERVICE_ACTIVE": "1",
            "TEST_MAIN_PID": "4321",
            "TEST_DRIVER": "postgres",
            "TEST_HELPER_FAILURE": "0",
        })
        return environment

    def run_gate(self, harness: str, script: Path, **overrides: str) -> subprocess.CompletedProcess[str]:
        environment = self.base_environment()
        environment.update(overrides)
        return subprocess.run(
            ["/bin/bash", "--noprofile", "--norc", "-c", harness, "bridgeops-postgres-gate-test", str(script)],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )

    def assert_rejected(self, harness: str, script: Path, **overrides: str) -> None:
        result = self.run_gate(harness, script, **overrides)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertEqual(result.stdout, "")

    def test_backup_gate_accepts_only_exact_active_postgres_service(self) -> None:
        result = self.run_gate(BACKUP_GATE_HARNESS, BACKUP_SCRIPT)
        self.assertEqual(result.returncode, 0, result.stderr)
        for overrides in (
            {"TEST_SERVICE_ACTIVE": "0"},
            {"TEST_MAIN_PID": "0"},
            {"TEST_DRIVER": "unsupported"},
            {"TEST_HELPER_FAILURE": "1"},
            {"TEST_EXEC_START": EXPECTED_EXEC_START + " { path=/bin/true ; argv[]=/bin/true ; }"},
            {"TEST_ENVIRONMENT_FILES": f"{POSTGRES_ENV_PATH} (ignore_errors=no) /tmp/extra.env (ignore_errors=no)"},
            {"TEST_ENVIRONMENT": 'APP_ENV=production "DATABASE_HOST=other"'},
            {"TEST_PASS_ENVIRONMENT": "DATABASE_PASSWORD"},
            {"TEST_SYSTEMCTL_FAILURE": "EnvironmentFiles"},
        ):
            with self.subTest(overrides=overrides):
                self.assert_rejected(BACKUP_GATE_HARNESS, BACKUP_SCRIPT, **overrides)

    def test_restore_unit_gate_rejects_unreviewed_service_configuration(self) -> None:
        result = self.run_gate(RESTORE_UNIT_HARNESS, RESTORE_SCRIPT)
        self.assertEqual(result.returncode, 0, result.stderr)
        for overrides in (
            {"TEST_EXEC_START": EXPECTED_EXEC_START.replace(CONFIG_PATH, CONFIG_PATH + ".old")},
            {"TEST_ENVIRONMENT_FILES": f"{POSTGRES_ENV_PATH} (ignore_errors=no) /tmp/extra.env (ignore_errors=no)"},
            {"TEST_ENVIRONMENT": '"DATABASE_HOST=other"'},
            {"TEST_PASS_ENVIRONMENT": "DATABASE_PASSWORD"},
            {"TEST_SYSTEMCTL_FAILURE": "PassEnvironment"},
        ):
            with self.subTest(overrides=overrides):
                self.assert_rejected(RESTORE_UNIT_HARNESS, RESTORE_SCRIPT, **overrides)

    def test_restored_runtime_requires_active_pid_and_postgres_driver(self) -> None:
        result = self.run_gate(RESTORED_RUNTIME_HARNESS, RESTORE_SCRIPT)
        self.assertEqual(result.returncode, 0, result.stderr)
        for overrides in (
            {"TEST_SERVICE_ACTIVE": "0"},
            {"TEST_MAIN_PID": "0"},
            {"TEST_DRIVER": "unsupported"},
            {"TEST_HELPER_FAILURE": "1"},
        ):
            with self.subTest(overrides=overrides):
                self.assert_rejected(RESTORED_RUNTIME_HARNESS, RESTORE_SCRIPT, **overrides)


class CleanEnvironmentTests(unittest.TestCase):
    def test_exported_shell_functions_are_removed_with_forged_marker(self) -> None:
        for script in (BACKUP_SCRIPT, RESTORE_SCRIPT):
            with self.subTest(script=script.name):
                environment = os.environ.copy()
                for name in ("BASH_ENV", "ENV", "LD_PRELOAD", "LD_LIBRARY_PATH", "PYTHONHOME", "PYTHONPATH", "PYTHONSTARTUP"):
                    environment.pop(name, None)
                environment["BRIDGEOPS_CLEAN_ENV"] = "1"
                environment["BASH_FUNC_cat%%"] = "() { printf 'INJECTED_FUNCTION'; }"
                result = subprocess.run(
                    ["/bin/bash", str(script), CLEAN_ENV_MARKER, "--help"],
                    check=False,
                    capture_output=True,
                    text=True,
                    env=environment,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("Usage:", result.stdout)
                self.assertNotIn("INJECTED_FUNCTION", result.stdout)


if __name__ == "__main__":
    unittest.main()
