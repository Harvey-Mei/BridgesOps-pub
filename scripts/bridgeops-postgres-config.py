#!/usr/bin/env python3
"""Validate matching BridgeOps YAML and PostgreSQL environment settings."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import stat
import sys
from pathlib import Path
from typing import Any

import yaml


class UniqueKeyLoader(yaml.SafeLoader):
    pass


def construct_unique_mapping(
    loader: UniqueKeyLoader, node: yaml.MappingNode, deep: bool = False
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ValueError(f"duplicate YAML key: {key}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, construct_unique_mapping
)

ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SAFE_TEXT_RE = re.compile(r"^[^\x00-\x1f\x7f]+$")
VALID_SSL_MODES = {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}
YAML_STRING_TAG = "tag:yaml.org,2002:str"


def fail(message: str) -> None:
    raise ValueError(message)


def read_yaml(path: Path) -> dict[str, Any]:
    try:
        document = yaml.load(path.read_text(encoding="utf-8"), Loader=UniqueKeyLoader)
    except OSError as exc:
        fail(f"cannot read BridgeOps YAML config: {exc.strerror or 'I/O error'}")
    except UnicodeError:
        fail("BridgeOps YAML config is not valid UTF-8")
    except yaml.MarkedYAMLError as exc:
        mark = exc.problem_mark
        location = f" at line {mark.line + 1}, column {mark.column + 1}" if mark else ""
        fail(f"cannot parse BridgeOps YAML config{location}")
    except (TypeError, ValueError) as exc:
        fail(f"cannot parse BridgeOps YAML config: {exc}")
    if not isinstance(document, dict):
        fail("BridgeOps YAML config must contain a mapping")
    database = document.get("database")
    if not isinstance(database, dict):
        fail("BridgeOps YAML config must contain a database mapping")
    return database


def validate_unique_mapping_keys(
    node: yaml.Node, visited: set[int] | None = None
) -> None:
    if visited is None:
        visited = set()
    node_id = id(node)
    if node_id in visited:
        return
    visited.add(node_id)

    if isinstance(node, yaml.MappingNode):
        keys: set[str] = set()
        for key_node, value_node in node.value:
            if not isinstance(key_node, yaml.ScalarNode) or key_node.tag != YAML_STRING_TAG:
                fail("BridgeOps YAML config mapping keys must be strings")
            if key_node.value in keys:
                fail("BridgeOps YAML config contains a duplicate key")
            keys.add(key_node.value)
            validate_unique_mapping_keys(value_node, visited)
    elif isinstance(node, yaml.SequenceNode):
        for child in node.value:
            validate_unique_mapping_keys(child, visited)


def mapping_value(node: yaml.MappingNode, key: str) -> yaml.Node | None:
    for key_node, value_node in node.value:
        if key_node.value == key:
            return value_node
    return None


def read_driver_only(path: Path) -> str:
    try:
        with path.open("r", encoding="utf-8") as stream:
            document = yaml.compose(stream, Loader=yaml.SafeLoader)
    except OSError as exc:
        fail(f"cannot read BridgeOps YAML config: {exc.strerror or 'I/O error'}")
    except UnicodeError:
        fail("BridgeOps YAML config is not valid UTF-8")
    except yaml.MarkedYAMLError as exc:
        mark = exc.problem_mark
        location = f" at line {mark.line + 1}, column {mark.column + 1}" if mark else ""
        fail(f"cannot parse BridgeOps YAML config{location}")
    except yaml.YAMLError:
        fail("cannot parse BridgeOps YAML config")

    if not isinstance(document, yaml.MappingNode):
        fail("BridgeOps YAML config must contain a mapping")
    validate_unique_mapping_keys(document)
    database = mapping_value(document, "database")
    if not isinstance(database, yaml.MappingNode):
        fail("BridgeOps YAML config must contain a database mapping")
    driver_node = mapping_value(database, "driver")
    if not isinstance(driver_node, yaml.ScalarNode) or driver_node.tag != YAML_STRING_TAG:
        fail("database.driver must be a string")
    driver = driver_node.value.strip().lower()
    if driver != "postgres":
        fail("database.driver must be postgres")
    return driver


def unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            fail("runtime status contains a duplicate field")
        result[key] = value
    return result


def process_start_identity(pid: int) -> str:
    if pid < 1:
        fail("BridgeOps process PID must be positive")
    try:
        process_stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(
            encoding="utf-8"
        ).strip()
    except (OSError, UnicodeError):
        fail("cannot read BridgeOps process start identity")
    closing_parenthesis = process_stat.rfind(")")
    if closing_parenthesis < 0:
        fail("BridgeOps process stat is invalid")
    fields = process_stat[closing_parenthesis + 1 :].split()
    if len(fields) <= 19 or not fields[19].isdigit() or not boot_id:
        fail("BridgeOps process start identity is invalid")
    return hashlib.sha256(f"{boot_id}\0{fields[19]}".encode("utf-8")).hexdigest()


def read_runtime_driver(path: Path, expected_pid: int | None) -> str:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        fail("cannot open BridgeOps runtime status")
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            fail("BridgeOps runtime status must be a regular file")
        if info.st_uid != os.geteuid() or info.st_mode & 0o022:
            fail("BridgeOps runtime status has unsafe ownership or permissions")
        if info.st_size < 2 or info.st_size > 4096:
            fail("BridgeOps runtime status has an invalid size")
        with os.fdopen(descriptor, "r", encoding="utf-8") as stream:
            descriptor = -1
            document = json.load(stream, object_pairs_hook=unique_json_object)
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError):
        fail("cannot parse BridgeOps runtime status")
    finally:
        if descriptor >= 0:
            os.close(descriptor)

    if not isinstance(document, dict) or document.get("schema_version") != 3:
        fail("BridgeOps runtime status schema is invalid")
    pid = document.get("pid")
    process_start_id = document.get("process_start_id")
    driver = document.get("database_driver")
    version = document.get("bridgeops_version")
    started_at = document.get("started_at")
    if isinstance(pid, bool) or not isinstance(pid, int) or pid < 1:
        fail("BridgeOps runtime status PID is invalid")
    if expected_pid is not None and pid != expected_pid:
        fail("BridgeOps runtime status does not belong to the active service")
    if not isinstance(process_start_id, str) or not re.fullmatch(
        r"[0-9a-f]{64}", process_start_id
    ):
        fail("BridgeOps runtime process start identity is invalid")
    if expected_pid is not None and process_start_identity(pid) != process_start_id:
        fail("BridgeOps runtime status has a stale process start identity")
    if driver != "postgres":
        fail("BridgeOps runtime database driver is invalid")
    if not isinstance(version, str) or not version or len(version) > 64:
        fail("BridgeOps runtime version is invalid")
    if not isinstance(started_at, str) or not started_at or len(started_at) > 64:
        fail("BridgeOps runtime start time is invalid")
    return driver


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        fail(f"cannot read PostgreSQL environment file: {exc}")

    for line_number, original in enumerate(lines, start=1):
        line = original.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, separator, raw_value = line.partition("=")
        key = key.strip()
        if not separator or not ENV_KEY_RE.fullmatch(key):
            fail(f"invalid environment assignment at line {line_number}")
        if key in values:
            fail(f"duplicate environment variable: {key}")
        raw_value = raw_value.strip()
        if raw_value == "":
            value = ""
        else:
            try:
                tokens = shlex.split(raw_value, comments=False, posix=True)
            except ValueError as exc:
                fail(f"invalid quoted environment value at line {line_number}: {exc}")
            if len(tokens) != 1:
                fail(f"environment value at line {line_number} must be one scalar")
            value = tokens[0]
        if "\x00" in value or "\n" in value or "\r" in value:
            fail(f"environment value at line {line_number} contains a control character")
        values[key] = value
    return values


def required_yaml_scalar(database: dict[str, Any], key: str) -> str:
    if key not in database:
        fail(f"database.{key} must be explicit in BridgeOps YAML config")
    value = database[key]
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        fail(f"database.{key} must be a scalar")
    text = str(value).strip()
    if not text:
        fail(f"database.{key} must not be empty")
    return text


def resolve_env(values: dict[str, str], field: str, aliases: tuple[str, ...]) -> str:
    present = [(key, values[key].strip()) for key in aliases if key in values]
    if not present:
        fail(f"PostgreSQL environment file is missing {aliases[0]}")
    if any(value != present[0][1] for _, value in present[1:]):
        fail(f"conflicting PostgreSQL environment aliases for {field}")
    if not present[0][1]:
        fail(f"PostgreSQL environment field {field} must not be empty")
    return present[0][1]


def optional_env(values: dict[str, str], field: str, aliases: tuple[str, ...]) -> str:
    present = [(key, values[key]) for key in aliases if key in values]
    if not present:
        return ""
    if any(value != present[0][1] for _, value in present[1:]):
        fail(f"conflicting PostgreSQL environment aliases for {field}")
    return present[0][1]


def validate_text(field: str, value: str) -> str:
    if not SAFE_TEXT_RE.fullmatch(value):
        fail(f"database field {field} contains unsupported control characters")
    return value


def inspect(config_path: Path, env_path: Path) -> tuple[list[str], str, str]:
    database = read_yaml(config_path)
    env = read_env(env_path)

    config_values = {
        "driver": required_yaml_scalar(database, "driver").lower(),
        "host": required_yaml_scalar(database, "host"),
        "port": required_yaml_scalar(database, "port"),
        "user": required_yaml_scalar(database, "user"),
        "database": required_yaml_scalar(database, "name"),
        "timezone": required_yaml_scalar(database, "timeZone"),
        "sslmode": required_yaml_scalar(database, "sslMode").lower(),
    }
    env_values = {
        "driver": resolve_env(env, "driver", ("DATABASE_DRIVER",)).lower(),
        "host": resolve_env(env, "host", ("DATABASE_HOST", "PGHOST")),
        "port": resolve_env(env, "port", ("DATABASE_PORT", "PGPORT")),
        "user": resolve_env(env, "user", ("DATABASE_USER", "PGUSER")),
        "database": resolve_env(env, "database", ("DATABASE_NAME", "PGDATABASE")),
        "timezone": resolve_env(
            env, "timezone", ("DATABASE_TIMEZONE", "DATABASE_TIME_ZONE")
        ),
        "sslmode": resolve_env(
            env, "sslmode", ("DATABASE_SSLMODE", "DATABASE_SSL_MODE", "PGSSLMODE")
        ).lower(),
    }

    for field, config_value in config_values.items():
        env_value = env_values[field]
        if field == "timezone":
            matches = config_value.upper() == env_value.upper()
        else:
            matches = config_value == env_value
        if not matches:
            fail(f"PostgreSQL environment does not match database.{field}")

    if config_values["driver"] != "postgres":
        fail("database.driver must be postgres")
    try:
        port = int(config_values["port"], 10)
    except ValueError:
        fail("database.port must be an integer")
    if port < 1 or port > 65535 or str(port) != config_values["port"]:
        fail("database.port must be a canonical integer from 1 through 65535")
    config_values["port"] = str(port)
    if config_values["timezone"].upper() != "UTC":
        fail("database.timeZone must be UTC for PostgreSQL")
    config_values["timezone"] = "UTC"
    if config_values["sslmode"] not in VALID_SSL_MODES:
        fail("database.sslMode is invalid for PostgreSQL")

    ordered = [
        validate_text(field, config_values[field])
        for field in ("driver", "host", "port", "user", "database", "timezone", "sslmode")
    ]
    password = optional_env(env, "password", ("DATABASE_PASSWORD", "PGPASSWORD"))
    passfile = optional_env(env, "passfile", ("PGPASSFILE",))
    if not password and not passfile:
        fail("PostgreSQL environment must provide DATABASE_PASSWORD/PGPASSWORD or PGPASSFILE")
    if passfile and not Path(passfile).is_absolute():
        fail("PGPASSFILE must be an absolute path")
    for field, value in (("password", password), ("passfile", passfile)):
        if value and not SAFE_TEXT_RE.fullmatch(value):
            fail(f"PostgreSQL {field} contains unsupported control characters")
    return ordered, password, passfile


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--env", type=Path)
    parser.add_argument("--runtime-status", type=Path)
    parser.add_argument("--expected-pid", type=int)
    output_mode = parser.add_mutually_exclusive_group()
    output_mode.add_argument("--connection", action="store_true")
    output_mode.add_argument("--driver-only", action="store_true")
    args = parser.parse_args()
    if args.driver_only:
        if args.env is not None:
            parser.error("--env cannot be used with --driver-only")
        if args.runtime_status is None:
            parser.error("--runtime-status is required with --driver-only")
        if args.expected_pid is None:
            parser.error("--expected-pid is required with --driver-only")
        if args.expected_pid < 1:
            parser.error("--expected-pid must be positive")
        try:
            driver = read_driver_only(args.config)
            runtime_driver = read_runtime_driver(args.runtime_status, args.expected_pid)
            if runtime_driver != driver:
                fail("configured and active database drivers do not match")
        except ValueError as exc:
            print(f"BridgeOps database driver validation failed: {exc}", file=sys.stderr)
            return 1
        print(driver)
        return 0
    if args.env is None:
        parser.error("--env is required unless --driver-only is used")
    if args.runtime_status is not None or args.expected_pid is not None:
        parser.error("runtime status options require --driver-only")
    try:
        fields, password, passfile = inspect(args.config, args.env)
    except ValueError as exc:
        print(f"PostgreSQL config validation failed: {exc}", file=sys.stderr)
        return 1
    if args.connection:
        fields.extend((password, passfile))
    for field in fields:
        sys.stdout.buffer.write(field.encode("utf-8") + b"\0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
