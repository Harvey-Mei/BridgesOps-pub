from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BUILD_SCRIPT = ROOT / "scripts" / "build-deployment-artifact.sh"


class DeploymentArtifactBuildTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="bridgeops-artifact-build-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.repository = self.base / "repository"
        self.fake_bin = self.base / "fake-bin"
        self.output_dir = self.base / "release"
        self.output = self.output_dir / "bridgeops-linux-amd64"
        self.marker = self.base / "pnpm-called"
        self.fake_bin.mkdir()
        self.output_dir.mkdir()
        self.create_repository()
        self.create_fake_toolchain()

    def test_dirty_worktree_is_rejected_before_frontend_build(self) -> None:
        (self.repository / "tracked.txt").write_text("dirty\n", encoding="utf-8")

        result = self.run_build()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Git worktree must be clean and committed", result.stderr)
        self.assertFalse(self.marker.exists())
        self.assertFalse(self.output.exists())

    def test_artifact_with_missing_or_dirty_vcs_metadata_is_rejected(self) -> None:
        for mode in ("missing", "dirty"):
            with self.subTest(mode=mode):
                result = self.run_build(FAKE_GO_METADATA=mode)

                self.assertNotEqual(result.returncode, 0)
                self.assertIn("artifact ELF or Go metadata validation failed", result.stderr)
                self.assertFalse(self.output.exists())

    def test_missing_frontend_index_is_rejected_before_synchronization(self) -> None:
        stale = self.repository / "web" / "dist" / "stale.js"
        stale.write_text("keep until synchronization\n", encoding="utf-8")

        result = self.run_build(FAKE_FRONTEND_MISSING_INDEX="1")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("frontend build did not create frontend/dist/index.html", result.stderr)
        self.assertTrue(self.marker.is_file())
        self.assertTrue(stale.is_file())
        self.assertFalse((self.repository / "web" / "dist" / "index.html").exists())
        self.assertFalse(self.output.exists())

    def test_successful_build_synchronizes_frontend_dist(self) -> None:
        stale = self.repository / "web" / "dist" / "stale.js"
        stale.write_text("remove during synchronization\n", encoding="utf-8")

        result = self.run_build()

        self.assertEqual(result.returncode, 0, result.stderr)
        frontend_index = self.repository / "frontend" / "dist" / "index.html"
        embedded_index = self.repository / "web" / "dist" / "index.html"
        self.assertTrue(embedded_index.is_file())
        self.assertEqual(embedded_index.read_bytes(), frontend_index.read_bytes())
        self.assertTrue((self.repository / "web" / "dist" / "assets" / "app-contract.js").is_file())
        self.assertFalse(stale.exists())
        self.assertTrue(self.output.is_file())

    def test_artifact_without_embedded_index_or_asset_is_rejected(self) -> None:
        cases = {
            "missing-index": "artifact does not contain embedded frontend index",
            "missing-asset": "artifact does not contain referenced frontend asset",
        }
        for mode, expected_error in cases.items():
            with self.subTest(mode=mode):
                if self.output.exists():
                    self.output.unlink()
                result = self.run_build(FAKE_STRINGS_MODE=mode)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected_error, result.stderr)
                self.assertFalse(self.output.exists())

    def run_build(self, **overrides: str) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment.update(
            {
                "PATH": f"{self.fake_bin}:/usr/bin:/bin",
                "GO_BIN": "go",
                "FAKE_PNPM_MARKER": str(self.marker),
                "GIT_CONFIG_GLOBAL": "/dev/null",
                "GIT_CONFIG_NOSYSTEM": "1",
            }
        )
        environment.update(overrides)
        return subprocess.run(
            [str(self.repository / "scripts" / BUILD_SCRIPT.name), str(self.output)],
            cwd=self.repository,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )

    def create_repository(self) -> None:
        for relative in (
            "scripts",
            "backend/global",
            "frontend",
            "web/dist",
        ):
            (self.repository / relative).mkdir(parents=True, exist_ok=True)
        shutil.copy2(BUILD_SCRIPT, self.repository / "scripts" / BUILD_SCRIPT.name)
        (self.repository / "scripts" / BUILD_SCRIPT.name).chmod(0o755)
        (self.repository / "backend" / "global" / "version.go").write_text(
            'package global\n\nconst VERSION = "0.0.35"\n', encoding="utf-8"
        )
        (self.repository / "frontend" / "package.json").write_text(
            '{"name":"bridgeops-contract","version":"0.0.35"}\n', encoding="utf-8"
        )
        (self.repository / "web" / "dist" / ".gitkeep").touch()
        (self.repository / "tracked.txt").write_text("clean\n", encoding="utf-8")
        (self.repository / ".gitignore").write_text(
            "frontend/dist/\nweb/dist/*\n!web/dist/.gitkeep\n", encoding="utf-8"
        )
        self.git("init", "-q")
        self.git("config", "user.email", "contract@example.invalid")
        self.git("config", "user.name", "Contract Test")
        self.git("add", ".")
        self.git("commit", "-qm", "test fixture")

    def git(self, *arguments: str) -> None:
        subprocess.run(
            ["git", *arguments],
            cwd=self.repository,
            check=True,
            capture_output=True,
            text=True,
        )

    def create_fake_toolchain(self) -> None:
        self.write_executable(
            "pnpm",
            r"""
                #!/bin/sh
                set -eu
                printf 'called\n' > "$FAKE_PNPM_MARKER"
                mkdir -p frontend/dist/assets
                printf 'contract bundle\n' > frontend/dist/assets/app-contract.js
                if [ "${FAKE_FRONTEND_MISSING_INDEX:-0}" != 1 ]; then
                    printf '%s\n' '<script type="module" src="/assets/app-contract.js"></script>' > frontend/dist/index.html
                fi
            """,
        )
        self.write_executable(
            "node",
            r"""
                #!/bin/sh
                case "${1:-}" in
                    -p) printf '0.0.35\n' ;;
                    -e) printf 'assets/app-contract.js' ;;
                    *) exit 2 ;;
                esac
            """,
        )
        self.write_executable(
            "go",
            r"""
                #!/bin/sh
                set -eu
                case "${1:-}" in
                    test)
                        exit 0
                        ;;
                    build)
                        output=
                        while [ "$#" -gt 0 ]; do
                            if [ "$1" = -o ]; then
                                shift
                                output=$1
                                break
                            fi
                            shift
                        done
                        [ -n "$output" ]
                        printf 'fake static ELF\n' > "$output"
                        exit 0
                        ;;
                    run)
                        [ "${FAKE_GO_METADATA:-complete}" = complete ]
                        exit 0
                        ;;
                    version)
                        [ "${2:-}" = -m ]
                        printf '%s: go1.24.0\n' "$3"
                        printf 'path\tgithub.com/Harvey-Mei/BridgeOps/cmd/server\n'
                        revision=$(/usr/bin/git rev-parse --verify HEAD)
                        printf 'build\tvcs.revision=%s\n' "$revision"
                        case "${FAKE_GO_METADATA:-complete}" in
                            complete) printf 'build\tvcs.modified=false\n' ;;
                            dirty) printf 'build\tvcs.modified=true\n' ;;
                        esac
                        exit 0
                        ;;
                esac
                exit 2
            """,
        )
        self.write_executable(
            "strings",
            r"""
                #!/bin/sh
                case "${FAKE_STRINGS_MODE:-complete}" in
                    complete)
                        printf 'dist/index.html\ndist/assets/app-contract.js\n'
                        ;;
                    missing-index)
                        printf 'dist/assets/app-contract.js\n'
                        ;;
                    missing-asset)
                        printf 'dist/index.html\n'
                        ;;
                    *) exit 2 ;;
                esac
            """,
        )

    def write_executable(self, name: str, contents: str) -> None:
        path = self.fake_bin / name
        path.write_text(textwrap.dedent(contents).lstrip(), encoding="utf-8")
        path.chmod(0o755)


if __name__ == "__main__":
    unittest.main()
