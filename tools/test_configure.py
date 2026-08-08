from pathlib import Path
import os
import subprocess
import tempfile
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CONFIGURE = REPOSITORY_ROOT / "configure"


class ConfigureTests(unittest.TestCase):
    def run_configure(self, rustc_version):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            binary = home / ".cargo" / "bin"
            binary.mkdir(parents=True)
            tools = {
                "cargo": "#!/bin/sh\necho 'cargo 1.88.0 (test)'\n",
                "rustc": f"#!/bin/sh\necho 'rustc {rustc_version} (test)'\n",
            }
            for name, contents in tools.items():
                path = binary / name
                path.write_text(contents)
                path.chmod(0o755)

            environment = os.environ.copy()
            environment.update({"HOME": str(home), "PATH": "/usr/bin:/bin"})
            return subprocess.run(
                ["/bin/sh", str(CONFIGURE)],
                env=environment,
                text=True,
                capture_output=True,
                check=False,
            )

    def test_accepts_minimum_rustc_from_cargo_home(self):
        result = self.run_configure("1.88.0")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("rustc 1.88.0", result.stdout)

    def test_rejects_older_rustc(self):
        result = self.run_configure("1.87.9")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("rustc 1.88 or newer is required", result.stderr)

    def test_windows_configure_stays_in_sync(self):
        windows = REPOSITORY_ROOT / "configure.win"
        self.assertEqual(CONFIGURE.read_bytes(), windows.read_bytes())

    def test_makefiles_keep_cache_override_portable(self):
        for name in ("Makevars", "Makevars.win"):
            contents = (REPOSITORY_ROOT / "src" / name).read_text()
            self.assertIn("CARGO_TARGET_DIR:-$(LOCAL_TARGET_DIR)", contents)
            for extension in (":=", "?=", "+=", "$(shell", "$(wildcard"):
                self.assertNotIn(extension, contents)


if __name__ == "__main__":
    unittest.main()
