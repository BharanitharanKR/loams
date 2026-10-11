#!/usr/bin/env python3
"""NF1 Task 3: tests for scripts/nf1/check-pinned-revs.sh.

Most tests run the script on a scratch tree of Cargo.toml files with a fake
`gh` (GH=<fake>) that answers from canned responses, so they need no network.

`pinned_revs_reachable` and the live half of `unreachable_rev_fails` ask
GitHub for real; they run when `gh api user` works (GH_TOKEN, or a gh login)
and are skipped otherwise.

`force_push_rejected` pushes to ostrium-labs/loams-postgres and runs only with
NF1_PROBE=1: it creates loams/probe, force-pushes it (which the ruleset
"loams: no force-push (main, loams/*)" must refuse) and deletes it again
(loams/probe is the one branch the no-deletion ruleset leaves out).

Run: python3 -I scripts/nf1/tests/test_pinned_revs.py
"""

import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
NF1 = HERE.parent
REPO = NF1.parent.parent
CHECK = NF1 / "check-pinned-revs.sh"

FORK_POINT = "fa504217c61bbcaf5c512d75830564541f917f8f"
TRIM_1 = "1218fb7a6a37e1b4c268bad5c2952c238d81d368"  # on main since Task 3
TRIM_2 = "80356b2998710722020134d81102d810f921fe6b"  # tag loams-decoder-trim-2
DIVERGED = "de27c7dc667650aa1a89553d574d5358e72bd0a9"  # a Neon branch head, not on main
RUST_PG = "f3cf448febde5fd298071d54d568a9c875a7a62b"

FAKE_GH = """#!/usr/bin/env bash
# Answers `gh api <path> [flags]` from $FAKE_GH_DIR/<path with / as __>.
[ "$1" = api ] || exit 2
f="$FAKE_GH_DIR/$(printf '%s' "$2" | sed 's|/|__|g')"
if [ -f "$f" ]; then cat "$f"; else echo "gh: Not Found (HTTP 404)" >&2; exit 1; fi
"""


def cargo(repo, rev, crate="x", url=None):
    url = url or f"https://github.com/ostrium-labs/{repo}"
    return f'{crate} = {{ git = "{url}", rev = "{rev}" }}\n'


def gh_works():
    if not shutil.which("gh"):
        return False
    return subprocess.run(["gh", "api", "user"], capture_output=True).returncode == 0


class Fake(unittest.TestCase):
    """A scratch tree and a fake gh."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="nf1-revs-")
        tmp = Path(self._tmp.name)
        self.root = tmp / "tree"
        self.api = tmp / "api"
        self.root.mkdir()
        self.api.mkdir()
        gh = tmp / "gh"
        gh.write_text(FAKE_GH)
        gh.chmod(0o755)
        self.env = dict(os.environ, GH=str(gh), FAKE_GH_DIR=str(self.api))

    def tearDown(self):
        self._tmp.cleanup()

    def toml(self, rel, text):
        p = self.root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)

    def answer(self, path, body):
        (self.api / path.replace("/", "__")).write_text(body)

    def compare(self, repo, branch, rev, status):
        self.answer(f"repos/ostrium-labs/{repo}/compare/{branch}...{rev}", status + "\n")

    def tags(self, repo, lines=""):
        self.answer(f"repos/ostrium-labs/{repo}/git/matching-refs/tags/loams-", lines)

    def run_check(self):
        return subprocess.run(
            [str(CHECK), str(self.root)], env=self.env, capture_output=True, text=True
        )


class ReachableByBranch(Fake):
    def test_behind_main_passes(self):
        self.toml("Cargo.toml", cargo("loams-postgres", TRIM_1))
        self.compare("loams-postgres", "main", TRIM_1, "behind")
        r = self.run_check()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn(f"ok   loams-postgres {TRIM_1}", r.stdout)

    def test_identical_passes(self):
        self.toml("Cargo.toml", cargo("loams-postgres", TRIM_1))
        self.compare("loams-postgres", "main", TRIM_1, "identical")
        self.assertEqual(self.run_check().returncode, 0)

    def test_old_fork_url_is_loams_postgres(self):
        self.toml("a/Cargo.toml", cargo("neon", TRIM_1))
        self.compare("loams-postgres", "main", TRIM_1, "behind")
        r = self.run_check()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("loams-postgres", r.stdout)

    def test_small_fork_uses_its_loams_branch(self):
        self.toml("Cargo.toml", cargo("rust-postgres", RUST_PG, url="https://github.com/ostrium-labs/rust-postgres.git"))
        self.compare("rust-postgres", "loams/neon", RUST_PG, "identical")
        r = self.run_check()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("loams/neon", r.stdout)

    def test_rev_before_git_and_multiline_tables(self):
        self.toml(
            "Cargo.toml",
            textwrap.dedent(
                f"""\
                [dependencies]
                a = {{ rev = "{TRIM_1}", git = "https://github.com/ostrium-labs/loams-postgres", default-features = false }}

                [dependencies.b]
                git = "https://github.com/ostrium-labs/loams-postgres"
                rev = "{TRIM_2}"
                """
            ),
        )
        self.compare("loams-postgres", "main", TRIM_1, "behind")
        self.compare("loams-postgres", "main", TRIM_2, "behind")
        r = self.run_check()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn(TRIM_1, r.stdout)
        self.assertIn(TRIM_2, r.stdout)


class ReachableByTag(Fake):
    def test_annotated_loams_tag_passes(self):
        self.toml("Cargo.toml", cargo("loams-postgres", TRIM_2))
        self.compare("loams-postgres", "main", TRIM_2, "ahead")
        self.tags("loams-postgres", f"tag 0e771a02f7f38c1955fbfff93d6e5b66ec71a486 refs/tags/loams-decoder-trim-2\n")
        self.answer(
            "repos/ostrium-labs/loams-postgres/git/tags/0e771a02f7f38c1955fbfff93d6e5b66ec71a486",
            f"commit {TRIM_2}\n",
        )
        r = self.run_check()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("tag loams-decoder-trim-2", r.stdout)

    def test_lightweight_loams_tag_passes(self):
        self.toml("Cargo.toml", cargo("loams-postgres", TRIM_2))
        self.compare("loams-postgres", "main", TRIM_2, "diverged")
        self.tags("loams-postgres", f"commit {TRIM_2} refs/tags/loams-20261012-9\n")
        self.assertEqual(self.run_check().returncode, 0)


class UnreachableRevFails(Fake):
    """unreachable_rev_fails"""

    def test_diverged_rev_without_tag_fails(self):
        self.toml("Cargo.toml", cargo("loams-postgres", DIVERGED))
        self.compare("loams-postgres", "main", DIVERGED, "diverged")
        self.tags("loams-postgres", f"commit {TRIM_1} refs/tags/loams-20261012-1\n")
        r = self.run_check()
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn(f"FAIL loams-postgres {DIVERGED}", r.stdout)

    def test_ahead_of_main_fails(self):
        self.toml("Cargo.toml", cargo("loams-postgres", TRIM_2))
        self.compare("loams-postgres", "main", TRIM_2, "ahead")
        self.tags("loams-postgres")
        self.assertEqual(self.run_check().returncode, 1)

    def test_unknown_rev_fails(self):
        self.toml("Cargo.toml", cargo("loams-postgres", "0" * 40))
        self.tags("loams-postgres")
        r = self.run_check()
        self.assertEqual(r.returncode, 1)
        self.assertIn("compare failed", r.stdout)

    def test_missing_loams_branch_fails(self):
        self.toml("Cargo.toml", cargo("tokio-epoll-uring", TRIM_1))
        self.tags("tokio-epoll-uring")
        r = self.run_check()
        self.assertEqual(r.returncode, 1)
        self.assertIn("loams/main", r.stdout)

    def test_one_bad_rev_fails_the_run(self):
        self.toml("a/Cargo.toml", cargo("loams-postgres", TRIM_1))
        self.toml("b/Cargo.toml", cargo("loams-postgres", DIVERGED))
        self.compare("loams-postgres", "main", TRIM_1, "behind")
        self.compare("loams-postgres", "main", DIVERGED, "diverged")
        self.tags("loams-postgres")
        r = self.run_check()
        self.assertEqual(r.returncode, 1)
        self.assertIn(f"ok   loams-postgres {TRIM_1}", r.stdout)


class Scope(Fake):
    def test_other_ostrium_repos_are_listed_not_checked(self):
        self.toml("Cargo.toml", cargo("resonate", "e3606698e6e3f2502bb018bba1e618deb63f907a"))
        r = self.run_check()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("skip resonate", r.stdout)

    def test_other_hosts_and_branch_deps_ignored(self):
        self.toml(
            "Cargo.toml",
            'a = { git = "https://github.com/neondatabase/neon", rev = "%s" }\n'
            'b = { git = "https://github.com/ostrium-labs/loams-postgres", branch = "main" }\n' % TRIM_1,
        )
        r = self.run_check()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertNotIn("FAIL", r.stdout)

    def test_target_dirs_skipped(self):
        self.toml("target/debug/x/Cargo.toml", cargo("loams-postgres", DIVERGED))
        self.assertEqual(self.run_check().returncode, 0)


@unittest.skipUnless(gh_works(), "gh is not authenticated")
class Live(unittest.TestCase):
    def run_check(self, root):
        return subprocess.run([str(CHECK), str(root)], capture_output=True, text=True)

    def test_pinned_revs_reachable(self):
        """pinned_revs_reachable: the repository's pins, today."""
        r = self.run_check(REPO)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn(f"ok   loams-postgres {TRIM_2}", r.stdout)
        self.assertIn(f"ok   rust-postgres {RUST_PG}", r.stdout)

    def test_decoder_trim_1_on_main(self):
        """The fast-forward: 1218fb7a is reachable from main, by the old URL too."""
        with tempfile.TemporaryDirectory(prefix="nf1-revs-") as d:
            Path(d, "Cargo.toml").write_text(cargo("neon", TRIM_1))
            r = self.run_check(d)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("main", r.stdout)

    def test_unreachable_rev_fails_live(self):
        with tempfile.TemporaryDirectory(prefix="nf1-revs-") as d:
            Path(d, "Cargo.toml").write_text(cargo("loams-postgres", DIVERGED))
            r = self.run_check(d)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("diverged", r.stdout)


@unittest.skipUnless(os.environ.get("NF1_PROBE") == "1", "NF1_PROBE=1 not set (pushes to the fork)")
class ForcePushRejected(unittest.TestCase):
    """force_push_rejected: a non-fast-forward push to loams/probe is refused."""

    REMOTE = "git@github.com:ostrium-labs/loams-postgres.git"
    BRANCH = "refs/heads/loams/probe"

    def git(self, *args, check=True):
        r = subprocess.run(["git", "-C", self.dir, *args], capture_output=True, text=True)
        if check and r.returncode:
            self.fail(f"git {' '.join(args)}: {r.stderr}")
        return r

    def remote_head(self):
        out = self.git("ls-remote", self.REMOTE, self.BRANCH).stdout.split()
        return out[0] if out else None

    def test_force_push_rejected(self):
        with tempfile.TemporaryDirectory(prefix="nf1-probe-") as self.dir:
            self.git("init", "-q")
            self.assertIsNone(self.remote_head(), "loams/probe already exists; not ours to touch")
            ident = ["-c", "user.name=nf1-probe", "-c", "user.email=nf1-probe@invalid"]
            self.git(*ident, "commit", "-q", "--allow-empty", "-m", "nf1 probe: first")
            first = self.git("rev-parse", "HEAD").stdout.strip()
            self.git(*ident, "commit", "-q", "--allow-empty", "-m", "nf1 probe: second")
            second = self.git("rev-parse", "HEAD").stdout.strip()
            self.git("push", "-q", self.REMOTE, f"HEAD:{self.BRANCH}")
            try:
                r = self.git("push", "--force", self.REMOTE, f"{first}:{self.BRANCH}", check=False)
                print("\nforce push:", r.returncode, r.stderr.strip().replace("\n", " | "))
                self.assertNotEqual(r.returncode, 0, "the force push was accepted")
                self.assertRegex(r.stderr, r"GH013|Cannot force-push")
                self.assertEqual(self.remote_head(), second, "loams/probe moved")
            finally:
                # Delete only the commit this test pushed.
                d = self.git("push", f"--force-with-lease={self.BRANCH}:{second}", self.REMOTE, f":{self.BRANCH}", check=False)
                print("delete:", d.returncode, d.stderr.strip().replace("\n", " | "))
            self.assertEqual(d.returncode, 0, d.stderr)
            self.assertIsNone(self.remote_head())

if __name__ == "__main__":
    unittest.main(verbosity=2)
