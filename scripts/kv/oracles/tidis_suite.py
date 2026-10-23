"""Runs Tidis's Python suite (test/, unmodified) against a server, without
its Lua cases (KV1 plan KV0 Task 10, step 2; Q1223 leaves Lua out of v1).

Usage: python tidis_suite.py <tidis checkout> <host> <port>

This driver is ours: it imports Tidis's test cases and builds the suite
test_helper.py builds, minus LuaTest, with the server set directly instead
of through test_helper.py's arguments.
"""

import sys
import unittest


def main() -> int:
    checkout, host, port = sys.argv[1], sys.argv[2], int(sys.argv[3])
    sys.path.insert(0, checkout + "/test")
    sys.argv = sys.argv[:1]
    from rediswrap import RedisWrapper

    RedisWrapper.set_instance_manually(host, port)
    import test_generic, test_hash, test_invalid, test_list, test_set, test_string, test_zset

    suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    for case in (
        test_generic.GenericTest,
        test_invalid.InvalidTest,
        test_string.StringTest,
        test_hash.HashTest,
        test_list.ListTest,
        test_set.SetTest,
        test_zset.ZsetTest,
    ):
        suite.addTest(loader.loadTestsFromTestCase(case))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    print(
        f"tidis suite: run {result.testsRun}, failures {len(result.failures)}, "
        f"errors {len(result.errors)}, skipped {len(result.skipped)}"
    )
    for test, _ in result.failures + result.errors:
        print(f"deviation: {test.id()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
