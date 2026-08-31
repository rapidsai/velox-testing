# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import sys
import types
import unittest

sys.modules.setdefault("requests", types.SimpleNamespace())

import presto_api  # noqa: E402


class CountUniqueNodeUrisTest(unittest.TestCase):
    def test_ignores_stale_duplicate_records(self) -> None:
        nodes = [
            {"uri": "http://10.0.0.1:8080/v1/status"},
            {
                "uri": "http://10.0.0.1:8080/v1/status",
                "lastFailureInfo": {"message": "connection refused"},
            },
            {"uri": "http://10.0.0.2:8080/v1/status"},
        ]

        self.assertEqual(presto_api.count_unique_node_uris(nodes), 2)

    def test_distinguishes_workers_on_different_ports(self) -> None:
        nodes = [
            {"uri": "http://10.0.0.1:8080/v1/status"},
            {"uri": "http://10.0.0.1:8090/v1/status"},
        ]

        self.assertEqual(presto_api.count_unique_node_uris(nodes), 2)

    def test_ignores_malformed_records(self) -> None:
        nodes = [{}, {"uri": None}, {"uri": ""}, "not-a-node"]

        self.assertEqual(presto_api.count_unique_node_uris(nodes), 0)


if __name__ == "__main__":
    unittest.main()
