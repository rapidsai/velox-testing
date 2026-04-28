# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import os
import prestodb


def _default_port():
    env_port = os.getenv("PRESTO_COORDINATOR_PORT")
    if env_port:
        try:
            return int(env_port)
        except ValueError:
            pass
    return 8080


def test_simple_query():
    host = os.getenv("PRESTO_COORDINATOR_HOST", "localhost")
    port = _default_port()
    conn = prestodb.dbapi.connect(host=host, port=port, user="test_user", catalog="tpch", schema="sf1")
    cursor = conn.cursor()

    cursor.execute("select count(*) from customer")
    rows = cursor.fetchall()

    assert len(rows) == 1
    assert len(rows[0]) == 1
    assert rows[0][0] == 150000
