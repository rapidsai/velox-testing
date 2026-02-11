# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import prestodb


def test_simple_query():
    conn = prestodb.dbapi.connect(host="localhost", port=8080, user="test_user", catalog="tpch", schema="sf1")
    cursor = conn.cursor()

    cursor.execute("select count(*) from customer")
    rows = cursor.fetchall()

    assert len(rows) == 1
    assert len(rows[0]) == 1
    assert rows[0][0] == 150000
