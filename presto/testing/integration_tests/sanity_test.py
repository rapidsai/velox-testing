# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
