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

from ..common.conftest import *


def _default_port():
    env_port = os.getenv("PRESTO_COORDINATOR_PORT")
    if env_port:
        try:
            return int(env_port)
        except ValueError:
            pass
    return 8080


DEFAULT_HOST = os.getenv("PRESTO_COORDINATOR_HOST", "localhost")
DEFAULT_PORT = _default_port()

def pytest_addoption(parser):
    parser.addoption("--queries") # default is all queries for the benchmark type
    parser.addoption("--keep-tables", action="store_true", default=False)
    parser.addoption("--hostname", default=DEFAULT_HOST)
    parser.addoption("--port", default=DEFAULT_PORT, type=int)
    parser.addoption("--user", default="test_user")
    parser.addoption("--schema-name")
    parser.addoption("--scale-factor")
