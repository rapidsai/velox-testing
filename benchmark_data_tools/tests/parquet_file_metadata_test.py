# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import pyarrow.parquet as pq
from generate_data_files import generate_data_files

from .common_fixtures import get_all_parquet_relative_file_paths


def test_generated_files_use_v2_page_format(setup_and_teardown):
    """Verify every generated Parquet file is written with the v2 format."""
    data_dir_path, args = setup_and_teardown
    generate_data_files(args)

    for file_path in get_all_parquet_relative_file_paths(data_dir_path):
        metadata = pq.ParquetFile(f"{data_dir_path}/{file_path}").metadata
        assert int(float(metadata.format_version)) == 2, (
            f"Expected Parquet v2 format for '{file_path}', got '{metadata.format_version}'"
        )
