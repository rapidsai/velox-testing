import os
import json


def get_queries(benchmark_type):
    with open(get_abs_file_path(f"./queries/{benchmark_type}/queries.json"), "r") as file:
        return json.load(file)


def get_abs_file_path(relative_path):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), relative_path))
