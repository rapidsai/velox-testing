ARG TPCHGEN_IMAGE
FROM ${TPCHGEN_IMAGE}

RUN uv pip install --python /opt/venv/bin/python \
      duckdb==1.3.2 pyarrow==21.0.0 && \
    python -c "import duckdb; duckdb.sql('INSTALL tpch')"

COPY benchmark_data_tools/generate_data_files.py \
     benchmark_data_tools/duckdb_utils.py \
     /opt/benchmark_data_tools/

ENTRYPOINT ["python", "/opt/benchmark_data_tools/generate_data_files.py"]
