# velox-testing
This repository contains infrastructure for Velox and Presto functional and benchmark testing. The scripts in this repository are intended to be usable by CI/CD systems, such as GitHub Actions, as well as usable for local development and testing.

The provided infrastructure is broken down into four categories:
- Velox Testing
- Velox Benchmarking
- Presto Testing
- Presto Benchmarking

Important details about each category is provided below.

## Velox Testing
TODO: Add details when related infrastructure is added.

## Velox Benchmarking
TODO: Add details when related infrastructure is added.

## Presto Testing
A number of docker image build and container services infrastructure (using docker compose) have been added to facilitate and simplify the process of building and deploying presto native CPU and GPU workers for a given snapshot/branch of the [presto](https://github.com/prestodb/presto) and [velox](https://github.com/facebookincubator/velox) repositories. In order to build and deploy presto using this infrastructure, the following directory structure is expected for the involved repositories:
```
├─ base_directory/
  ├─ velox-testing
  ├─ presto
  ├─ velox
``` 
Specifically, the `velox-testing`, `presto`, and `velox` repositories have to be checked out as sibling directories under the same parent directory. Once that is done, navigate (`cd`) into the `velox-testing/presto/scripts` directory and execute the start up script for the needed presto deployment variant. The following scripts: `start_java_presto.sh`, `start_native_cpu_presto.sh`, and `start_native_gpu_presto.sh` can be used to build/deploy "Presto Java Coordinator + Presto Java Worker", "Presto Java Coordinator + Presto Native CPU Worker", and "Presto Java Coordinator + Presto Native GPU Worker" variants respectively. The presto server can then be accessed at http://localhost:8080.

## Presto Benchmarking
TODO: Add details when related infrastructure is added.
