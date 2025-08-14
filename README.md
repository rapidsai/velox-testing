# velox-testing
This repository contains infrastructure for Velox and Presto functional and benchmark testing. The scripts in this repository are intended to be usable by CI/CD systems, such as GitHub Actions, as well as usable for local development and testing.

The provided infrastructure is broken down into four categories:
- Velox Testing
- Velox Benchmarking
- Presto Testing
- Presto Benchmarking

Important details about each category is provided below.

## Velox Testing
A Docker-based build infrastructure has been added to facilitate building Velox with comprehensive configuration options including GPU support, various storage adapters, and CI-mirrored settings. This infrastructure builds Velox libraries and executables only. In order to build Velox using this infrastructure, the following directory structure is expected:

```
├─ base_directory/
  ├─ velox-testing
  ├─ velox
  ├─ presto (optional, for presto-native builds)
```

Specifically, the `velox-testing` and `velox` repositories must be checked out as sibling directories under the same parent directory. Once that is done, navigate (`cd`) into the `velox-testing/velox/scripts` directory and execute the build script `build_velox.sh`. After a successful build, the Velox libraries and executables are available in the container at `/opt/velox-build/release`. Build logs can be accessed with `docker exec -it velox-adapters-build cat /workspace/adapters_build.log`.

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
