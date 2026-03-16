#!/bin/bash
#SBATCH --job-name=copy-images-to-scratch
#SBATCH --output=/home/prestouser/Misiu/Build/velox-testing/presto/slurm/build/%x_%j.out
#SBATCH --error=/home/prestouser/Misiu/Build/velox-testing/presto/slurm/build/%x_%j.err
#SBATCH --time=00:30:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1

set -e

SRC_DIR=/home/prestouser/Misiu/Images
DST_DIR=/scratch/prestouser/images/presto

mkdir -p "${DST_DIR}"

echo "Copying images to ${DST_DIR}..."

cp -v "${SRC_DIR}/presto-coordinator-karth-Mar11.sqsh" "${DST_DIR}/"
cp -v "${SRC_DIR}/presto-native-worker-gpu-karth-Mar11.sqsh" "${DST_DIR}/"

echo "Done. Contents of ${DST_DIR}:"
ls -lh "${DST_DIR}/"
