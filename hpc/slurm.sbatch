#!/bin/bash

#SBATCH --time=00:10:00
#SBATCH --mem=1GB
#SBATCH --nodes=1
#SBATCH --gres=gpu:1

bash /home/s3811141/hpc_scripts/run $*
