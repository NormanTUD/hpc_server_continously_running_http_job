#!/bin/bash

#SBATCH --time=00:10:00
#SBATCH --mem=1GB
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --job-name=slurm_runner

slurmlogpath () {
	if command -v scontrol 2>/dev/null >/dev/null; then
		scontrol show job "$1" | grep StdOut | sed -e 's/^[[:space:]]*StdOut=//'
	fi
}

if [ -n "$SLURM_JOB_ID" ]; then
	SCRIPT="$(slurmlogpath $SLURM_JOB_ID)/slurm.sbatch"
	sbatch --dependency=afterany:$SLURM_JOB_ID "$SCRIPT" "$@"
	bash $(slurmlogpath $SLURM_JOB_ID)/run $*
else
	bash run $*
fi

