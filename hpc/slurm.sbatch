#!/bin/bash

#SBATCH --time=00:10:00
#SBATCH --mem=1GB
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --job-name=slurm_runner

slurmlogdir () {
	if command -v scontrol >/dev/null 2>&1; then
		local logfile
		logfile=$(scontrol show job "$1" | grep -oP 'StdOut=\K\S+')
		if [ -n "$logfile" ]; then
			dirname "$logfile"
		else
			echo "Kein StdOut-Pfad gefunden." >&2
			return 1
		fi
	else
		echo "scontrol nicht verfügbar." >&2
		return 1
	fi
}

if [ -n "$SLURM_JOB_ID" ]; then
	SCRIPT="$(slurmlogdir $SLURM_JOB_ID)/slurm.sbatch"
	sbatch --dependency=afterany:$SLURM_JOB_ID "$SCRIPT" "$@"
	bash $(slurmlogdir $SLURM_JOB_ID)/run $*
else
	bash run $*
fi

