# Allow a service to be deployed on SLURM-Systems

This contains 2 scripts. One starts a very simple webserver on an HPC-system over SSH inside a sbatch job.

The other one allows to keep that server in Slurm running by making sure a job is always there, and it's always responding, and restarts it when it's not, and forwards everything to localhost, so it can be used for an API.

This allows you to run a service on the enterprise, or research cloud or any computer you wish, and redirect the HPC system's outputs to your localhost.

## How to run

For example, with a jumphost:

```bash
bash run --hpc-system-url login1.partition.cluster-name.com --jumphost-url jumphost.com --local-hpc-script-dir ../hpc --username your_username --jumphost-username service --hpc-script-dir "/home/your_username/hpc_scripts" --copy
```

## `--help`

```
  --hpc-system-url HPC_SYSTEM_URL
                        SSH target for primary HPC head-node (user@host)
  --fallback-system-url FALLBACK_SYSTEM_URL
                        SSH target for fallback HPC head-node
  --jumphost-url JUMPHOST_URL
                        Optional SSH jumphost in user@host form
  --local-hpc-script-dir LOCAL_HPC_SCRIPT_DIR
                        Local directory containing Slurm scripts
  --hpc-script-dir HPC_SCRIPT_DIR
                        Directory on the HPC System where the files should be copied to
  --copy                If set, rsync the script directory before anything else
  --debug               Verbose local shell output
  --retries RETRIES     SSH retry attempts before using fallback
  --local-port LOCAL_PORT
                        Local port to expose the remote service
  --heartbeat-time HEARTBEAT_TIME
                        Time to re-check if the server is still running properly
  --username USERNAME   SSH username for HPC and (by default) also for jumphost
  --jumphost-username JUMPHOST_USERNAME
                        SSH username for jumphost (defaults to --username)
  --hpc-job-name HPC_JOB_NAME
                        Name of the HPC job (defaults to slurm_runner)
  --server-and-port-file SERVER_AND_PORT_FILE
                        Globally available path to a file where the hostname and port for the host should be put on HPC (defaults to ~/hpc_server_host_and_file)
  --max-attempts-get-server-and-port MAX_ATTEMPTS_GET_SERVER_AND_PORT
                        Number of attempts to get the --server-and-port-file from the HPC (defaults to 60)
  --delay_between_server_and_port DELAY_BETWEEN_SERVER_AND_PORT
                        Delay between calls to the --server-and-port-file check on HPC (defaults to 5)
  --daemonize
```
