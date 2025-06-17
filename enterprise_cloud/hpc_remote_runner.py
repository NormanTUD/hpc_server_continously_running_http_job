#!/usr/bin/env python3
"""
hpc_remote_runner.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Automated helper that

1.  Parses command‑line arguments (`argparse`).
2.  Executes a command on an HPC head‑node via SSH, with automatic fail‑over to
    a fallback node after a configurable number of failures.
3.  Verifies the presence of Slurm tools (`sbatch`, `srun`, `squeue` …) on the
    remote host and checks whether the current user can log in password‑less.
4.  Keeps a local HPC script directory in sync with the remote target via
    `rsync --update --archive --delete`.
5.  Ensures that a Slurm job named *hpc_system_server_runner* is running for
    the current user; if not, submits `{local_hpc_script_dir}/slurm.sbatch`.
6.  Once the job is running, reads `~/hpc_server_host_and_file` on the remote
    side, extracts *internal_host:port*, and sets up an SSH port‑forward so
    that a **local** HTTP endpoint transparently proxies to the remote server,
    optionally hopping through a jumphost.

The code attempts to stay robust and *loud* in its diagnostics, so you always
see what is happening.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import shlex
import shutil
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path, PosixPath
from typing import Final, Optional
import getpass
import sys
from pprint import pprint
import psutil

from beartype import beartype
from rich.console import Console
from rich.table import Table
from rich.text import Text
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_fixed

try:
    from subprocess import Popen, DEVNULL, PIPE
except ImportError as err:  # pragma: no cover
    sys.exit(
        "Missing runtime dependencies.  Run `pip install -r requirements.txt` "
        f"first!  ({err})"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Globals & helpers
# ──────────────────────────────────────────────────────────────────────────────

console: Final = Console(highlight=False)

def dier (msg):
    pprint(msg)
    sys.exit(10)

@dataclass(slots=True)
class SSHConfig:
    target: str
    jumphost_url: str | None = None
    retries: int = 3
    debug: bool = False
    username: str | None = None
    jumphost_username: str | None = None

@beartype
def run_local(cmd: str, debug: bool = False, timeout: Optional[int] = 60) -> subprocess.CompletedProcess:
    """Run a *local* shell command, streaming output if `debug`."""
    if debug:
        console.log(f"[yellow]$ {cmd}")
    return subprocess.run(
        shlex.split(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
        check=False,
    )


@beartype
def build_ssh_cmd(
    cfg: SSHConfig,
    remote_cmd: str,
    allocate_tty: bool = False,
) -> str:
    """
    Compose an `ssh` command string with optional jumphost and debug flags.
    Uses ControlMaster auto‑socket for multiplexed connections (faster).
    """
    options = [
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ConnectTimeout=10",
        "-o", "ControlMaster=auto",
        "-o", "ControlPersist=60",
        "-o", "ControlPath=~/.ssh/ctl-%r@%h:%p",
    ]
    if cfg.jumphost_url:
        options.extend(["-J", cfg.jumphost_url])

    if allocate_tty:
        options.append("-tt")  # force TTY allocation (Slurm sbatch often needs it)

    cmd = ["ssh", *options, cfg.target, remote_cmd]
    return " ".join(shlex.quote(a) for a in cmd)


@beartype
async def ssh_run(
    cfg: SSHConfig,
    remote_cmd: str,
    tty: bool = False,
) -> subprocess.CompletedProcess:
    """Async wrapper that retries a remote ssh command per cfg.retries."""
    async for attempt in AsyncRetrying(
        stop=stop_after_attempt(cfg.retries),
        retry=retry_if_exception_type(subprocess.CalledProcessError),
        wait=wait_fixed(3),
        reraise=True,
    ):
        with attempt:
            cp = run_local(build_ssh_cmd(cfg, remote_cmd, tty), debug=cfg.debug)
            if cp.returncode != 0:
                msg = f"SSH command failed (attempt {attempt.retry_state.attempt_number}): \n"
                msg += f"{remote_cmd}\n"
                msg += f"\n{cp.stderr.strip()}\n"
                console.print(f"[red]{msg}[/red]")
                raise subprocess.CalledProcessError(cp.returncode, cp.args, cp.stdout, cp.stderr)
            return cp
    raise RuntimeError("Unreachable")


# ──────────────────────────────────────────────────────────────────────────────
# core – step‑wise helper functions
# ──────────────────────────────────────────────────────────────────────────────

@beartype
async def verify_slurm_and_key(cfg: SSHConfig) -> None:
    """Check for Slurm commands & password‑less SSH."""
    console.rule("[bold]Verifying remote environment[/bold]")

    # test key auth
    cp = await ssh_run(cfg, "echo OK")
    if cp.stdout.strip() != "OK":
        console.print("[red]Password‑less SSH seems not configured.  Aborting.[/red]")
        sys.exit(1)
    console.print("[green]✓ Password‑less SSH works.[/green]")

    # check Slurm binaries
    slurm_tools = ["sbatch", "squeue", "srun"]
    missing: list[str] = []
    for tool in slurm_tools:
        cmd = f"command -v {shlex.quote(tool)} >/dev/null"
        cp = await ssh_run(cfg, cmd)
        if cp.returncode != 0:
            missing.append(tool)

    if missing:
        console.print(f"[red]Missing Slurm tools on {cfg.target}: {', '.join(missing)}[/red]")
        sys.exit(1)
    console.print("[green]✓ Slurm utilities present.[/green]")


@beartype
async def rsync_scripts(
    cfg: SSHConfig,
    local_dir: PosixPath,
    remote_dir: str | PosixPath,
) -> None:
    """Rsync local script directory to remote."""
    if not local_dir.is_dir():
        console.print(f"[red]{local_dir} is not a directory.[/red]")
        sys.exit(1)

    console.rule("[bold]Ensuring remote directory exists[/bold]")

    mkdir_cmd = f"mkdir -p {shlex.quote(str(remote_dir))}"
    try:
        await ssh_run(cfg, mkdir_cmd)
    except subprocess.CalledProcessError as e:
        console.print(f"[red]Failed to create remote directory:[/red] {e.stderr}")
        sys.exit(e.returncode)

    console.rule("[bold]Synchronising script directory[/bold]")
    rsync_cmd = (
        f"rsync -az --delete "
        f"{shlex.quote(str(local_dir))}/ "
        f"{cfg.target}:{shlex.quote(str(remote_dir))}/"
    )
    # (jumphost w/ rsync: use ProxyJump via SSH config file or rely on our ssh options)
    cp = run_local(rsync_cmd, debug=cfg.debug)
    if cp.returncode:
        console.print(f"[red]rsync failed:[/red] {cp.stderr}")
        sys.exit(cp.returncode)

    console.print(f"[green]✓ {local_dir} → {cfg.target}:{remote_dir} updated.[/green]")

@beartype
def to_absolute(path: str | PosixPath)  -> Path:
    """Convert a relative or absolute path to an absolute path."""
    return Path(path).expanduser().resolve()

@beartype
async def ensure_job_running(
    cfg: "SSHConfig",  # Forward ref if SSHConfig not yet defined
    remote_script_dir: PosixPath,
) -> None:
    """
    Ensure a Slurm job named 'hpc_system_server_runner' is running for $USER.
    Submit the sbatch script if necessary.
    """
    console.rule("[bold]Ensuring server job is active[/bold]")
    user_expr = "$(whoami)"
    job_name = "hpc_system_server_runner"

    list_cmd = f"squeue -u {user_expr} -h -o '%j' | grep -Fx {job_name} || true"
    cp = await ssh_run(cfg, list_cmd)
    if cp.stdout.strip() == job_name:
        console.print("[green]✓ Job already running.[/green]")
        return

    console.print("[yellow]Job not running – submitting…[/yellow]")
    sbatch_path = f"{remote_script_dir}/slurm.sbatch"
    submit_cmd = f"sbatch {shlex.quote(sbatch_path)}"
    cp = await ssh_run(cfg, submit_cmd, tty=True)

    # Parse job ID from output like: Submitted batch job 878778
    job_id_line = cp.stdout.strip()
    console.print(job_id_line)
    try:
        job_id = int(job_id_line.strip().split()[-1])
    except Exception as e:
        console.print(f"[red]❌ Failed to extract job ID: {e}[/red]")
        return

    console.print("[cyan]Waiting for job to appear in queue or start…[/cyan]")

    timeout_seconds = 300  # 5 minutes max wait
    poll_interval = 10     # check every 10 seconds
    elapsed = 0

    while elapsed < timeout_seconds:
        # Check squeue by job ID
        check_cmd = f"squeue -j {job_id} -h -o '%i'"
        cp = await ssh_run(cfg, check_cmd)
        if str(job_id) in cp.stdout.strip():
            console.print("[green]✓ Job now listed in squeue.[/green]")
            return

        # If not in squeue, maybe already failed? Check sacct
        check_sacct = (
            f"sacct -j {job_id} --format=JobID,State --parsable2 --noheader || true"
        )
        cp = await ssh_run(cfg, check_sacct)
        for line in cp.stdout.strip().splitlines():
            parts = line.strip().split("|")
            if len(parts) >= 2 and parts[0].startswith(str(job_id)):
                state = parts[1]
                if state in ("FAILED", "CANCELLED", "TIMEOUT", "COMPLETED"):
                    console.print(f"[red]✗ Job terminated early: {state}[/red]")
                    return

        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

    console.print("[red]✗ Timed out waiting for job to start.[/red]")


@beartype
async def read_remote_host_port(cfg: SSHConfig) -> tuple[str, int]:
    """
    Poll remote ~/hpc_server_host_and_file until it exists and contains "host:port",
    then parse and return it.
    """
    remote_path = "~/hpc_server_host_and_file"
    max_attempts = 60
    delay_seconds = 5

    last_error: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        try:
            cp = await ssh_run(cfg, f"cat {remote_path}")
            host_port = cp.stdout.strip()

            if not host_port:
                raise RuntimeError("Empty response from remote file")

            host, port_s = host_port.split(":", 1)
            port = int(port_s)

            console.print(f"[green]Remote server: {host}:{port}[/green]")
            return host, port

        except Exception as exc:
            last_error = exc
            console.print(
                f"[yellow]Waiting for remote host file ({attempt}/{max_attempts})…[/yellow]"
            )
            await asyncio.sleep(delay_seconds)

    console.print(f"[red]❌ Remote host file not found after {max_attempts} attempts[/red]")
    raise RuntimeError(
        f"Failed to read valid host:port from {remote_path} on {cfg.target} "
        f"after {max_attempts} tries"
    ) from last_error


class SSHForwardProcess:
    def __init__(self, process: Popen, local_port: int, remote_host: str, remote_port: int):
        self.process = process
        self.local_port = local_port
        self.remote_host = remote_host
        self.remote_port = remote_port

    def stop(self):
        if self.process.poll() is None:
            try:
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
            except Exception as e:
                console.log(f"[red]Fehler beim Beenden des Port-Forwardings: {e}[/red]")
        else:
            console.log("[yellow]SSH-Forwarding-Prozess war bereits beendet.[/yellow]")

@beartype
def find_process_using_port(port: int) -> Optional[tuple[int, str]]:
    try:
        for conn in psutil.net_connections(kind="inet"):
            if conn.laddr and conn.laddr.port == port:
                if conn.pid is not None:
                    try:
                        proc = psutil.Process(conn.pid)
                        return (proc.pid, proc.name())
                    except psutil.NoSuchProcess:
                        return (conn.pid, "Unknown")
        return None
    except Exception as e:
        return None

@beartype
def start_port_forward(cfg, remote_host: str, remote_port: int, local_port: int) -> SSHForwardProcess:
    from rich.console import Console
    console = Console()

    console.rule("[bold]Starting Port Forwarding[/bold]")

    try:
        if not cfg.jumphost_url:
            raise ValueError("Jumphost URL ist nicht gesetzt!")

        # --- Port already in use? ---
        existing_proc_info = find_process_using_port(local_port)
        if existing_proc_info:
            pid, name = existing_proc_info
            raise RuntimeError(
                f"Local port {local_port} already used by PID {pid} ({name})"
            )

        ssh_cmd_parts = [
            "ssh",
            "-L", f"{local_port}:{remote_host}:{remote_port}",
            "-N",
            "-T",
        ]

        # Falls ein Jumphost gesetzt ist, nutzen wir ihn über ProxyJump oder ProxyCommand
        if hasattr(cfg, "proxyjump") and cfg.proxyjump:
            ssh_cmd_parts += ["-J", cfg.proxyjump]
            console.log(f"Using ProxyJump: {cfg.proxyjump}")
        else:
            ssh_cmd_parts += ["-o", f"ProxyCommand=ssh -W %h:%p {shlex.quote(cfg.jumphost_url)}"]
            console.log(f"Using ProxyCommand: ssh -W %h:%p {shlex.quote(cfg.jumphost_url)}")

        if hasattr(cfg, "identity_file") and cfg.identity_file:
            ssh_cmd_parts += ["-i", cfg.identity_file]
            console.log(f"Using IdentityFile: {cfg.identity_file}")

        ssh_cmd_parts.append(cfg.target)

        ssh_cmd_str = " ".join(shlex.quote(part) for part in ssh_cmd_parts)
        console.log(f"SSH-Forward-Command: {ssh_cmd_str}")

        process = Popen(
            ssh_cmd_parts,
            stdout=DEVNULL,
            stderr=PIPE,
            preexec_fn=os.setsid  # eigene Prozessgruppe für sauberes Beenden
        )

        # Kurzes Warten, um zu prüfen, ob Prozess korrekt startet
        import time
        time.sleep(1.0)
        if process.poll() is not None:
            err_output = process.stderr.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"SSH-Forwarding failed:\n{err_output}")

        console.log(f"[green]Port forwarding is running: localhost:{local_port} -> {remote_host}:{remote_port}[/green]")
        return SSHForwardProcess(process, local_port, remote_host, remote_port)

    except Exception as e:
        console.log(f"[red]Error while trying to port-forward: {e}[/red]")
        raise

# ──────────────────────────────────────────────────────────────────────────────
# main entry‑point
# ──────────────────────────────────────────────────────────────────────────────

@beartype
def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(
            """\
            Run or proxy the *hpc_system_server_runner* job on an HPC cluster.

            Examples
            --------
              # Basic usage
              python hpc_remote_runner.py \\
                  --hpc-system-url user@login.hpc.example.com \\
                  --fallback-system-url user@login2.hpc.example.com \\
                  --hpc-script-dir ./my_slurm_scripts \\
                  --copy --debug

              # Limit to one retry
              python hpc_remote_runner.py ... --retries 1
            """
        ),
    )

    parser.add_argument("--hpc-system-url", required=True, help="SSH target for primary HPC head-node (user@host)")
    parser.add_argument("--fallback-system-url", help="SSH target for fallback HPC head-node")
    parser.add_argument("--jumphost-url", help="Optional SSH jumphost in user@host form")
    parser.add_argument("--local-hpc-script-dir", required=True, type=Path, help="Local directory containing Slurm scripts")
    parser.add_argument("--hpc-script-dir", required=True, type=Path, help="Directory on the HPC System where the files should be copied to")
    parser.add_argument("--copy", action="store_true", help="If set, rsync the script directory before anything else")
    parser.add_argument("--debug", action="store_true", help="Verbose local shell output")
    parser.add_argument("--retries", type=int, default=3, help="SSH retry attempts before using fallback")
    parser.add_argument("--local-port", type=int, default=8000, help="Local port to expose the remote service")
    parser.add_argument("--username", default=getpass.getuser(), help="SSH username for HPC and (by default) also for jumphost")
    parser.add_argument("--jumphost-username", help="SSH username for jumphost (defaults to --username)")

    return parser


@beartype
async def run_with_host(cfg: SSHConfig, local_script_dir: Path) -> tuple[bool, Optional[SSHForwardProcess]]:
    """
    Execute the entire workflow for *one* remote host.

    Returns (success, forwarder-or-None).
    """
    try:
        await verify_slurm_and_key(cfg)
        if args.copy:
            await rsync_scripts(cfg, local_script_dir, args.hpc_script_dir)
        await ensure_job_running(cfg, to_absolute(args.hpc_script_dir))
        host, port = await read_remote_host_port(cfg)
        fwd = start_port_forward(cfg, host, port, args.local_port)
        return True, fwd
    except Exception as exc:  # noqa: BLE001
        console.print_exception()
        console.print(f"[red]❌  Host {cfg.target} failed: {exc}[/red]")
        return False, None


async def main() -> None:  # noqa: C901 – a bit long but readable
    global args
    parser = build_cli()
    args = parser.parse_args()

    if not args.jumphost_username:
        args.jumphost_username = args.username

    console.print(f":rocket:  Starting with [bold]{args.hpc_system_url}[/bold]  (retries={args.retries})")

    target_url = f"{args.username}@{args.hpc_system_url}"
    jumphost_url = f"{args.jumphost_username}@{args.jumphost_url}"

    # Try primary host
    primary_cfg = SSHConfig(
        target          = target_url,
        jumphost_url    = jumphost_url,
        retries         = args.retries,
        debug           = args.debug,
        username        = args.username,
        jumphost_username = args.jumphost_username
    )
    ok, fwd = await run_with_host(primary_cfg, args.local_hpc_script_dir)
    if ok:
        console.print("[bold green]✓  All done – tunnel is up.  Press Ctrl+C to stop.[/bold green]")
        try:
            while True:  # Keep process alive
                await asyncio.sleep(10)
        except KeyboardInterrupt:
            console.print("\n[cyan]Stopping tunnel…[/cyan]")
            fwd.stop()
            return

    if args.fallback_system_url:
        target_url = f"{args.username}@{args.fallback_system_url}"

        console.print("[yellow]Trying fallback host…[/yellow]")
        fallback_cfg = SSHConfig(
            target          = target_url,
            jumphost_url    = jumphost_url,
            retries         = args.retries,
            debug           = args.debug,
            username        = args.username,
            jumphost_username = args.jumphost_username
        )
        ok, fwd = await run_with_host(fallback_cfg, args.local_hpc_script_dir)
        if not ok:
            console.print("[bold red]Both hosts failed.  Giving up.[/bold red]")
            sys.exit(1)

        console.print("[bold green]✓  Tunnel to fallback host established.  Press Ctrl+C to stop.[/bold green]")
    else:
        console.print("[red]❌No fallback host defined. Use --fallback-system-url to define a fallback-host[/red]")

        if fwd is not None:
            fwd.stop()
        sys.exit(1)
    try:
        while True:
            await asyncio.sleep(10)
    except KeyboardInterrupt:
        console.print("\n[cyan]Stopping tunnel…[/cyan]")
        fwd.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[red]Interrupted by user.[/red]")
