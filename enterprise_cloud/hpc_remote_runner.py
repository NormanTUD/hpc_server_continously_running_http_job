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
    the current user; if not, submits `{hpc_script_dir}/slurm.sbatch`.
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
from pathlib import Path
from typing import Final, Optional

from beartype import beartype
from rich.console import Console
from rich.table import Table
from rich.text import Text
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_fixed

try:
    import paramiko
    from sshtunnel import SSHTunnelForwarder
except ImportError as err:  # pragma: no cover
    sys.exit(
        "Missing runtime dependencies.  Run `pip install -r requirements.txt` "
        f"first!  ({err})"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Globals & helpers
# ──────────────────────────────────────────────────────────────────────────────

console: Final = Console(highlight=False)


@dataclass(slots=True)
class SSHConfig:
    target: str
    jumphost: Optional[str]
    retries: int
    debug: bool


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
    if cfg.jumphost:
        options.extend(["-J", cfg.jumphost])

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
                msg = Text(f"SSH command failed (attempt {attempt.retry_state.attempt_number}): ", style="bold red")
                msg.append(remote_cmd)
                msg.append_text(f"\n{cp.stderr.strip()}", style="red")
                console.print(msg)
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
    local_dir: Path,
    remote_dir: str,
) -> None:
    """Rsync local script directory to remote."""
    if not local_dir.is_dir():
        console.print(f"[red]{local_dir} is not a directory.[/red]")
        sys.exit(1)

    console.rule("[bold]Synchronising script directory[/bold]")
    rsync_cmd = (
        f"rsync -az --delete "
        f"{shlex.quote(str(local_dir))}/ "
        f"{cfg.target}:{shlex.quote(remote_dir)}/"
    )
    # (jumphost w/ rsync: use ProxyJump via SSH config file or rely on our ssh options)
    cp = run_local(rsync_cmd, debug=cfg.debug)
    if cp.returncode:
        console.print(f"[red]rsync failed:[/red] {cp.stderr}")
        sys.exit(cp.returncode)

    console.print(f"[green]✓ {local_dir} → {cfg.target}:{remote_dir} updated.[/green]")


@beartype
async def ensure_job_running(
    cfg: SSHConfig,
    remote_script_dir: str,
) -> None:
    """
    Ensure a Slurm job named 'hpc_system_server_runner' is running for $USER.
    Submit the sbatch script if necessary.
    """
    console.rule("[bold]Ensuring server job is active[/bold]")
    user_expr = "$(whoami)"
    job_name = "hpc_system_server_runner"

    # check if job exists
    list_cmd = f"squeue -u {user_expr} -h -o '%j' | grep -Fx {job_name} || true"
    cp = await ssh_run(cfg, list_cmd)
    if cp.stdout.strip() == job_name:
        console.print("[green]✓ Job already running.[/green]")
        return

    console.print("[yellow]Job not running – submitting…[/yellow]")
    sbatch_path = f"{remote_script_dir}/slurm.sbatch"
    submit_cmd = f"sbatch {shlex.quote(sbatch_path)}"
    cp = await ssh_run(cfg, submit_cmd, tty=True)
    console.print(cp.stdout.strip())

    console.print("[cyan]Waiting for job to appear in queue…[/cyan]")
    while True:
        cp = await ssh_run(cfg, list_cmd)
        if cp.stdout.strip() == job_name:
            console.print("[green]✓ Job now listed in squeue.[/green]")
            return
        await asyncio.sleep(30)


@beartype
async def read_remote_host_port(cfg: SSHConfig) -> tuple[str, int]:
    """
    Read ~/hpc_server_host_and_file which contains "host:port".
    """
    path = "~/hpc_server_host_and_file"
    cp = await ssh_run(cfg, f"cat {path}")
    host_port = cp.stdout.strip()
    try:
        host, port_s = host_port.split(":", 1)
        port = int(port_s)
    except ValueError as exc:  # pragma: no cover
        raise RuntimeError(f"Invalid host:port string in {path!s}: {host_port}") from exc
    console.print(f"[green]Remote server: {host}:{port}[/green]")
    return host, port


@beartype
def start_port_forward(
    cfg: SSHConfig,
    remote_host: str,
    remote_port: int,
    local_port: int = 8000,
) -> SSHTunnelForwarder:
    """
    Establish an SSH forwarder that maps :local_port → remote_host:remote_port.
    The forward goes through cfg.target (head‑node) and, optionally, cfg.jumphost.
    """
    console.rule("[bold]Opening SSH tunnel[/bold]")
    forwarder = SSHTunnelForwarder(
        (cfg.target.split("@")[-1], 22),
        ssh_username=(cfg.target.split("@")[0] if "@" in cfg.target else None),
        remote_bind_address=(remote_host, remote_port),
        local_bind_address=("localhost", local_port),
        ssh_pkey=os.path.expanduser("~/.ssh/id_rsa"),
        # Paramiko-style jump: we can set `ssh_proxy` here if jumphost
        ssh_proxy=paramiko.ProxyCommand(f"ssh -W %h:%p {shlex.quote(cfg.jumphost)}") if cfg.jumphost else None,
    )
    forwarder.start()
    console.print(f"[green]✓ Forwarding localhost:{local_port} → {remote_host}:{remote_port}[/green]")
    return forwarder


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
    parser.add_argument("--fallback-system-url", required=True, help="SSH target for fallback HPC head-node")
    parser.add_argument("--jumphost-url", help="Optional SSH jumphost in user@host form")
    parser.add_argument("--hpc-script-dir", required=True, type=Path, help="Local directory containing Slurm scripts")
    parser.add_argument("--copy", action="store_true", help="If set, rsync the script directory before anything else")
    parser.add_argument("--debug", action="store_true", help="Verbose local shell output")
    parser.add_argument("--retries", type=int, default=3, help="SSH retry attempts before using fallback")
    parser.add_argument("--local-port", type=int, default=8000, help="Local port to expose the remote service")
    return parser


@beartype
async def run_with_host(cfg: SSHConfig, local_script_dir: Path) -> tuple[bool, Optional[SSHTunnelForwarder]]:
    """
    Execute the entire workflow for *one* remote host.

    Returns (success, forwarder-or-None).
    """
    try:
        await verify_slurm_and_key(cfg)
        if args.copy:
            await rsync_scripts(cfg, local_script_dir, "$HOME/hpc_scripts")
        await ensure_job_running(cfg, "$HOME/hpc_scripts")
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

    console.print(f":rocket:  Starting with [bold]{args.hpc_system_url}[/bold]  (retries={args.retries})")

    # Try primary host
    primary_cfg = SSHConfig(args.hpc_system_url, args.jumphost_url, args.retries, args.debug)
    ok, fwd = await run_with_host(primary_cfg, args.hpc_script_dir)
    if ok:
        console.print("[bold green]✓  All done – tunnel is up.  Press Ctrl+C to stop.[/bold green]")
        try:
            while True:  # Keep process alive
                await asyncio.sleep(10)
        except KeyboardInterrupt:
            console.print("\n[cyan]Stopping tunnel…[/cyan]")
            fwd.stop()
            return

    console.print("[yellow]Trying fallback host…[/yellow]")
    fallback_cfg = SSHConfig(args.fallback_system_url, args.jumphost_url, args.retries, args.debug)
    ok, fwd = await run_with_host(fallback_cfg, args.hpc_script_dir)
    if not ok:
        console.print("[bold red]Both hosts failed.  Giving up.[/bold red]")
        sys.exit(1)

    console.print("[bold green]✓  Tunnel to fallback host established.  Press Ctrl+C to stop.[/bold green]")
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
