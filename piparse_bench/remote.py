"""Remote execution: SSH deploy, run, fetch results."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import yaml

_DEFAULT_CONFIG_PATHS = [
    Path("remote.yaml"),
    Path.home() / ".piparse-bench" / "remote.yaml",
]

PROJECT_ROOT = Path(__file__).parent.parent


class RemoteConfig:
    """Remote server configuration."""

    def __init__(
        self,
        host: str,
        user: str,
        port: int = 22,
        deploy_path: str = "/home/user/piparse-bench",
        build_dir: str | None = None,
        generator_bin: str | None = None,
        ssh_key: str | None = None,
    ):
        self.host = host
        self.user = user
        self.port = port
        self.deploy_path = deploy_path
        self.build_dir = build_dir
        self.generator_bin = generator_bin
        self.ssh_key = ssh_key

    @classmethod
    def load(cls, path: str | Path | None = None) -> "RemoteConfig":
        """Load remote config from YAML file."""
        if path:
            config_path = Path(path)
        else:
            config_path = None
            for p in _DEFAULT_CONFIG_PATHS:
                if p.exists():
                    config_path = p
                    break
            if config_path is None:
                raise FileNotFoundError(
                    f"No remote.yaml found. Searched: {[str(p) for p in _DEFAULT_CONFIG_PATHS]}"
                )

        with open(config_path) as f:
            raw = yaml.safe_load(f)

        r = raw.get("remote", raw)
        nes = r.get("nebulastream", {})

        return cls(
            host=r["host"],
            user=r.get("user", "user"),
            port=r.get("port", 22),
            deploy_path=r.get("deploy_path", "/home/user/piparse-bench"),
            build_dir=nes.get("build_dir"),
            generator_bin=r.get("generator_bin"),
            ssh_key=r.get("ssh_key"),
        )

    @property
    def ssh_target(self) -> str:
        return f"{self.user}@{self.host}"

    def _ssh_opts(self) -> list[str]:
        opts = ["-p", str(self.port)]
        if self.ssh_key:
            opts.extend(["-i", self.ssh_key])
        return opts

    def _rsync_ssh_cmd(self) -> str:
        parts = ["ssh", "-p", str(self.port)]
        if self.ssh_key:
            parts.extend(["-i", self.ssh_key])
        return " ".join(parts)


def deploy(config: RemoteConfig, verbose: bool = False) -> None:
    """Deploy the piparse-bench framework to the remote server via rsync."""
    excludes = [
        ".venv",
        "__pycache__",
        "*.pyc",
        ".egg-info",
        "piparse_bench.egg-info",
        "results/piparse_bench.duckdb",
        "results/csv",
        "results/figures",
        "generator/target",
        ".git",
    ]

    cmd = [
        "rsync", "-avz", "--delete",
        "-e", config._rsync_ssh_cmd(),
    ]
    for exc in excludes:
        cmd.extend(["--exclude", exc])

    cmd.extend([
        f"{PROJECT_ROOT}/",
        f"{config.ssh_target}:{config.deploy_path}/",
    ])

    if verbose:
        print(f"  rsync → {config.ssh_target}:{config.deploy_path}/")

    result = subprocess.run(cmd, capture_output=not verbose, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"rsync failed (exit {result.returncode}):\n{getattr(result, 'stderr', '')}"
        )


def run_remote(
    config: RemoteConfig,
    experiment_paths: list[str],
    extra_args: list[str] | None = None,
    verbose: bool = False,
) -> None:
    """Run experiments on the remote server via SSH."""
    # Build the remote command
    remote_exp_paths = []
    for p in experiment_paths:
        # Convert local paths to remote paths relative to deploy_path
        rel = Path(p).resolve().relative_to(PROJECT_ROOT.resolve())
        remote_exp_paths.append(f"{config.deploy_path}/{rel}")

    parts = [
        f"cd {config.deploy_path}",
        "&&",
        "python3", "bench.py", "run",
    ]

    if config.build_dir:
        parts.extend(["--build-dir", config.build_dir])
    if config.generator_bin:
        parts.extend(["--generator-bin", config.generator_bin])

    parts.extend(remote_exp_paths)

    if extra_args:
        parts.extend(extra_args)

    remote_cmd = " ".join(parts)

    ssh_cmd = [
        "ssh",
        *config._ssh_opts(),
        config.ssh_target,
        remote_cmd,
    ]

    if verbose:
        print(f"  ssh {config.ssh_target}: {remote_cmd}")

    result = subprocess.run(ssh_cmd, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Remote execution failed (exit {result.returncode})")


def fetch(config: RemoteConfig, verbose: bool = False) -> None:
    """Fetch results from the remote server back to local."""
    local_results = PROJECT_ROOT / "results"
    local_results.mkdir(parents=True, exist_ok=True)

    cmd = [
        "rsync", "-avz",
        "-e", config._rsync_ssh_cmd(),
        f"{config.ssh_target}:{config.deploy_path}/results/",
        f"{local_results}/",
    ]

    if verbose:
        print(f"  rsync ← {config.ssh_target}:{config.deploy_path}/results/")

    result = subprocess.run(cmd, capture_output=not verbose, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"rsync failed (exit {result.returncode}):\n{getattr(result, 'stderr', '')}"
        )
