"""Docker support: build and run NES benchmarks in containers."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any


class DockerManager:
    """Manages Docker builds and container execution for NES benchmarks."""

    def __init__(
        self,
        source_dir: str | Path,
        image_prefix: str = "nebulastream",
        build_type: str = "RelWithDebInfo",
    ):
        self.source_dir = Path(source_dir)
        self.image_prefix = image_prefix
        self.build_type = build_type

    @property
    def image_name(self) -> str:
        return f"{self.image_prefix}:bench"

    def build(self, branch: str | None = None, verbose: bool = False) -> None:
        """Build the NES Docker image for benchmarking.

        If branch is specified, checks out that branch before building.
        """
        if branch:
            subprocess.run(
                ["git", "-C", str(self.source_dir), "checkout", branch],
                check=True,
                capture_output=not verbose,
            )

        cmd = [
            "docker", "build",
            "-t", self.image_name,
            "--build-arg", f"BUILD_TYPE={self.build_type}",
            str(self.source_dir),
        ]

        if verbose:
            print(f"  docker build -t {self.image_name} ...")

        result = subprocess.run(cmd, capture_output=not verbose, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                f"Docker build failed (exit {result.returncode}):\n"
                f"{getattr(result, 'stderr', '')[:2000]}"
            )

    def run_systest(
        self,
        test_file: Path,
        worker_flags: list[str],
        data_dir: str | Path,
        topology_yaml: Path | None = None,
        data_size: str = "large",
    ) -> subprocess.CompletedProcess:
        """Run systest inside a Docker container."""
        data_dir = Path(data_dir)

        cmd = [
            "docker", "run", "--rm",
            "-v", f"{test_file}:/tmp/test.test:ro",
            "-v", f"{data_dir}:/data:ro",
        ]

        if topology_yaml:
            cmd.extend(["-v", f"{topology_yaml}:/tmp/topology.yaml:ro"])

        cmd.extend([
            self.image_name,
            "/build/nes-systests/systest/systest",
            "-t", "/tmp/test.test:1",
            "-e", data_size, "CompilationIntensive",
            "-b",
        ])

        if topology_yaml:
            cmd.extend(["-c", "/tmp/topology.yaml"])

        cmd.append("--")
        cmd.extend(worker_flags)

        return subprocess.run(cmd, capture_output=True, text=True, timeout=600)

    def run_worker(
        self,
        worker_flags: list[str],
        ports: list[int] | None = None,
        network: str = "host",
    ) -> subprocess.Popen:
        """Start a NES worker inside a Docker container."""
        cmd = [
            "docker", "run", "--rm",
            f"--network={network}",
        ]

        if ports:
            for port in ports:
                cmd.extend(["-p", f"{port}:{port}"])

        cmd.extend([
            self.image_name,
            "/build/nes-single-node-worker/nes-single-node-worker",
            *worker_flags,
        ])

        return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
