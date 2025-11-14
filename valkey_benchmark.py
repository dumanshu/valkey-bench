#!/usr/bin/env python3
"""
Run valkey-benchmark against the Valkey load-test stack with convenient defaults and
capture a short perf flamegraph on the target node mid-run.
"""

import argparse
import os
import shlex
import shutil
import subprocess
import sys
import threading
import time
import textwrap
from datetime import datetime
from pathlib import Path
from typing import Optional

import boto3
from botocore.config import Config

DEFAULT_REGION = "us-east-1"
DEFAULT_SEED = "vlklt-001"
DEFAULT_PROFILE = os.environ.get("AWS_PROFILE", "sandbox")
DEFAULT_TARGET = "valkey-loadtest-vlklt-001-nlb-194011211c572f50.elb.us-east-1.amazonaws.com"
DEFAULT_PORT = 6379
DEFAULT_S3_BUCKET = "valkey-loadtest-vlklt-001-flamegraphs"

BOTO_CONFIG = Config(
    retries={"max_attempts": 10, "mode": "adaptive"},
    connect_timeout=15,
    read_timeout=60,
)


def build_proxy_option(jump_host: Optional[str], jump_user: str, jump_key: Optional[Path]) -> Optional[str]:
    if not jump_host:
        return None
    proxy = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "ConnectTimeout=10",
    ]
    if jump_key:
        proxy += ["-i", str(jump_key)]
    proxy += [f"{jump_user}@{jump_host}", "-W", "%h:%p"]
    return " ".join(shlex.quote(part) for part in proxy)


def build_remote_cmd(
    command: str,
    host: Optional[str],
    user: str,
    key: Optional[Path],
    use_bash: bool = False,
    jump_host: Optional[str] = None,
    jump_user: Optional[str] = None,
    jump_key: Optional[Path] = None,
):
    base = []
    if host:
        base = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "ConnectTimeout=10",
        ]
        if key:
            base += ["-i", str(key)]
        proxy = build_proxy_option(jump_host, jump_user or user, jump_key)
        if proxy:
            base += ["-o", f"ProxyCommand={proxy}"]
        base += [f"{user}@{host}"]
        if use_bash:
            base += ["bash", "-lc", command]
        else:
            base += [command]
    else:
        if use_bash:
            base = ["bash", "-lc", command]
        else:
            base = command.split()
    return base


def ec2_client(profile: Optional[str], region: str):
    session = boto3.session.Session(profile_name=profile, region_name=region)
    return session.client("ec2", region_name=region, config=BOTO_CONFIG)


def discover_valkey_ip(region: str, profile: Optional[str], seed: str) -> str:
    client = ec2_client(profile, region)
    stack = f"valkey-loadtest-{seed}"
    filters = [
        {"Name": "tag:Project", "Values": [stack]},
        {"Name": "instance-state-name", "Values": ["pending", "running"]},
    ]
    resp = client.describe_instances(Filters=filters)
    candidates = []
    for reservation in resp.get("Reservations", []):
        for inst in reservation.get("Instances", []):
            tags = {tag["Key"]: tag["Value"] for tag in inst.get("Tags", [])}
            role = tags.get("Role", "")
            if role.startswith("valkey-"):
                ip = inst.get("PrivateIpAddress")
                if ip:
                    try:
                        index = int(role.rsplit("-", 1)[1])
                    except (IndexError, ValueError):
                        index = 0
                    candidates.append((index, ip))
    if not candidates:
        raise SystemExit("ERROR: Unable to discover a Valkey node; specify --target-host explicitly.")
    candidates.sort(key=lambda item: item[0])
    return candidates[0][1]


def scp_from(
    host: str,
    user: str,
    key: Optional[Path],
    remote_path: str,
    local_path: Path,
    jump_host: Optional[str] = None,
    jump_user: Optional[str] = None,
    jump_key: Optional[Path] = None,
):
    cmd = [
        "scp",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "ConnectTimeout=10",
    ]
    if key:
        cmd += ["-i", str(key)]
    proxy = build_proxy_option(jump_host, jump_user or user, jump_key)
    if proxy:
        cmd += ["-o", f"ProxyCommand={proxy}"]
    cmd += [f"{user}@{host}:{remote_path}", str(local_path)]
    subprocess.run(cmd, check=True)


def copy_to_downloads(local_path: Path) -> Optional[Path]:
    downloads_dir = Path.home() / "Downloads"
    try:
        downloads_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        print(f"Warning: Unable to create {downloads_dir}: {exc}")
        return None
    destination = downloads_dir / local_path.name
    try:
        shutil.copy2(local_path, destination)
    except OSError as exc:
        print(f"Warning: Failed to copy flamegraph to {destination}: {exc}")
        return None
    return destination


def stream_process_output(proc: subprocess.Popen, log_path: Path):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log:
        for line in proc.stdout:
            print(line, end="")
            log.write(line)
            log.flush()
    proc.wait()


def parse_args():
    parser = argparse.ArgumentParser(description="Run valkey-benchmark and capture a flamegraph.")
    parser.add_argument("--target-host", default=DEFAULT_TARGET, help=f"Host/IP to benchmark (default: {DEFAULT_TARGET})")
    parser.add_argument("--target-port", type=int, default=DEFAULT_PORT, help="Valkey/Envoy TCP port (default: 6379)")
    parser.add_argument(
        "--mode",
        choices=["proxy", "valkey"],
        default="proxy",
        help="Use 'proxy' to hit Envoy/NLB (default) or 'valkey' to target a Valkey node directly.",
    )
    parser.add_argument("--region", default=DEFAULT_REGION, help="AWS region for stack discovery (default: us-east-1)")
    parser.add_argument("--seed", default=DEFAULT_SEED, help="Stack seed used in the Project tag (default: vlklt-001)")
    parser.add_argument("--tests", default="SET,GET", help="Comma-separated valkey-benchmark -t argument.")
    parser.add_argument("--operations", type=int, default=100_000_000, help="Total operations (-n).")
    parser.add_argument("--clients", type=int, default=256, help="Concurrent clients (-c).")
    parser.add_argument("--random-range", type=int, default=3_000_000, help="Key range (-r).")
    parser.add_argument("--threads", type=int, default=6, help="valkey-benchmark --threads value.")
    parser.add_argument("--data-size", type=int, default=1024, help="Payload size (-d).")
    parser.add_argument("--ssh-host", required=True, help="Host where valkey-benchmark should be executed (e.g., client bastion).")
    parser.add_argument("--ssh-user", default="ec2-user", help="SSH user for benchmark host (default: ec2-user).")
    parser.add_argument("--ssh-key", type=Path, help="Path to SSH private key for benchmark host.")
    parser.add_argument("--log-file", type=Path, default=Path("valkey-benchmark.log"), help="Local file to store benchmark output.")
    parser.add_argument("--profile-delay", type=int, default=60, help="Seconds to wait before capturing perf data.")
    parser.add_argument("--profile-duration", type=int, default=30, help="Duration (seconds) of perf capture.")
    parser.add_argument("--flamegraph-host", help="SSH host to capture perf on (defaults to --target-host if reachable).")
    parser.add_argument("--flamegraph-user", default="ec2-user", help="SSH user for flamegraph host.")
    parser.add_argument("--flamegraph-key", type=Path, help="SSH private key for flamegraph host (defaults to --ssh-key).")
    parser.add_argument("--flamegraph-jump-host", help="Optional bastion host used to reach the flamegraph host (default: --ssh-host).")
    parser.add_argument("--flamegraph-jump-user", help="Jump host SSH user (default: --ssh-user).")
    parser.add_argument("--flamegraph-jump-key", type=Path, help="Jump host SSH key (default: --ssh-key).")
    parser.add_argument("--flamegraph-tools", default="~/FlameGraph", help="Path to FlameGraph utilities on remote host.")
    parser.add_argument("--flamegraph-remote-path", default="/tmp/valkey-flamegraph.svg", help="Remote SVG output path.")
    parser.add_argument("--flamegraph-output", type=Path, default=Path("valkey-flamegraph.svg"), help="Local path for downloaded flamegraph.")
    parser.add_argument("--flamegraph-frequency", type=int, default=99, help="perf record sampling frequency.")
    parser.add_argument("--skip-flamegraph", action="store_true", help="Disable perf capture.")
    parser.add_argument("--s3-bucket", default=DEFAULT_S3_BUCKET, help="Upload flamegraph SVG to this S3 bucket (blank to skip).")
    parser.add_argument("--s3-prefix", default="profiles", help="Key prefix within the S3 bucket.")
    parser.add_argument("--aws-profile", default=DEFAULT_PROFILE, help="AWS profile for discovery/S3 uploads (default: env/instance profile).")
    return parser.parse_args()


def build_benchmark_command(args):
    cmd = [
        "valkey-benchmark",
        f"-h {shlex.quote(args.target_host)}",
        f"-p {args.target_port}",
        f"-t {args.tests}",
        f"-n {args.operations}",
        f"-c {args.clients}",
        f"-r {args.random_range}",
        f"--threads {args.threads}",
        f"-d {args.data_size}",
    ]
    return " ".join(cmd)


def run_flamegraph_capture(args, perf_host, perf_user, perf_key, jump_host=None, jump_user=None, jump_key=None):
    data_path = "/tmp/perf-valkey.data"
    script = textwrap.dedent(
        f"""
        set -euo pipefail
        sudo rm -f {data_path} {args.flamegraph_remote_path}
        sudo perf record -F {args.flamegraph_frequency} -a -g -o {data_path} -- sleep {args.profile_duration}
        sudo perf script -i {data_path} | {args.flamegraph_tools}/stackcollapse-perf.pl --kernel | {args.flamegraph_tools}/flamegraph.pl > {args.flamegraph_remote_path}
        """
    ).strip()
    cmd = build_remote_cmd(
        script,
        perf_host,
        perf_user,
        perf_key,
        use_bash=True,
        jump_host=jump_host,
        jump_user=jump_user,
        jump_key=jump_key,
    )
    subprocess.run(cmd, check=True)
    scp_from(
        perf_host,
        perf_user,
        perf_key,
        args.flamegraph_remote_path,
        args.flamegraph_output,
        jump_host=jump_host,
        jump_user=jump_user,
        jump_key=jump_key,
    )


def upload_flamegraph_to_s3(local_path: Path, bucket: str, prefix: str, profile: Optional[str]):
    session = boto3.session.Session(profile_name=profile) if profile else boto3.session.Session()
    client = session.client("s3")
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    key = f"{prefix.rstrip('/')}/valkey-flamegraph-{timestamp}.svg"
    client.upload_file(str(local_path), bucket, key, ExtraArgs={"ContentType": "image/svg+xml"})
    url = client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=7 * 24 * 3600,
    )
    return key, url


def main():
    args = parse_args()
    if args.mode == "valkey" and (args.target_host == DEFAULT_TARGET or not args.target_host):
        print("Discovering Valkey node IP via AWS tags...")
        args.target_host = discover_valkey_ip(args.region, args.aws_profile, args.seed)
        print(f"Discovered Valkey host: {args.target_host}")
    target_cmd = build_benchmark_command(args)
    print(f"Executing benchmark command: {target_cmd}")

    ssh_key = args.ssh_key.expanduser() if args.ssh_key else None
    flame_key = args.flamegraph_key.expanduser() if args.flamegraph_key else ssh_key
    flame_jump_key = args.flamegraph_jump_key.expanduser() if args.flamegraph_jump_key else ssh_key

    if ssh_key and not ssh_key.exists():
        raise SystemExit(f"SSH key not found: {ssh_key}")
    if flame_key and not flame_key.exists():
        raise SystemExit(f"Flamegraph SSH key not found: {flame_key}")
    if flame_jump_key and not flame_jump_key.exists():
        raise SystemExit(f"Flamegraph jump SSH key not found: {flame_jump_key}")

    benchmark_cmd = build_remote_cmd(target_cmd, args.ssh_host, args.ssh_user, ssh_key)
    print(f"Remote command: {' '.join(shlex.quote(part) for part in benchmark_cmd)}")
    bench_proc = subprocess.Popen(
        benchmark_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    output_thread = threading.Thread(target=stream_process_output, args=(bench_proc, args.log_file))
    output_thread.start()

    flamegraph_proc = None
    perf_host = None
    if not args.skip_flamegraph:
        if not args.flamegraph_host:
            raise SystemExit("Please provide --flamegraph-host (Envoy) when capturing perf data.")
        perf_host = args.flamegraph_host
    jump_host = args.flamegraph_jump_host or args.ssh_host
    jump_user = args.flamegraph_jump_user or args.ssh_user

    start_ts = time.time()
    try:
        while True:
            if bench_proc.poll() is not None:
                break
            elapsed = time.time() - start_ts
            if (
                not args.skip_flamegraph
                and perf_host
                and flamegraph_proc is None
                and elapsed >= args.profile_delay
            ):
                flamegraph_proc = threading.Thread(
                    target=run_flamegraph_capture,
                    args=(args, perf_host, args.flamegraph_user, flame_key, jump_host, jump_user, flame_jump_key),
                )
                flamegraph_proc.start()
            time.sleep(1)
    finally:
        bench_proc.wait()
        output_thread.join()
        if flamegraph_proc:
            flamegraph_proc.join()

    print("Benchmark complete.")
    if not args.skip_flamegraph:
        if args.flamegraph_output.exists():
            print(f"Flamegraph saved to {args.flamegraph_output.resolve()}")
            downloads_copy = copy_to_downloads(args.flamegraph_output)
            if downloads_copy:
                print(f"Flamegraph also copied to {downloads_copy}")
        else:
            print("Flamegraph capture did not produce an SVG.")
        if args.s3_bucket and args.flamegraph_output.exists():
            key, url = upload_flamegraph_to_s3(
                args.flamegraph_output,
                args.s3_bucket,
                args.s3_prefix,
                args.aws_profile,
            )
            print(f"Uploaded flamegraph to s3://{args.s3_bucket}/{key}")
            print(f"Presigned URL (7d): {url}")
    print(f"Benchmark log saved to {args.log_file.resolve()}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
