#!/usr/bin/env python3
"""
Fetch Envoy latency histograms and print a simple table of p50/p90/p99.
Auto-discovers the stack by Project tag: valkey-loadtest-<seed>.
"""

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple

import boto3
import botocore
from botocore.config import Config

DEFAULT_REGION = "us-east-1"
DEFAULT_SEED = "vlklt-001"
DEFAULT_PROFILE = os.environ.get("AWS_PROFILE", "sandbox")
DEFAULT_STAT = "listener.workers.request_time"  # Envoy in-process request handling
DEFAULT_WINDOW = 30
BOTO_CONFIG = Config(
    retries={"max_attempts": 10, "mode": "adaptive"},
    connect_timeout=10,
    read_timeout=10,
)


@dataclass
class InstanceInfo:
    role: str
    public_ip: str
    private_ip: str


def aws_session(profile: str, region: str):
    try:
        return boto3.session.Session(profile_name=profile, region_name=region)
    except botocore.exceptions.ProfileNotFound as exc:
        raise SystemExit(f"AWS profile '{profile}' not found") from exc


def discover_instances(region: str, seed: str, profile: str) -> Dict[str, InstanceInfo]:
    stack = f"valkey-loadtest-{seed}"
    ec2 = aws_session(profile, region).client("ec2", region_name=region, config=BOTO_CONFIG)
    filters = [
        {"Name": "tag:Project", "Values": [stack]},
        {"Name": "instance-state-name", "Values": ["pending", "running"]},
    ]
    resp = ec2.describe_instances(Filters=filters)
    infos = {}
    for reservation in resp.get("Reservations", []):
        for inst in reservation.get("Instances", []):
            tags = {t["Key"]: t["Value"] for t in inst.get("Tags", [])}
            role = tags.get("Role") or tags.get("Name")
            if not role:
                continue
            infos[role] = InstanceInfo(
                role=role,
                public_ip=inst.get("PublicIpAddress"),
                private_ip=inst.get("PrivateIpAddress"),
            )
    if not infos:
        raise SystemExit(f"No instances found for stack {stack}")
    return infos


def ssh_cmd_via_client(client_ip: str, target_ip: str, key: Path, remote_cmd: str):
    proxy = (
        f"ssh -i {key} -o StrictHostKeyChecking=no -o IdentitiesOnly=yes "
        f"-o ConnectTimeout=5 ec2-user@{client_ip} -W %h:%p"
    )
    return [
        "ssh",
        "-i",
        str(key),
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "ConnectTimeout=5",
        "-o",
        f"ProxyCommand={proxy}",
        f"ec2-user@{target_ip}",
        "bash",
        "-lc",
        remote_cmd,
    ]


def fetch_histogram(client_ip: str, envoy_ip: str, key: Path, stat: str, window: int) -> str:
    cmd = f"""
set -euo pipefail
url=http://127.0.0.1:9901/stats?usedonly
curl -s "$url" | grep -E '^histogram\\.{stat}\\b' || true
sleep {window}
curl -s "$url" | grep -E '^histogram\\.{stat}\\b' || true
"""
    return subprocess.check_output(
        ssh_cmd_via_client(client_ip, envoy_ip, key, cmd),
        text=True,
        stderr=subprocess.STDOUT,
    )


def parse_histogram(text: str) -> Tuple[float, float, float]:
    def extract(metric: str) -> float:
        hits = re.findall(rf"{re.escape(metric)}\(([^,]+)", text)
        if hits:
            try:
                return float(hits[-1])
            except ValueError:
                return 0.0
        return 0.0

    return extract("P50"), extract("P90"), extract("P99")


def print_table(rows: Dict[str, Tuple[float, float, float]], stat: str, window: int):
    roles = sorted(rows.keys())
    print(f"\nEnvoy latency percentiles for '{stat}' over ~{window}s:\n")
    print(f"{'Envoy':<12} {'P50(ms)':>10} {'P90(ms)':>10} {'P99(ms)':>10}")
    print("-" * 46)
    for r in roles:
        p50, p90, p99 = rows[r]
        print(f"{r:<12} {p50:10.2f} {p90:10.2f} {p99:10.2f}")
    print("")


def main():
    ap = argparse.ArgumentParser(description="Report Envoy latency percentiles in a table.")
    ap.add_argument("--region", default=DEFAULT_REGION)
    ap.add_argument("--seed", default=DEFAULT_SEED)
    ap.add_argument("--aws-profile", default=DEFAULT_PROFILE)
    default_key = Path(__file__).resolve().with_name("valkey-load-test-key.pem")
    ap.add_argument("--ssh-key", default=str(default_key), help=f"Path to SSH key for ec2-user (default: {default_key})")
    ap.add_argument("--client-ip", help="Override client public IP (auto if omitted)")
    ap.add_argument("--stat-name", default=DEFAULT_STAT, help="Histogram stat name without 'histogram.'")
    ap.add_argument("--window-seconds", type=int, default=DEFAULT_WINDOW, help="Seconds between snapshots")
    ap.add_argument("--print-raw", action="store_true", help="Also print raw histogram lines per Envoy.")
    args = ap.parse_args()

    key_path = Path(args.ssh_key).expanduser().resolve()
    if not key_path.exists():
        raise SystemExit(f"SSH key not found: {key_path}")

    instances = discover_instances(args.region, args.seed, args.aws_profile)
    client = instances.get("client")
    if not client or not client.public_ip:
        raise SystemExit("Client instance with public IP not found; specify --client-ip.")
    client_ip = args.client_ip or client.public_ip

    envoy_nodes = [info for role, info in instances.items() if role.startswith("envoy")]
    if not envoy_nodes:
        raise SystemExit("No Envoy instances found.")
    envoy_nodes.sort(key=lambda x: int(x.role.split("-", 1)[1]) if "-" in x.role else 0)

    results: Dict[str, Tuple[float, float, float]] = {}
    raw_outputs: Dict[str, str] = {}
    for info in envoy_nodes:
        sys.stdout.write(f"Collecting {args.stat_name} from {info.role}...\n")
        sys.stdout.flush()
        raw = fetch_histogram(client_ip, info.private_ip, key_path, args.stat_name, args.window_seconds)
        raw_outputs[info.role] = raw
        results[info.role] = parse_histogram(raw)

    print_table(results, args.stat_name, args.window_seconds)
    if args.print_raw:
        for role in sorted(raw_outputs.keys()):
            print(f"\nRaw histogram lines for {role}:\n{raw_outputs[role]}\n")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(f"Command failed: {exc}\nOutput:\n{exc.output}\n")
        sys.exit(exc.returncode)
