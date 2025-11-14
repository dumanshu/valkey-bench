#!/usr/bin/env python3
"""Capture normalized CPU usage for Envoy and Valkey in parallel via SSH."""

import argparse
import os
import shlex
import subprocess
import sys
import textwrap
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple

import boto3
import botocore
from botocore.config import Config

DEFAULT_REGION = "us-east-1"
DEFAULT_SEED = "vlklt-001"
DEFAULT_PROFILE = os.environ.get("AWS_PROFILE", "sandbox")
STACK_FMT = "valkey-loadtest-{seed}"
BOTO_CONFIG = Config(
    retries={"max_attempts": 10, "mode": "adaptive"},
    connect_timeout=15,
    read_timeout=60,
)


@dataclass
class InstanceInfo:
    role: str
    public_ip: str
    private_ip: str


def parse_args():
    parser = argparse.ArgumentParser(description="Capture Envoy and Valkey CPU splits.")
    parser.add_argument("--region", default=DEFAULT_REGION, help="AWS region for the stack (default: us-east-1).")
    parser.add_argument("--seed", default=DEFAULT_SEED, help="Stack seed, e.g. vlklt-001.")
    parser.add_argument("--aws-profile", default=DEFAULT_PROFILE, help="AWS profile name for discovery.")
    parser.add_argument("--client-ip", help="Override client public IP (auto-discovered if omitted).")
    parser.add_argument("--ssh-key", default="valkey-load-test-key.pem", help="Path to SSH key for ec2-user.")
    parser.add_argument("--envoy-ip", help="Override Envoy private IP (auto).")
    parser.add_argument("--valkey-ip", help="Override Valkey private IP (auto).")
    parser.add_argument("--samples", type=int, default=15, help="Number of 1s pidstat samples (captures duration seconds).")
    return parser.parse_args()


def aws_session(profile: str, region: str):
    try:
        return boto3.session.Session(profile_name=profile, region_name=region)
    except botocore.exceptions.ProfileNotFound as exc:
        raise SystemExit(f"AWS profile '{profile}' not found") from exc


def describe_stack_instances(region: str, seed: str, profile: str):
    stack = STACK_FMT.format(seed=seed)
    session = aws_session(profile, region)
    client = session.client("ec2", region_name=region, config=BOTO_CONFIG)
    filters = [
        {"Name": "tag:Project", "Values": [stack]},
        {"Name": "instance-state-name", "Values": ["pending", "running"]},
    ]
    resp = client.describe_instances(Filters=filters)
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
        raise SystemExit(f"No instances discovered for stack {stack}")
    return infos


def select_instance(instances: Dict[str, InstanceInfo], prefix: str) -> InstanceInfo:
    candidates = [info for role, info in instances.items() if role.startswith(prefix)]
    if not candidates:
        raise SystemExit(f"No instances found with role prefix '{prefix}'")
    # sort by numeric suffix if available
    candidates.sort(key=lambda x: int(x.role.split("-", 1)[1]) if "-" in x.role else 0)
    return candidates[0]


def build_ssh_command(client_ip: str, target_ip: str, key_path: Path, remote_script: str):
    proxy_command = (
        f"ssh -i {key_path} -o StrictHostKeyChecking=no -o IdentitiesOnly=yes "
        f"-o ConnectTimeout=10 ec2-user@{client_ip} -W %h:%p"
    )
    return [
        "ssh",
        "-i",
        str(key_path),
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        f"ProxyCommand={proxy_command}",
        f"ec2-user@{target_ip}",
        "bash",
        "-lc",
        shlex.quote(remote_script),
    ]


def remote_cpu_script(process_name: str, label: str, samples: int) -> str:
    return textwrap.dedent(
        f"""
        pid=$(pgrep {process_name} | head -n1)
        if [ -z "$pid" ]; then
          echo "{label}: process '{process_name}' not found" >&2
          exit 1
        fi
        cores=$(nproc)
        samples={samples}
        sudo pidstat -t -u -p "$pid" 1 "$samples" |
          awk -v cores="$cores" -v label="{label}" -v samples={samples} '
            $3 ~ /^[0-9]+$/ && $5 == pid {{
              global_usr+=$6; global_sys+=$7; global_n++
            }}
            $3 ~ /^[0-9]+$/ && $5 ~ /^[0-9]+$/ && $5 != pid {{
              perthread[$5" "$11]+=$6+$7
            }}
            END {{
              if (global_n==0 && global_usr==0 && global_sys==0) {{
                printf "%s CPU: USER=0 SYS=0 TOTAL=0\\n", label
              }} else {{
                user=(global_usr/(global_n>0?global_n:samples))/cores
                sys=(global_sys/(global_n>0?global_n:samples))/cores
                printf "%s CPU: USER=%.2f%% SYS=%.2f%% TOTAL=%.2f%%\\n", label, user, sys, user+sys
              }}
              if (length(perthread)) {{
                printf "%s per-thread (%%CPU):\\n", label
                for (thread in perthread) {{
                  printf "  %s: %.2f%%\\n", thread, perthread[thread]/samples/cores
                }}
              }}
            }}' pid=$pid
        """
    ).strip()


def run_capture(label: str, cmd: list) -> Tuple[str, subprocess.CompletedProcess]:
    result = subprocess.run(cmd, capture_output=True, text=True)
    return label, result


def main():
    args = parse_args()
    key_path = Path(args.ssh_key).expanduser().resolve()
    if not key_path.exists():
        raise SystemExit(f"SSH key not found: {key_path}")

    instances = describe_stack_instances(args.region, args.seed, args.aws_profile)
    client_instance = instances.get("client")
    if not client_instance:
        raise SystemExit("Client instance not found in stack discovery.")

    client_ip = args.client_ip or client_instance.public_ip
    if not client_ip:
        raise SystemExit("Client instance has no public IP; specify --client-ip manually.")

    envoy_ip = args.envoy_ip or select_instance(instances, "envoy").private_ip
    valkey_ip = args.valkey_ip or select_instance(instances, "valkey").private_ip
    if not envoy_ip or not valkey_ip:
        raise SystemExit("Unable to determine Envoy or Valkey IP; specify overrides.")

    tasks: Dict[str, list] = {
        "Envoy": build_ssh_command(
            client_ip,
            envoy_ip,
            key_path,
            remote_cpu_script("envoy", "Envoy", args.samples),
        ),
        "Valkey": build_ssh_command(
            client_ip,
            valkey_ip,
            key_path,
            remote_cpu_script("valkey-server", "Valkey", args.samples),
        ),
    }

    print(f"Capturing {args.samples} seconds of CPU data from Envoy and Valkey...\n", flush=True)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(run_capture, label, cmd): label for label, cmd in tasks.items()}
        for future in as_completed(futures):
            label = futures[future]
            _, result = future.result()
            if result.returncode != 0:
                print(f"[{label}] ERROR (exit {result.returncode}): {result.stderr.strip() or result.stdout.strip()}", flush=True)
            else:
                print(result.stdout.strip(), flush=True)


if __name__ == "__main__":
    main()
