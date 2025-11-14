#!/usr/bin/env python3
"""Validation and benchmarking utility for Valkey load-test stacks."""

import argparse
import boto3
import botocore
import ipaddress
import json
import os
import shutil
import subprocess
import textwrap
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from botocore.config import Config


REGION = "us-east-1"
AWS_PROFILE = os.environ.get("AWS_PROFILE", "sandbox")
SEED = "vlklt-001"
STACK = f"valkey-loadtest-{SEED}"
OWNER = os.environ.get("OWNER", "")
REDIS_PORT = 6379
DEFAULT_BENCH_OPS = 200_000
DEFAULT_BENCH_CONCURRENCY = 100
DEFAULT_MTR_COUNT = 20
DEFAULT_MTR_INTERVAL = 0.2
HOURS_PER_MONTH = Decimal("730")

BOTO_CONFIG = Config(
    retries={"max_attempts": 10, "mode": "adaptive"},
    connect_timeout=15,
    read_timeout=60,
)

AWS_LOCATION_MAP = {
    "us-east-1": "US East (N. Virginia)",
    "us-west-2": "US West (Oregon)",
    "eu-west-1": "EU (Ireland)",
}

PRICE_CACHE = {}


def parse_args():
    parser = argparse.ArgumentParser(description="Validate Valkey load-test deployment")
    parser.add_argument("--region", default=REGION, help="AWS region (default: us-east-1)")
    parser.add_argument("--seed", default=SEED, help="Stack seed (default: vlklt-001)")
    parser.add_argument("--ssh-private-key-path", dest="ssh_key_path", default=str(Path(__file__).with_name("valkey-load-test-key.pem")), help="SSH key for ec2-user")
    parser.add_argument("--ssh-cidr", help="CIDR used for SSH bastion access (defaults to detected public IP)")
    parser.add_argument("--bench-operations", type=int, default=DEFAULT_BENCH_OPS, help="Total operations for read-only benchmark")
    parser.add_argument("--bench-concurrency", type=int, default=DEFAULT_BENCH_CONCURRENCY, help="Client concurrency for benchmark")
    parser.add_argument("--mtr-count", type=int, default=DEFAULT_MTR_COUNT, help="Number of probes for mtr reports")
    parser.add_argument("--mtr-interval", type=float, default=DEFAULT_MTR_INTERVAL, help="Seconds between mtr probes")
    return parser


def configure_runtime(region=None, seed=None):
    global REGION, SEED, STACK
    if region:
        REGION = region
    if seed:
        SEED = seed
    STACK = f"valkey-loadtest-{SEED}"


def ts():
    return datetime.now().strftime("[%Y-%m-%dT%H:%M:%S%z]")


def log(msg):
    print(f"{ts()} {msg}", flush=True)


def need_cmd(cmd):
    if shutil.which(cmd):
        return
    raise SystemExit(f"ERROR: Required command '{cmd}' not found in PATH.")


def my_public_cidr():
    with urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10) as resp:
        ip = resp.read().decode().strip()
    ipaddress.ip_address(ip)
    return f"{ip}/32"


def aws_session():
    try:
        return boto3.session.Session(profile_name=AWS_PROFILE, region_name=REGION)
    except botocore.exceptions.ProfileNotFound:
        raise SystemExit(f"ERROR: AWS profile '{AWS_PROFILE}' not found.")


def ec2():
    return aws_session().client("ec2", region_name=REGION, config=BOTO_CONFIG)


def elbv2():
    return aws_session().client("elbv2", region_name=REGION, config=BOTO_CONFIG)


def pricing():
    return aws_session().client("pricing", region_name="us-east-1", config=BOTO_CONFIG)


@dataclass
class InstanceInfo:
    role: str
    instance_id: str
    instance_type: str
    public_ip: str
    private_ip: str


@dataclass
class BootstrapContext:
    ssh_key_path: Path
    self_cidr: str
    client: InstanceInfo


def describe_stack_instances():
    filters = [
        {"Name": "tag:Project", "Values": [STACK]},
        {"Name": "instance-state-name", "Values": ["pending", "running"]},
    ]
    resp = ec2().describe_instances(Filters=filters)
    infos = {}
    for reservation in resp.get("Reservations", []):
        for inst in reservation.get("Instances", []):
            tags = {t["Key"]: t["Value"] for t in inst.get("Tags", [])}
            role = tags.get("Role") or tags.get("Name")
            if not role:
                continue
            infos[role] = InstanceInfo(
                role=role,
                instance_id=inst["InstanceId"],
                instance_type=inst.get("InstanceType", ""),
                public_ip=inst.get("PublicIpAddress"),
                private_ip=inst.get("PrivateIpAddress"),
            )
    return infos


def discover_envoy_lb_dns():
    name = f"{STACK}-nlb"
    try:
        resp = elbv2().describe_load_balancers(Names=[name])
    except botocore.exceptions.ClientError:
        return None
    lbs = resp.get("LoadBalancers", [])
    if not lbs:
        return None
    return lbs[0].get("DNSName")


def host_target_and_jump(host: InstanceInfo, ctx: BootstrapContext):
    target = host.public_ip or host.private_ip
    if not target:
        raise RuntimeError(f"{host.role} has no reachable IP")
    jump = None
    if host.role != ctx.client.role and not host.public_ip:
        if not ctx.client.public_ip:
            raise RuntimeError("Client missing public IP for ProxyJump")
        jump = ctx.client.public_ip
    return target, jump


def ssh_base_cmd(host: InstanceInfo, ctx: BootstrapContext):
    target, jump = host_target_and_jump(host, ctx)
    cmd = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=no",
        "-o", "IdentitiesOnly=yes",
        "-o", "ConnectTimeout=10",
        "-i", str(ctx.ssh_key_path),
    ]
    if jump:
        proxy = (
            "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "
            f"-o IdentitiesOnly=yes -i {ctx.ssh_key_path} ec2-user@{jump} -W %h:%p"
        )
        cmd += ["-o", f"ProxyCommand={proxy}"]
    cmd += [f"ec2-user@{target}"]
    return cmd


def ssh_run(host: InstanceInfo, script: str, ctx: BootstrapContext, strict=True):
    full = textwrap.dedent(script).lstrip()
    if strict:
        full = "set -euo pipefail\n" + full
    cmd = ssh_base_cmd(host, ctx) + ["bash", "-s"]
    subprocess.run(cmd, input=full, text=True, check=strict)


def ssh_capture(host: InstanceInfo, script: str, ctx: BootstrapContext, strict=True):
    full = textwrap.dedent(script).lstrip()
    if strict:
        full = "set -euo pipefail\n" + full
    cmd = ssh_base_cmd(host, ctx) + ["bash", "-s"]
    proc = subprocess.run(cmd, input=full, text=True, capture_output=True)
    if strict:
        proc.check_returncode()
    return proc


def cluster_summary(client: InstanceInfo, valkey_hosts, ctx: BootstrapContext):
    primary = valkey_hosts[0]
    if len(valkey_hosts) >= 3:
        log("Collecting cluster node map")
        proc = ssh_capture(
            client,
            f"valkey-cli --cluster nodes {primary.private_ip}:{REDIS_PORT} | sort",
            ctx,
            strict=False,
        )
        return proc.stdout.strip() or "(no cluster output)"
    log("Collecting standalone server info")
    proc = ssh_capture(client, f"valkey-cli -h {primary.private_ip} info server", ctx, strict=False)
    return proc.stdout.strip() or "(no server output)"


def keyspace_stats(client: InstanceInfo, valkey_hosts, ctx: BootstrapContext):
    stats = []
    total_keys = 0
    for host in valkey_hosts:
        proc = ssh_capture(client, f"valkey-cli -h {host.private_ip} info keyspace", ctx, strict=False)
        keys = 0
        for line in proc.stdout.splitlines():
            if line.startswith("db"):
                parts = line.split(":", 1)[1]
                for token in parts.split(","):
                    if token.startswith("keys="):
                        try:
                            keys = int(token.split("=", 1)[1])
                        except ValueError:
                            keys = 0
                break
        stats.append((host.role, keys))
        total_keys += keys
    return total_keys, stats


def run_readonly_benchmark(client: InstanceInfo, envoy_hosts, valkey_hosts, ctx: BootstrapContext, total_ops, concurrency, cluster_mode):
    if cluster_mode:
        if not envoy_hosts:
            return "No Envoy hosts available"
        target = envoy_hosts[0].private_ip
        log(f"Running read-only valkey-benchmark via {envoy_hosts[0].role} ({target})")
    else:
        target = valkey_hosts[0].private_ip
        log(f"Running read-only valkey-benchmark directly against {valkey_hosts[0].role} ({target})")
    cmd = (
        f"valkey-benchmark -h {target} -p {REDIS_PORT} "
        f"-t get -n {total_ops} -c {concurrency} -d 32"
    )
    if cluster_mode:
        cmd += " --cluster"
    proc = ssh_capture(client, cmd, ctx, strict=False)
    return proc.stdout.strip() or proc.stderr.strip() or "(no benchmark output)"


def format_mtr_microseconds(report_text: str) -> str:
    header_found = False
    lines = []
    for raw_line in report_text.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        lower = stripped.lower()
        if "loss%" in lower and "avg" in lower and "wrst" in lower:
            header_found = True
            continue
        if (
            lower.startswith("start:")
            or lower.startswith("host:")
            or lower.startswith("bitpattern")
            or lower.startswith("psize")
            or lower.startswith("packet size")
            or "packetsize" in lower
        ):
            continue
        if not header_found:
            continue
        sanitized = stripped.replace("|--", " ").replace("`--", " ")
        tokens = sanitized.split()
        if len(tokens) < 9:
            continue
        hop_token, host, loss_pct, sent = tokens[:4]
        hop = hop_token.rstrip(".")
        try:
            last_ms, avg_ms, best_ms, worst_ms, stdev_ms = map(float, tokens[4:9])
        except ValueError:
            continue
        lines.append(
            f"hop {hop:>2} {host:<18} sent={sent:<4} loss={loss_pct:<6} "
            f"avg={avg_ms * 1000:.1f}us best={best_ms * 1000:.1f}us "
            f"worst={worst_ms * 1000:.1f}us last={last_ms * 1000:.1f}us "
            f"stdev={stdev_ms * 1000:.1f}us"
        )
    return "\n".join(lines) if lines else report_text


def measure_rtt(source: InstanceInfo, target: InstanceInfo, ctx: BootstrapContext, count, interval, label):
    log(f"Running mtr RTT report {label}")
    cmd = f"sudo mtr -n -c {count} -i {interval} --report --report-wide {target.private_ip}"
    proc = ssh_capture(source, cmd, ctx, strict=False)
    raw = proc.stdout.strip() or proc.stderr.strip() or "(no mtr output)"
    return format_mtr_microseconds(raw)


def get_pricing_location(region):
    return AWS_LOCATION_MAP.get(region, "US East (N. Virginia)")


def instance_hourly_price(instance_type):
    if instance_type in PRICE_CACHE:
        return PRICE_CACHE[instance_type]
    try:
        loc = get_pricing_location(REGION)
        filters = [
            {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
            {"Type": "TERM_MATCH", "Field": "location", "Value": loc},
            {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
            {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
            {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
            {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
        ]
        resp = pricing().get_products(ServiceCode="AmazonEC2", Filters=filters, MaxResults=1)
        price_list = resp.get("PriceList")
        if not price_list:
            return None
        product = json.loads(price_list[0])
        terms = product["terms"].get("OnDemand", {})
        if not terms:
            return None
        term = next(iter(terms.values()))
        price_dimensions = term["priceDimensions"]
        dimension = next(iter(price_dimensions.values()))
        rate = Decimal(dimension["pricePerUnit"]["USD"])
        PRICE_CACHE[instance_type] = rate
        return PRICE_CACHE[instance_type]
    except botocore.exceptions.BotoCoreError:
        return None
    except botocore.exceptions.ClientError:
        return None


def summarize_costs(instances):
    summary = {}
    for host in instances:
        summary.setdefault(host.instance_type, 0)
        summary[host.instance_type] += 1
    rows = []
    total = Decimal("0")
    for itype, count in summary.items():
        price = instance_hourly_price(itype)
        hourly = price * count if price is not None else None
        monthly = hourly * HOURS_PER_MONTH if hourly is not None else None
        if monthly is not None:
            total += monthly
        rows.append((itype, count, price, monthly))
    return rows, total if total else None


def format_cost_rows(rows):
    if not rows:
        return "  (none)"
    lines = ["  Type             Count   Unit($/h)   Monthly($)"]
    for itype, count, price, monthly in rows:
        price_str = f"${float(price):.4f}" if price is not None else "unknown"
        monthly_str = f"${float(monthly):,.2f}" if monthly is not None else "unknown"
        lines.append(f"  {itype:<15} {count:<6} {price_str:<11} {monthly_str}")
    return "\n".join(lines)


def fmt_monthly(value):
    if value is None:
        return "unknown"
    return f"${float(value):,.2f}/mo"


def main():
    parser = parse_args()
    args = parser.parse_args()
    configure_runtime(region=args.region, seed=args.seed)

    key_path = Path(args.ssh_key_path).expanduser().resolve()
    if not key_path.exists():
        raise SystemExit(f"ERROR: SSH key not found: {key_path}")
    need_cmd("ssh")

    instances = describe_stack_instances()
    client = instances.get("client")
    if not client:
        raise SystemExit("ERROR: client instance not found for this stack")
    valkey_hosts = sorted(
        [info for role, info in instances.items() if role.startswith("valkey-")],
        key=lambda x: int(x.role.split("-", 1)[1]),
    )
    envoy_hosts = sorted(
        [info for role, info in instances.items() if role.startswith("envoy-")],
        key=lambda x: int(x.role.split("-", 1)[1]),
    )
    if not valkey_hosts:
        raise SystemExit("ERROR: No Valkey nodes discovered")
    if not envoy_hosts:
        raise SystemExit("ERROR: No Envoy nodes discovered")

    ctx = BootstrapContext(
        ssh_key_path=key_path,
        self_cidr=args.ssh_cidr or my_public_cidr(),
        client=client,
    )

    cluster_mode = len(valkey_hosts) >= 3
    log("Discovered instances:")
    for host in [client] + envoy_hosts + valkey_hosts:
        log(f"  {host.role}: public={host.public_ip or '-'} private={host.private_ip}")
    lb_dns = discover_envoy_lb_dns()
    if lb_dns:
        log(f"Envoy load balancer endpoint: {lb_dns}")

    key_path_str = str(key_path)
    if client.public_ip:
        log("SSH shortcuts:")
        log(f"  client: ssh -i {key_path_str} ec2-user@{client.public_ip}")
        if envoy_hosts:
            proxy = (
                f"ssh -i {key_path_str} -o StrictHostKeyChecking=no "
                f"-o IdentitiesOnly=yes -o ConnectTimeout=10 "
                f"ec2-user@{client.public_ip} -W %h:%p"
            )
            log(
                "  envoy-1: "
                f"ssh -i {key_path_str} -o ProxyCommand=\"{proxy}\" ec2-user@{envoy_hosts[0].private_ip}"
            )
        if valkey_hosts:
            proxy = (
                f"ssh -i {key_path_str} -o StrictHostKeyChecking=no "
                f"-o IdentitiesOnly=yes -o ConnectTimeout=10 "
                f"ec2-user@{client.public_ip} -W %h:%p"
            )
            log(
                "  valkey-1: "
                f"ssh -i {key_path_str} -o ProxyCommand=\"{proxy}\" ec2-user@{valkey_hosts[0].private_ip}"
            )

    proxy_ip = envoy_hosts[0].private_ip
    valkey_ip = valkey_hosts[0].private_ip
    manual_block = textwrap.dedent(f"""
    VALKEY_IP={valkey_ip};
    PROXY_IP={proxy_ip};

    NODE_IP=$PROXY_IP
    # NODE_IP=$VALKEY_IP

    # SET - Data population
    memtier_benchmark -s $NODE_IP -p 6379 --ratio=1:0 --print-percentiles 50,95,99 --clients=8 --threads=8 -d 1024 --key-maximum=10000000 --distinct-client-seed --test-time 120 --hide-histogram; valkey-cli -h $VALKEY_IP -p 6379 dbsize && valkey-cli -h $VALKEY_IP -p 6379 info memory | grep human;

    # SET:GET
    memtier_benchmark -s $NODE_IP -p 6379 --ratio=1:10 --print-percentiles 50,95,99 --clients=8 --threads=8 -d 1024 --key-maximum=10000000 --distinct-client-seed --test-time 120 --hide-histogram


      sudo mtr -n -c 20 -i 0.2 --report --report-wide {proxy_ip} --tcp --psize 1024

      to measure client→Envoy RTT, and:

      __SSH_CMD__

      to capture Envoy→Valkey RTT. Both mirror the validation flow exactly.

  ssh -i {key_path_str} -o ProxyCommand="ssh -i {key_path_str} -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -o ConnectTimeout=10 ec2-user@{client.public_ip} -W %h:%p" ec2-user@{proxy_ip} 'bash -lc "set -euo pipefail; sudo rm -f /tmp/perf-valkey.data /tmp/valkey-flamegraph.svg && sudo perf record -F 99 -a -g -o /tmp/perf-valkey.data -- sleep 30 && sudo perf script -i /tmp/perf-valkey.data | ~/FlameGraph/stackcollapse-perf.pl --kernel | ~/FlameGraph/flamegraph.pl > /tmp/valkey-flamegraph.svg"'

  scp -i {key_path_str} -o ProxyCommand="ssh -i {key_path_str} -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -o ConnectTimeout=10 ec2-user@{client.public_ip} -W %h:%p" ec2-user@{proxy_ip}:/tmp/valkey-flamegraph.svg ~/Downloads/valkey-flamegraph.svg
    """).strip("\n")
    ssh_cmd = "Client public IP unavailable; cannot build ssh ProxyCommand."
    if client.public_ip:
        ssh_cmd = (
            f"ssh -o ProxyCommand=\"ssh -i {key_path_str} "
            f"-o StrictHostKeyChecking=no -o IdentitiesOnly=yes -o ConnectTimeout=10 "
            f"ec2-user@{client.public_ip} -W %h:%p\" "
            f"-i {key_path_str} ec2-user@{proxy_ip} "
            f"'sudo mtr -n -c 20 -i 0.2 --report --report-wide {valkey_ip}'"
        )
    manual_block = manual_block.replace("__SSH_CMD__", ssh_cmd)
    log("=== Manual Test Commands ===")
    for line in manual_block.splitlines():
        log(line)

    cluster_info = cluster_summary(client, valkey_hosts, ctx)
    total_keys, key_stats = keyspace_stats(client, valkey_hosts, ctx)
    bench_output = run_readonly_benchmark(
        client,
        envoy_hosts,
        valkey_hosts,
        ctx,
        args.bench_operations,
        args.bench_concurrency,
        cluster_mode,
    )
    mtr_ce = measure_rtt(client, envoy_hosts[0], ctx, args.mtr_count, args.mtr_interval, "client->envoy")
    mtr_ev = measure_rtt(envoy_hosts[0], valkey_hosts[0], ctx, args.mtr_count, args.mtr_interval, "envoy->valkey")

    envoy_cost_rows, envoy_total = summarize_costs(envoy_hosts)
    valkey_cost_rows, valkey_total = summarize_costs(valkey_hosts)

    log("=== Cluster Overview ===")
    log(cluster_info)

    log("=== Keyspace Stats ===")
    for role, keys in key_stats:
        log(f"{role}: {keys} keys")
    log(f"Total keys across cluster: {total_keys}")

    log("=== Envoy GET sanity ===")
    envoy_check = "Skipped (no Envoy hosts)" if not envoy_hosts else run_readonly_benchmark(client, envoy_hosts, valkey_hosts, ctx, 1, 1, True)
    if "no upstream host" in envoy_check.lower():
        log("Envoy check failed: no upstream host")
    elif "Could not" in envoy_check:
        log(f"Envoy check failed: {envoy_check}")
    else:
        log("Envoy GET succeeded.")

    log("=== Read-only Benchmark ===")
    log(bench_output)

    log("=== RTT Latency (mtr) ===")
    log("Client -> Envoy:\n" + mtr_ce)
    log("Envoy -> Valkey:\n" + mtr_ev)

    log("=== Cost Summary (On-Demand Linux) ===")
    log("Envoy instances:")
    log(format_cost_rows(envoy_cost_rows))
    log(f"Envoy total: {fmt_monthly(envoy_total)}")
    log("Valkey instances:")
    log(format_cost_rows(valkey_cost_rows))
    log(f"Valkey total: {fmt_monthly(valkey_total)}")

    log("=== Validation Complete ===")
    if cluster_mode:
        log("Cluster mode detected; node map shown above.")
    else:
        log("Standalone Valkey node validated; server info shown above.")


if __name__ == "__main__":
    main()
