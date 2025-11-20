#!/usr/bin/env python3
import argparse
import boto3
import botocore
from botocore.config import Config
import ipaddress
import os
import json
import shutil
import subprocess
import tempfile
import textwrap
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

# -----------------------------
# Static config (per your specs)
# -----------------------------
REGION = "us-east-1"
AWS_PROFILE = os.environ.get("AWS_PROFILE", "sandbox")
SEED = "vlklt-001"
STACK = f"valkey-loadtest-{SEED}"
OWNER = os.environ.get("OWNER", "")
CLEANUP_LOG_PATH = Path("cleanup.log")
LOG_TO_FILE = None
BOTO_CONFIG = Config(
    retries={"max_attempts": 10, "mode": "adaptive"},
    connect_timeout=15,
    read_timeout=60,
)

# Instance types (ARM)
CLIENT_TYPE = "c8g.4xlarge"
ENVOY_TYPE = "c8g.2xlarge"
VALKEY_TYPE = "c8g.2xlarge"
def _int_env(var, default):
    value = os.environ.get(var, None)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise SystemExit(f"ERROR: {var} env var must be an integer.") from exc


DEFAULT_VALKEY_NODES = _int_env("VALKEY_NODES", 1)
DEFAULT_ENVOY_NODES = _int_env("ENVOY_NODES", 1)

# AMI resolution (Amazon Linux 2 aarch64)
AMI_OVERRIDE = os.environ.get("VALKEY_AMI_ID")
AMI_SSM_PARAM = os.environ.get(
    "VALKEY_AMI_PARAM",
    "/aws/service/ami-amazon-linux-latest/amzn2-ami-hvm-arm64-gp2",
)
_RESOLVED_AMI_ID = None

# Existing KeyPair required
KEY_NAME = "valkey-load-test-key"
DEFAULT_SSH_KEY_PATH = Path(__file__).resolve().with_name("valkey-load-test-key.pem")

# Amazon Linux 2 AMI resolved via SSM (override with VALKEY_AMI_ID when needed)

# CIDRs (tightened)
VPC_CIDR = "10.42.0.0/16"
PUB_CIDR = "10.42.1.0/24"
PRIV_CIDR = "10.42.2.0/24"

# Ports
REDIS_PORT = 6379
SSH_PORT = 22

VALKEY_VERSION = "9.0.0"
VALKEY_SRC_URL = f"https://github.com/valkey-io/valkey/archive/refs/tags/{VALKEY_VERSION}.tar.gz"
VALKEY_BIN_URL = os.environ.get(
    "VALKEY_BIN_URL",
    f"https://github.com/valkey-io/valkey/releases/download/valkey-{VALKEY_VERSION}/valkey-{VALKEY_VERSION}-linux-arm64.tar.gz",
)
VALKEY_DOCKER_IMAGE = os.environ.get("VALKEY_DOCKER_IMAGE", f"valkey/valkey:{VALKEY_VERSION}")
ENVOY_DOCKER_IMAGE = os.environ.get("ENVOY_DOCKER_IMAGE", "envoyproxy/envoy-debug:v1.31.0")
MEMTIER_VERSION = os.environ.get("MEMTIER_VERSION", "2.0.0")
MEMTIER_SRC_URL = os.environ.get(
    "MEMTIER_SRC_URL",
    f"https://github.com/RedisLabs/memtier_benchmark/archive/refs/tags/{MEMTIER_VERSION}.tar.gz",
)


def parse_args():
    parser = argparse.ArgumentParser(description="Provision Valkey load test stack and bootstrap cluster.")
    parser.add_argument("--region", default=REGION, help="AWS region (default: us-east-1)")
    parser.add_argument("--seed", default=SEED, help="Unique seed used in stack name.")
    parser.add_argument("--owner", default=os.environ.get("OWNER", ""), help="Owner tag value (optional).")
    parser.add_argument(
        "--ssh-private-key-path",
        dest="ssh_key_path",
        default=str(DEFAULT_SSH_KEY_PATH),
        help=f"Path to SSH private key (.pem) for ec2-user (default: {DEFAULT_SSH_KEY_PATH}).",
    )
    parser.add_argument("--ssh-cidr", help="CIDR allowed for SSH (default: detected public IP /32).")
    parser.add_argument("--aws-profile", help="AWS named profile to use (default: sandbox or $AWS_PROFILE).")
    parser.add_argument("--skip-bootstrap", action="store_true", help="Provision infrastructure only, skip remote bootstrap.")
    parser.add_argument("--cleanup", action="store_true", help="Tear down stack resources (keeps the S3 bucket).")
    parser.add_argument(
        "--valkey-nodes",
        type=int,
        default=DEFAULT_VALKEY_NODES,
        help="Number of Valkey nodes to provision (1 for standalone, >=3 for clustered mode).",
    )
    parser.add_argument(
        "--envoy-nodes",
        type=int,
        default=DEFAULT_ENVOY_NODES,
        help="Number of Envoy instances to run behind the load balancer (default: 1).",
    )
    parser.add_argument(
        "--force-cluster",
        action="store_true",
        help="Force cluster-mode configuration even when fewer than 3 Valkey nodes are provisioned.",
    )
    return parser


def configure_runtime(*, region=None, seed=None, owner=None, aws_profile=None):
    global REGION, SEED, STACK, AWS_PROFILE, OWNER
    if region:
        REGION = region
    if seed:
        SEED = seed
    if owner is not None:
        OWNER = owner
    if aws_profile:
        AWS_PROFILE = aws_profile
    STACK = f"valkey-loadtest-{SEED}"


def configure_from_args(args):
    configure_runtime(
        region=args.region,
        seed=args.seed,
        owner=args.owner if args.owner != "" else OWNER,
        aws_profile=args.aws_profile,
    )

# -------------
# Helpers/logging
# -------------
def ts():
    return datetime.now().strftime("[%Y-%m-%dT%H:%M:%S%z]")

def log(msg):
    global LOG_TO_FILE
    line = f"{ts()} {msg}"
    print(line, flush=True)
    if LOG_TO_FILE:
        try:
            LOG_TO_FILE.parent.mkdir(parents=True, exist_ok=True)
            with LOG_TO_FILE.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        except OSError:
            pass


def need_cmd(cmd):
    if shutil.which(cmd):
        return
    raise SystemExit(f"ERROR: Required command '{cmd}' not found in PATH.")

def my_public_cidr():
    ip = urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10).read().decode().strip()
    # ensure format
    ipaddress.ip_address(ip)
    return f"{ip}/32"

def aws_session():
    try:
        return boto3.session.Session(profile_name=AWS_PROFILE, region_name=REGION)
    except botocore.exceptions.ProfileNotFound:
        raise SystemExit(f"ERROR: AWS profile '{AWS_PROFILE}' not found. Configure it with aws configure or export AWS_PROFILE.")

def ec2():
    return aws_session().client("ec2", region_name=REGION, config=BOTO_CONFIG)

def elbv2():
    return aws_session().client("elbv2", region_name=REGION, config=BOTO_CONFIG)


def ssm():
    return aws_session().client("ssm", region_name=REGION, config=BOTO_CONFIG)


def resolved_ami_id():
    global _RESOLVED_AMI_ID
    if AMI_OVERRIDE:
        return AMI_OVERRIDE
    if _RESOLVED_AMI_ID:
        return _RESOLVED_AMI_ID
    try:
        param = ssm().get_parameter(Name=AMI_SSM_PARAM)
        value = param["Parameter"]["Value"]
        _RESOLVED_AMI_ID = value
        log(f"Using Amazon Linux 2 AMI via {AMI_SSM_PARAM}: {value}")
        return value
    except botocore.exceptions.ClientError as exc:
        msg = exc.response.get("Error", {}).get("Message", str(exc))
        raise SystemExit(
            "ERROR: Unable to resolve Amazon Linux 2 AMI from SSM "
            f"({AMI_SSM_PARAM}): {msg}. Set VALKEY_AMI_ID to override."
        )


@dataclass
class InstanceInfo:
    role: str
    instance_id: str
    public_ip: str
    private_ip: str


@dataclass
class BootstrapContext:
    ssh_key_path: Path
    self_cidr: str
    client: InstanceInfo


def describe_instance(iid):
    resp = ec2().describe_instances(InstanceIds=[iid])
    for res in resp.get("Reservations", []):
        for inst in res.get("Instances", []):
            return inst
    raise RuntimeError(f"Instance {iid} not found.")


def gather_instance_info(role_to_id):
    info = {}
    for role, iid in role_to_id.items():
        inst = describe_instance(iid)
        info[role] = InstanceInfo(
            role=role,
            instance_id=iid,
            public_ip=inst.get("PublicIpAddress"),
            private_ip=inst.get("PrivateIpAddress"),
        )
    return info


def host_target_and_jump(host: InstanceInfo, ctx: BootstrapContext):
    target = host.public_ip or host.private_ip
    if not target:
        raise RuntimeError(f"Instance {host.role} has no reachable IP.")
    jump = None
    if host.role != ctx.client.role and not host.public_ip:
        if not ctx.client.public_ip:
            raise RuntimeError("Client instance missing public IP for ProxyJump.")
        jump = ctx.client.public_ip
    return target, jump


def ssh_base_cmd(host: InstanceInfo, ctx: BootstrapContext):
    target, jump = host_target_and_jump(host, ctx)
    cmd = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "ConnectTimeout=10",
        "-i",
        str(ctx.ssh_key_path),
    ]
    if jump:
        proxy = (
            "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "
            f"-o IdentitiesOnly=yes -i {ctx.ssh_key_path} ec2-user@{jump} -W %h:%p"
        )
        cmd += ["-o", f"ProxyCommand={proxy}"]
    cmd += [f"ec2-user@{target}", "bash", "-s"]
    return cmd


def ssh_run(host: InstanceInfo, script: str, ctx: BootstrapContext, strict=True):
    full_script = textwrap.dedent(script).lstrip()
    if strict:
        full_script = "set -euo pipefail\n" + full_script
    cmd = ssh_base_cmd(host, ctx)
    subprocess.run(cmd, input=full_script, text=True, check=True)


def ssh_capture(host: InstanceInfo, script: str, ctx: BootstrapContext, strict=True):
    full_script = textwrap.dedent(script).lstrip()
    if strict:
        full_script = "set -euo pipefail\n" + full_script
    cmd = ssh_base_cmd(host, ctx)
    result = subprocess.run(cmd, input=full_script, text=True, capture_output=True)
    if strict:
        result.check_returncode()
    return result


def scp_put(host: InstanceInfo, local_path: Path, remote_path: str, ctx: BootstrapContext):
    target, jump = host_target_and_jump(host, ctx)
    cmd = [
        "scp",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "BatchMode=yes",
        "-o",
        "IdentitiesOnly=yes",
        "-i",
        str(ctx.ssh_key_path),
    ]
    if jump:
        proxy = (
            "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "
            f"-o IdentitiesOnly=yes -i {ctx.ssh_key_path} ec2-user@{jump} -W %h:%p"
        )
        cmd += ["-o", f"ProxyCommand={proxy}"]
    cmd += [str(local_path), f"ec2-user@{target}:{remote_path}"]
    subprocess.run(cmd, check=True)


def scp_get(host: InstanceInfo, remote_path: str, local_path: Path, ctx: BootstrapContext):
    target, jump = host_target_and_jump(host, ctx)
    cmd = [
        "scp",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "BatchMode=yes",
        "-o",
        "IdentitiesOnly=yes",
        "-i",
        str(ctx.ssh_key_path),
    ]
    if jump:
        proxy = (
            "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "
            f"-o IdentitiesOnly=yes -i {ctx.ssh_key_path} ec2-user@{jump} -W %h:%p"
        )
        cmd += ["-o", f"ProxyCommand={proxy}"]
    cmd += [f"ec2-user@{target}:{remote_path}", str(local_path)]
    subprocess.run(cmd, check=True)

# -------------
# Tag helpers
# -------------
def tags_common():
    tags = [{"Key": "Project", "Value": STACK}]
    if OWNER:
        tags.append({"Key": "Owner", "Value": OWNER})
    return tags

def ensure_tags(resource_ids, extra):
    if not resource_ids:
        return
    ec2().create_tags(Resources=resource_ids, Tags=tags_common() + extra)

def find_by_tag(describe_fn, key, value, extract):
    # describe_fn: callable that returns a list of dicts
    items = describe_fn()
    for it in items:
        t = {t["Key"]: t["Value"] for t in it.get("Tags", [])}
        if t.get(key) == value and t.get("Project") == STACK:
            return extract(it)
    return None

# -------------
# VPC & Network
# -------------
def get_vpcs():
    resp = ec2().describe_vpcs(
        Filters=[{"Name": "tag:Project", "Values": [STACK]}]
    )
    return resp.get("Vpcs", [])

def ensure_vpc():
    v = get_vpcs()
    if v:
        vpc_id = v[0]["VpcId"]
        log(f"REUSED  vpc: {vpc_id}")
        return vpc_id
    # Create new VPC (do not reuse by CIDR, only by tags)
    resp = ec2().create_vpc(CidrBlock=VPC_CIDR, TagSpecifications=[{
        "ResourceType": "vpc",
        "Tags": tags_common() + [{"Key": "Name", "Value": f"{STACK}-vpc"}]
    }])
    vpc_id = resp["Vpc"]["VpcId"]
    log(f"CREATED vpc: {vpc_id}")
    # Enable attributes silently (not strictly required)
    try:
        ec2().modify_vpc_attribute(VpcId=vpc_id, EnableDnsSupport={"Value": True})
        ec2().modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={"Value": True})
    except botocore.exceptions.ClientError:
        pass
    return vpc_id

def get_igw_for_vpc(vpc_id):
    resp = ec2().describe_internet_gateways(
        Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}]
    )
    igws = resp.get("InternetGateways", [])
    return igws[0]["InternetGatewayId"] if igws else None

def ensure_igw(vpc_id):
    igw = get_igw_for_vpc(vpc_id)
    if igw:
        log(f"REUSED  igw (attached): {igw}")
        return igw
    resp = ec2().create_internet_gateway(TagSpecifications=[{
        "ResourceType": "internet-gateway",
        "Tags": tags_common() + [{"Key": "Name", "Value": f"{STACK}-igw"}]
    }])
    igw_id = resp["InternetGateway"]["InternetGatewayId"]
    ec2().attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
    log(f"CREATED igw: {igw_id}")
    return igw_id

def name_variants(base):
    return {base, f"{base}-a", f"{base}-b"}

def find_subnet(vpc_id, name, cidr):
    candidates = name_variants(name)
    resp = ec2().describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )
    subnets = resp.get("Subnets", [])
    def tag_dict(sn):
        return {t["Key"]: t["Value"] for t in sn.get("Tags", [])}
    # Prefer exact tag matches
    for sn in subnets:
        tags = tag_dict(sn)
        if tags.get("Project") == STACK and tags.get("Name") in candidates:
            return sn["SubnetId"], tags
    # Then match by Name tag only
    for sn in subnets:
        tags = tag_dict(sn)
        if tags.get("Name") in candidates:
            return sn["SubnetId"], tags
    # Finally match by CIDR block
    for sn in subnets:
        if sn.get("CidrBlock") == cidr:
            return sn["SubnetId"], tag_dict(sn)
    return None, None

def ensure_subnet(vpc_id, name, cidr, public=False):
    sn, tags = find_subnet(vpc_id, name, cidr)
    if sn:
        kv_updates = []
        project_missing = not tags or tags.get("Project") != STACK
        if not tags or tags.get("Name") not in name_variants(name):
            kv_updates.append({"Key": "Name", "Value": name})
        if kv_updates or project_missing:
            ensure_tags([sn], kv_updates)
        log(f"REUSED  subnet {('public' if public else 'private')}: {sn}")
        return sn
    resp = ec2().create_subnet(
        VpcId=vpc_id,
        CidrBlock=cidr,
        AvailabilityZone=f"{REGION}a",
        TagSpecifications=[{
            "ResourceType": "subnet",
            "Tags": tags_common() + [{"Key": "Name", "Value": name}]
        }],
    )
    subnet_id = resp["Subnet"]["SubnetId"]
    if public:
        ec2().modify_subnet_attribute(SubnetId=subnet_id, MapPublicIpOnLaunch={"Value": True})
    log(f"CREATED subnet {('public' if public else 'private')}: {subnet_id}")
    return subnet_id

def get_eip():
    # Find tagged EIP (address allocation id) for NAT
    resp = ec2().describe_addresses(
        Filters=[{"Name": "tag:Project", "Values": [STACK]}]
    )
    addrs = resp.get("Addresses", [])
    return addrs[0].get("AllocationId") if addrs else None

def ensure_eip():
    eipalloc = get_eip()
    if eipalloc:
        log(f"REUSED  eip: {eipalloc}")
        return eipalloc
    resp = ec2().allocate_address(Domain="vpc", TagSpecifications=[{
        "ResourceType": "elastic-ip",
        "Tags": tags_common() + [{"Key": "Name", "Value": f"{STACK}-nat-eip"}]
    }])
    eipalloc = resp["AllocationId"]
    log(f"CREATED eip: {eipalloc}")
    return eipalloc

def get_nat():
    while True:
        try:
            resp = ec2().describe_nat_gateways(
                Filters=[{"Name": "tag:Project", "Values": [STACK]}]
            )
            break
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "RequestExpired":
                log("AWS request expired while describing NAT gateways; refreshing session and retrying...")
                continue
            raise
    ngws = resp.get("NatGateways", [])
    if not ngws:
        return None, None
    gw = ngws[0]
    return gw["NatGatewayId"], gw["State"]

def ensure_nat(public_subnet_id, eipalloc):
    nat_id, state = get_nat()
    if nat_id:
        log(f"REUSED  natgw: {nat_id} (state={state})")
        if state == "failed":
            raise RuntimeError(f"NAT Gateway {nat_id} state=failed. Fix quotas/EIP and delete it before retry.")
        return nat_id
    resp = ec2().create_nat_gateway(
        SubnetId=public_subnet_id,
        AllocationId=eipalloc,
        TagSpecifications=[{
            "ResourceType": "natgateway",
            "Tags": tags_common() + [{"Key": "Name", "Value": f"{STACK}-nat"}]
        }]
    )
    nat_id = resp["NatGateway"]["NatGatewayId"]
    log(f"CREATED natgw: {nat_id}")
    # Wait
    waiter = ec2().get_waiter("nat_gateway_available")
    waiter.wait(NatGatewayIds=[nat_id])
    return nat_id

def find_rtb_by_name(name):
    resp = ec2().describe_route_tables(
        Filters=[{"Name": "tag:Name", "Values": [name]}, {"Name": "tag:Project", "Values": [STACK]}]
    )
    rtbs = resp.get("RouteTables", [])
    return rtbs[0]["RouteTableId"] if rtbs else None

def ensure_public_rtb(vpc_id, igw_id, public_subnet_id):
    name = f"{STACK}-rtb-public"
    rtb = find_rtb_by_name(name)
    if not rtb:
        resp = ec2().create_route_table(
            VpcId=vpc_id,
            TagSpecifications=[{
                "ResourceType": "route-table",
                "Tags": tags_common() + [{"Key": "Name", "Value": name}]
            }]
        )
        rtb = resp["RouteTable"]["RouteTableId"]
        ec2().create_route(RouteTableId=rtb, DestinationCidrBlock="0.0.0.0/0", GatewayId=igw_id)
        log(f"CREATED rtb public for {public_subnet_id}: {rtb}")
    else:
        log(f"REUSED  rtb public: {rtb}")
    # Associate
    assoc = ec2().associate_route_table(RouteTableId=rtb, SubnetId=public_subnet_id)
    return rtb

def ensure_private_rtb(vpc_id, nat_id, private_subnet_id):
    name = f"{STACK}-rtb-private"
    rtb = find_rtb_by_name(name)
    if not rtb:
        resp = ec2().create_route_table(
            VpcId=vpc_id,
            TagSpecifications=[{
                "ResourceType": "route-table",
                "Tags": tags_common() + [{"Key": "Name", "Value": name}]
            }]
        )
        rtb = resp["RouteTable"]["RouteTableId"]
        ec2().create_route(RouteTableId=rtb, DestinationCidrBlock="0.0.0.0/0", NatGatewayId=nat_id)
        log(f"CREATED rtb private for {private_subnet_id}: {rtb}")
    else:
        log(f"REUSED  rtb private: {rtb}")
    ec2().associate_route_table(RouteTableId=rtb, SubnetId=private_subnet_id)
    return rtb


def envoy_tg_name():
    norm_name = f"{STACK}-envoy-tg".replace("_", "-")
    safe_name = norm_name[:32].strip("-")
    if not safe_name:
        safe_name = (norm_name.replace("-", "") or f"{STACK}-tg")[:32]
    return safe_name

# -------------
# Security Groups
# -------------
def ensure_sg(vpc_id, name, description):
    resp = ec2().describe_security_groups(
        Filters=[{"Name":"group-name","Values":[name]},
                 {"Name":"vpc-id","Values":[vpc_id]}]
    )
    sgs = resp.get("SecurityGroups", [])
    if sgs:
        sg_id = sgs[0]["GroupId"]
        log(f"REUSED  sg {name}: {sg_id}")
    else:
        # Ensure description is <256 and uses allowed chars
        description = description[:200]
        resp = ec2().create_security_group(
            GroupName=name, Description=description, VpcId=vpc_id,
            TagSpecifications=[{
                "ResourceType":"security-group",
                "Tags": tags_common() + [{"Key":"Name","Value":name}]
            }]
        )
        sg_id = resp["GroupId"]
        log(f"CREATED sg {name}: {sg_id}")
    return sg_id

def cidr_authorized(sg, from_port, to_port, cidr):
    resp = ec2().describe_security_groups(GroupIds=[sg])
    perms = resp["SecurityGroups"][0].get("IpPermissions", [])
    for p in perms:
        if p.get("FromPort")==from_port and p.get("ToPort")==to_port and p.get("IpProtocol")=="tcp":
            for r in p.get("IpRanges", []):
                if r.get("CidrIp")==cidr:
                    return True
    return False

def ensure_ingress_tcp_cidr(sg, port, cidr):
    if cidr_authorized(sg, port, port, cidr):
        return False
    ec2().authorize_security_group_ingress(
        GroupId=sg, IpPermissions=[{
            "IpProtocol":"tcp", "FromPort":port, "ToPort":port,
            "IpRanges":[{"CidrIp":cidr}]
        }]
    )
    return True

def ensure_ingress_all_between(sg_from, sg_to):
    # allow ALL protocols/ports between two SGs both directions
    def allow(src, dst):
        ec2().authorize_security_group_ingress(
            GroupId=dst,
            IpPermissions=[{
                "IpProtocol":"-1",
                "UserIdGroupPairs":[{"GroupId":src}]
            }]
        )
    # check then allow (errors ignored if already present)
    for (src, dst) in [(sg_from, sg_to),(sg_to, sg_from),(sg_from, sg_from),(sg_to, sg_to)]:
        try:
            allow(src, dst)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] not in ("InvalidPermission.Duplicate","InvalidGroup.NotFound"):
                raise


def ensure_sg_ingress_from_group(dst_sg, src_sg, protocol="tcp", from_port=None, to_port=None):
    perm = {
        "IpProtocol": protocol,
        "UserIdGroupPairs": [{"GroupId": src_sg}]
    }
    if protocol != "-1":
        perm["FromPort"] = from_port
        perm["ToPort"] = to_port
    try:
        ec2().authorize_security_group_ingress(
            GroupId=dst_sg,
            IpPermissions=[perm]
        )
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "InvalidPermission.Duplicate":
            raise


def refresh_ssh_rule(sg_id, cidr):
    if not cidr:
        return
    resp = ec2().describe_security_groups(GroupIds=[sg_id])
    perms = resp["SecurityGroups"][0].get("IpPermissions", [])
    need_authorize = True
    to_revoke = []
    for p in perms:
        if p.get("IpProtocol") == "tcp" and p.get("FromPort") == SSH_PORT and p.get("ToPort") == SSH_PORT:
            for r in p.get("IpRanges", []):
                ip = r.get("CidrIp")
                if ip == cidr:
                    need_authorize = False
                else:
                    to_revoke.append(ip)
    if to_revoke:
        revoke_ranges = [{"CidrIp": ip} for ip in to_revoke]
        try:
            ec2().revoke_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[{
                    "IpProtocol": "tcp",
                    "FromPort": SSH_PORT,
                    "ToPort": SSH_PORT,
                    "IpRanges": revoke_ranges
                }]
            )
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "InvalidPermission.NotFound":
                raise
    if need_authorize:
        try:
            ec2().authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[{
                    "IpProtocol": "tcp",
                    "FromPort": SSH_PORT,
                    "ToPort": SSH_PORT,
                    "IpRanges": [{"CidrIp": cidr}]
                }]
            )
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "InvalidPermission.Duplicate":
                raise

# -------------
# Helper lookups
# -------------
def lookup_security_group_id(name):
    resp = ec2().describe_security_groups(
        Filters=[
            {"Name": "group-name", "Values": [name]},
            {"Name": "tag:Project", "Values": [STACK]},
        ]
    )
    sgs = resp.get("SecurityGroups", [])
    if not sgs:
        raise SystemExit(f"ERROR: Security group '{name}' not found. Run valkey_setup.py first.")
    return sgs[0]["GroupId"]


# -------------
# EC2 Instances
# -------------
def find_instance_id_by_name(name):
    resp = ec2().describe_instances(
        Filters=[{"Name":"tag:Name","Values":[name]},
                 {"Name":"tag:Project","Values":[STACK]},
                 {"Name":"instance-state-name","Values":["pending","running","stopping","stopped"]}]
    )
    for res in resp.get("Reservations", []):
        for inst in res.get("Instances", []):
            return inst["InstanceId"]
    return None

def wait_running(iid):
    ec2().get_waiter("instance_running").wait(InstanceIds=[iid])

def instance_private_ip(iid):
    r = ec2().describe_instances(InstanceIds=[iid])
    return r["Reservations"][0]["Instances"][0]["PrivateIpAddress"]

def instance_public_ip(iid):
    r = ec2().describe_instances(InstanceIds=[iid])
    return r["Reservations"][0]["Instances"][0].get("PublicIpAddress")

def ensure_instance(name, role, itype, subnet_id, sg_id, associate_public_ip):
    iid = find_instance_id_by_name(name)
    if iid:
        log(f"REUSED  instance {role}: {iid}")
        return iid
    # Verify KeyPair exists (and credentials are still valid)
    ensure_keypair_accessible()
    # Launch
    ni = [{
        "DeviceIndex": 0,
        "SubnetId": subnet_id,
        "AssociatePublicIpAddress": associate_public_ip,
        "Groups": [sg_id]
    }]
    resp = ec2().run_instances(
        ImageId=resolved_ami_id(),
        InstanceType=itype,
        KeyName=KEY_NAME,
        NetworkInterfaces=ni,
        TagSpecifications=[{
            "ResourceType":"instance",
            "Tags": tags_common() + [
                {"Key":"Name","Value":name},
                {"Key":"Role","Value":role}
            ]
        }],
        MinCount=1, MaxCount=1
    )
    iid = resp["Instances"][0]["InstanceId"]
    log(f"CREATED instance {role}: {iid}")
    wait_running(iid)
    return iid


def get_stack_instance_ids(roles=None):
    roles = roles or ["client", "envoy-1", "envoy-2", "valkey-1", "valkey-2", "valkey-3"]
    ids = {}
    for role in roles:
        name = f"{STACK}-{role}"
        iid = find_instance_id_by_name(name)
        if not iid:
            raise SystemExit(f"ERROR: Instance '{name}' not found. Run valkey_setup.py first.")
        ids[role] = iid
    return ids


# -------------
# NLB (internal)
# -------------
def ensure_tg(vpc_id, port):
    safe_name = envoy_tg_name()
    tgs = []
    try:
        resp = elbv2().describe_target_groups(Names=[safe_name])
        tgs = resp.get("TargetGroups", [])
    except botocore.exceptions.ClientError as e:
        # TargetGroupNotFound is expected on first run
        if e.response["Error"]["Code"] != "TargetGroupNotFound":
            raise
    if tgs:
        tg_info = tgs[0]
        if tg_info.get("VpcId") == vpc_id:
            arn = tg_info["TargetGroupArn"]
            log(f"REUSED  target-group: {safe_name}")
            return arn
        # Existing TG is tied to another VPC; delete and recreate
        log(f"DELETING target-group {safe_name} bound to mismatched VPC {tg_info.get('VpcId')}")
        elbv2().delete_target_group(TargetGroupArn=tg_info["TargetGroupArn"])
        time.sleep(5)
    try:
        tg = elbv2().create_target_group(
            Name=safe_name,
            Protocol="TCP",
            Port=port,
            VpcId=vpc_id,
            TargetType="instance",
            HealthCheckProtocol="TCP",
            HealthCheckPort=str(port)
        )
        arn = tg["TargetGroups"][0]["TargetGroupArn"]
        log(f"CREATED target-group: {safe_name}")
        return arn
    except botocore.exceptions.ClientError:
        # surface any unexpected API error to the caller
        raise

def ensure_nlb(name, subnet_id):
    # Try find by name/tag
    resp = elbv2().describe_load_balancers()
    for lb in resp.get("LoadBalancers", []):
        arn = lb["LoadBalancerArn"]
        # fetch tags
        tags = elbv2().describe_tags(ResourceArns=[arn])["TagDescriptions"][0]["Tags"]
        t = {t["Key"]: t["Value"] for t in tags}
        if t.get("Project")==STACK and t.get("Name")==name:
            log(f"REUSED  nlb: {arn}")
            return arn
    # Create internal NLB in private subnet
    lb = elbv2().create_load_balancer(
        Name=name[:32].strip("-"),
        Type="network",
        Scheme="internal",
        Subnets=[subnet_id],
        Tags=tags_common() + [{"Key":"Name","Value":name}]
    )
    arn = lb["LoadBalancers"][0]["LoadBalancerArn"]
    log(f"CREATED nlb: {arn}")
    return arn


def get_nlb_dns(lb_arn):
    resp = elbv2().describe_load_balancers(LoadBalancerArns=[lb_arn])
    lbs = resp.get("LoadBalancers", [])
    if not lbs:
        return None
    return lbs[0].get("DNSName")

def ensure_listener(lb_arn, tg_arn, port):
    # try to find existing
    ls = elbv2().describe_listeners(LoadBalancerArn=lb_arn).get("Listeners", [])
    for l in ls:
        for a in l.get("DefaultActions", []):
            if a.get("TargetGroupArn")==tg_arn and l.get("Port")==port:
                log(f"REUSED  listener:{port}")
                return l["ListenerArn"]
    l = elbv2().create_listener(
        LoadBalancerArn=lb_arn,
        Protocol="TCP",
        Port=port,
        DefaultActions=[{"Type":"forward","TargetGroupArn":tg_arn}]
    )
    log(f"CREATED listener:{port}")
    return l["Listeners"][0]["ListenerArn"]

def register_targets(tg_arn, instance_ids, port):
    tg = elbv2().describe_target_groups(TargetGroupArns=[tg_arn])["TargetGroups"][0]
    target_type = tg.get("TargetType", "instance")
    if target_type == "instance":
        targets = [{"Id":iid, "Port":port} for iid in instance_ids]
    elif target_type == "ip":
        targets = [{"Id":instance_private_ip(iid), "Port":port} for iid in instance_ids]
    else:
        raise RuntimeError(f"Unsupported target group type '{target_type}' for {tg_arn}")
    elbv2().register_targets(TargetGroupArn=tg_arn, Targets=targets)
    log(f"REGISTER  targets -> TG")


def ensure_bucket_with_lifecycle(bucket):
    s3 = aws_session().client("s3", region_name=REGION, config=BOTO_CONFIG)
    try:
        s3.head_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchBucket", "NotFound"):
            params = {"Bucket": bucket}
            if REGION != "us-east-1":
                params["CreateBucketConfiguration"] = {"LocationConstraint": REGION}
            s3.create_bucket(**params)
        else:
            raise
    lifecycle = {
        "Rules": [
            {
                "ID": "expire-profiles-7d",
                "Status": "Enabled",
                "Filter": {"Prefix": "profiles/"},
                "Expiration": {"Days": 7}
            },
            {
                "ID": "expire-benchmarks-7d",
                "Status": "Enabled",
                "Filter": {"Prefix": "benchmarks/"},
                "Expiration": {"Days": 7}
            },
        ]
    }
    try:
        s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=lifecycle)
    except botocore.exceptions.ClientError:
        pass
    return s3


def upload_file_to_s3(local_path: Path, bucket: str, key: str):
    s3 = aws_session().client("s3", region_name=REGION, config=BOTO_CONFIG)
    s3.upload_file(str(local_path), bucket, key)
    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=604800
    )
    return url


# -------------
# Cleanup helpers
# -------------
def terminate_stack_instances():
    roles = ["client", "envoy-1", "envoy-2", "valkey-1", "valkey-2", "valkey-3"]
    to_terminate = []
    for role in roles:
        name = f"{STACK}-{role}"
        iid = find_instance_id_by_name(name)
        if iid:
            log(f"TERMINATING instance {role}: {iid}")
            try:
                ec2().terminate_instances(InstanceIds=[iid])
                to_terminate.append(iid)
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] != "InvalidInstanceID.NotFound":
                    raise
    if to_terminate:
        log("Waiting for instances to terminate...")
        waiter = ec2().get_waiter("instance_terminated")
        try:
            waiter.wait(InstanceIds=to_terminate)
        except botocore.exceptions.WaiterError:
            log("Warning: Timeout while waiting for instances to terminate; continuing.")


def delete_stack_load_balancer():
    lb_name = f"{STACK}-nlb"
    lbs = elbv2().describe_load_balancers().get("LoadBalancers", [])
    for lb in lbs:
        arn = lb["LoadBalancerArn"]
        tags = elbv2().describe_tags(ResourceArns=[arn])["TagDescriptions"][0]["Tags"]
        tag_map = {t["Key"]: t["Value"] for t in tags}
        if tag_map.get("Project") == STACK and tag_map.get("Name") == lb_name:
            log(f"DELETING load balancer: {lb_name}")
            elbv2().delete_load_balancer(LoadBalancerArn=arn)
            for _ in range(30):
                try:
                    elbv2().describe_load_balancers(LoadBalancerArns=[arn])
                    time.sleep(5)
                except botocore.exceptions.ClientError as e:
                    if e.response["Error"]["Code"] == "LoadBalancerNotFound":
                        break
                    raise
            break


def delete_stack_target_group():
    name = envoy_tg_name()
    try:
        resp = elbv2().describe_target_groups(Names=[name])
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "TargetGroupNotFound":
            return
        raise
    if resp.get("TargetGroups"):
        arn = resp["TargetGroups"][0]["TargetGroupArn"]
        log(f"DELETING target group: {name}")
        try:
            elbv2().delete_target_group(TargetGroupArn=arn)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "ResourceInUse":
                raise


def revoke_all_permissions(sg_id):
    desc = ec2().describe_security_groups(GroupIds=[sg_id])["SecurityGroups"][0]
    ingress = desc.get("IpPermissions", [])
    if ingress:
        try:
            ec2().revoke_security_group_ingress(GroupId=sg_id, IpPermissions=ingress)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "InvalidPermission.NotFound":
                raise
    egress = desc.get("IpPermissionsEgress", [])
    if egress:
        try:
            ec2().revoke_security_group_egress(GroupId=sg_id, IpPermissions=egress)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "InvalidPermission.NotFound":
                raise


def remove_group_references(group_ids):
    if not group_ids:
        return
    resp = ec2().describe_security_groups()
    for sg in resp.get("SecurityGroups", []):
        sg_id = sg["GroupId"]
        ingress_revoke = []
        for perm in sg.get("IpPermissions", []):
            matching = [pair for pair in perm.get("UserIdGroupPairs", []) if pair["GroupId"] in group_ids]
            if matching:
                entry = {
                    "IpProtocol": perm.get("IpProtocol"),
                    "UserIdGroupPairs": [{"GroupId": pair["GroupId"]} for pair in matching],
                }
                if "FromPort" in perm:
                    entry["FromPort"] = perm["FromPort"]
                if "ToPort" in perm:
                    entry["ToPort"] = perm["ToPort"]
                ingress_revoke.append(entry)
        if ingress_revoke:
            log(f"REVOKING ingress references from {sg_id}")
            try:
                ec2().revoke_security_group_ingress(GroupId=sg_id, IpPermissions=ingress_revoke)
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] not in ("InvalidPermission.NotFound", "InvalidGroup.NotFound"):
                    raise
        egress_revoke = []
        for perm in sg.get("IpPermissionsEgress", []):
            matching = [pair for pair in perm.get("UserIdGroupPairs", []) if pair["GroupId"] in group_ids]
            if matching:
                entry = {
                    "IpProtocol": perm.get("IpProtocol"),
                    "UserIdGroupPairs": [{"GroupId": pair["GroupId"]} for pair in matching],
                }
                if "FromPort" in perm:
                    entry["FromPort"] = perm["FromPort"]
                if "ToPort" in perm:
                    entry["ToPort"] = perm["ToPort"]
                egress_revoke.append(entry)
        if egress_revoke:
            log(f"REVOKING egress references from {sg_id}")
            try:
                ec2().revoke_security_group_egress(GroupId=sg_id, IpPermissions=egress_revoke)
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] not in ("InvalidPermission.NotFound", "InvalidGroup.NotFound"):
                    raise


def delete_stack_security_groups():
    resp = ec2().describe_security_groups(
        Filters=[{"Name": "tag:Project", "Values": [STACK]}]
    )
    groups = [sg for sg in resp.get("SecurityGroups", []) if sg.get("GroupName") != "default"]
    if not groups:
        return
    target_ids = [sg["GroupId"] for sg in groups]
    remove_group_references(target_ids)
    for sg in groups:
        sg_id = sg["GroupId"]
        name = sg.get("GroupName", sg_id)
        revoke_all_permissions(sg_id)
        try:
            log(f"DELETING security group {name}: {sg_id}")
            ec2().delete_security_group(GroupId=sg_id)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] not in ("InvalidGroup.NotFound", "DependencyViolation"):
                raise


def delete_route_table_by_name(name):
    rtb = find_rtb_by_name(name)
    if not rtb:
        return
    log(f"DELETING route table {name}: {rtb}")
    desc = ec2().describe_route_tables(RouteTableIds=[rtb])["RouteTables"][0]
    for assoc in desc.get("Associations", []):
        if not assoc.get("Main"):
            assoc_id = assoc.get("RouteTableAssociationId")
            try:
                ec2().disassociate_route_table(AssociationId=assoc_id)
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] != "InvalidAssociationID.NotFound":
                    raise
    try:
        ec2().delete_route_table(RouteTableId=rtb)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "InvalidRouteTableID.NotFound":
            raise


def delete_stack_route_tables():
    delete_route_table_by_name(f"{STACK}-rtb-public")
    delete_route_table_by_name(f"{STACK}-rtb-private")


def delete_stack_network_interfaces(vpc_id):
    nis = ec2().describe_network_interfaces(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    ).get("NetworkInterfaces", [])
    for eni in nis:
        eni_id = eni["NetworkInterfaceId"]
        status = eni.get("Status")
        attachment = eni.get("Attachment")
        if attachment and attachment.get("AttachmentId"):
            att_id = attachment["AttachmentId"]
            log(f"DETACHING network interface {eni_id} ({status})")
            try:
                ec2().detach_network_interface(AttachmentId=att_id, Force=True)
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] not in ("InvalidAttachmentID.NotFound", "OperationNotPermitted"):
                    raise
        log(f"DELETING network interface: {eni_id}")
        for attempt in range(5):
            try:
                ec2().delete_network_interface(NetworkInterfaceId=eni_id)
                break
            except botocore.exceptions.ClientError as e:
                code = e.response["Error"]["Code"]
                if code in ("InvalidNetworkInterfaceID.NotFound",):
                    break
                if code in ("InvalidParameterValue", "IncorrectState") and attempt < 4:
                    time.sleep(3)
                    continue
                raise


def release_stack_eip():
    eipalloc = get_eip()
    if eipalloc:
        log(f"RELEASING EIP allocation: {eipalloc}")
        try:
            ec2().release_address(AllocationId=eipalloc)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "InvalidAllocationID.NotFound":
                raise


def wait_for_nat_deletion():
    for attempt in range(60):
        remaining, _ = get_nat()
        if remaining is None:
            return True
        log("NAT gateway still deleting; waiting 10s...")
        time.sleep(10)
    log("Warning: NAT gateway still present after waiting.")
    return False


def delete_stack_nat_and_eip(wait_for_completion=False):
    nat_id, _state = get_nat()
    pending_nat = False
    if nat_id:
        log(f"DELETING NAT gateway: {nat_id}")
        try:
            ec2().delete_nat_gateway(NatGatewayId=nat_id)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "NatGatewayNotFound":
                raise
        if wait_for_completion:
            wait_for_nat_deletion()
        else:
            pending_nat = True
            log("NAT deletion initiated; will verify at the end of cleanup.")
    if not pending_nat:
        release_stack_eip()
    return pending_nat


def delete_stack_subnets(vpc_id):
    resp = ec2().describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}, {"Name": "tag:Project", "Values": [STACK]}]
    )
    for subnet in resp.get("Subnets", []):
        subnet_id = subnet["SubnetId"]
        log(f"DELETING subnet: {subnet_id}")
        try:
            ec2().delete_subnet(SubnetId=subnet_id)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] not in ("InvalidSubnetID.NotFound", "DependencyViolation"):
                raise


def delete_stack_igw_and_vpc(vpc_id):
    igw = get_igw_for_vpc(vpc_id)
    if igw:
        log(f"DETACHING internet gateway: {igw}")
        try:
            ec2().detach_internet_gateway(InternetGatewayId=igw, VpcId=vpc_id)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] not in ("Gateway.NotAttached", "InvalidInternetGatewayID.NotFound"):
                raise
        log(f"DELETING internet gateway: {igw}")
        try:
            ec2().delete_internet_gateway(InternetGatewayId=igw)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "InvalidInternetGatewayID.NotFound":
                raise
    log(f"DELETING VPC: {vpc_id}")
    for attempt in range(12):
        try:
            ec2().delete_vpc(VpcId=vpc_id)
            return
        except botocore.exceptions.ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "InvalidVpcID.NotFound":
                return
            if code == "DependencyViolation" and attempt < 11:
                log("VPC deletion blocked by DependencyViolation; waiting 5s and retrying...")
                time.sleep(5)
                continue
            raise


def cleanup_stack_once():
    global LOG_TO_FILE
    LOG_TO_FILE = CLEANUP_LOG_PATH.resolve()
    try:
        LOG_TO_FILE.write_text("")
    except OSError:
        pass
    try:
        log(f"Cleanup requested for stack: {STACK} in {REGION} (S3 bucket retained).")
        vpcs = get_vpcs()
        if not vpcs:
            log("No tagged VPC found; nothing to clean up.")
            return
        terminate_stack_instances()
        delete_stack_load_balancer()
        delete_stack_target_group()
        delete_stack_security_groups()
        nat_pending = delete_stack_nat_and_eip(wait_for_completion=False)
        delete_stack_route_tables()
        for vpc in vpcs:
            delete_stack_network_interfaces(vpc["VpcId"])
        for vpc in vpcs:
            delete_stack_subnets(vpc["VpcId"])
            delete_stack_igw_and_vpc(vpc["VpcId"])
        if nat_pending:
            log("Verifying NAT gateway deletion at the end of cleanup...")
            if wait_for_nat_deletion():
                release_stack_eip()
            else:
                log("Cleanup warning: NAT gateway still exists; rerun cleanup later to finish EIP release.")
        log("Cleanup complete. Flamegraph S3 bucket retained.")
    finally:
        LOG_TO_FILE = None


def cleanup_stack():
    while True:
        try:
            cleanup_stack_once()
            return
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "RequestExpired":
                log("AWS request expired during cleanup; refreshing session and retrying...")
                time.sleep(3)
                continue
            raise


def install_base(host: InstanceInfo, ctx: BootstrapContext):
    log(f"Ensuring base dependencies on {host.role}")
    ssh_run(host, """
if command -v dnf >/dev/null 2>&1; then
  PKG=dnf
else
  PKG=yum
fi
sudo $PKG -y update || true
if command -v amazon-linux-extras >/dev/null 2>&1; then
  sudo amazon-linux-extras enable epel || true
fi
sudo $PKG -y install epel-release || true
sudo $PKG -y install tar xz git perf jq gcc make jemalloc-devel openssl-devel sysstat ethtool iperf3 mtr binutils iproute || true
if command -v mtr >/dev/null 2>&1; then
  sudo $PKG -y reinstall mtr || true
else
  sudo $PKG -y install mtr || true
fi
sudo tee /etc/security/limits.d/99-valkey-nofile.conf >/dev/null <<'EOF'
* soft nofile 65535
* hard nofile 65535
root soft nofile 65535
root hard nofile 65535
EOF
sudo bash -c 'set -euo pipefail
CURRENT=$(cat /proc/sys/fs/file-max)
TARGET=65535
if [ "$CURRENT" -lt "$TARGET" ]; then
  VALUE=$TARGET
else
  VALUE=$CURRENT
fi
cat >/etc/sysctl.d/99-valkey-fd.conf <<EOF
fs.file-max = $VALUE
EOF
sysctl -q -p /etc/sysctl.d/99-valkey-fd.conf || true'
sudo tee /etc/sysctl.d/99-valkey-perf.conf >/dev/null <<'EOF'
kernel.perf_event_paranoid = -1
kernel.kptr_restrict = 0
EOF
sudo sysctl -q -p /etc/sysctl.d/99-valkey-perf.conf || true
""", ctx)


def build_valkey_binaries_on_client(client: InstanceInfo, remote_tar: str, ctx: BootstrapContext):
    log("Building Valkey binaries from source on client (fallback)")
    ssh_run(client, f"""
cd /tmp
rm -rf valkey-{VALKEY_VERSION} valkey-src.tgz
curl -L -o valkey-src.tgz '{VALKEY_SRC_URL}'
tar -xzf valkey-src.tgz
cd valkey-{VALKEY_VERSION}
make -j $(nproc) BUILD_TLS=no
sudo install -m 0755 src/valkey-server /usr/local/bin/valkey-server
sudo install -m 0755 src/valkey-cli /usr/local/bin/valkey-cli
sudo install -m 0755 src/valkey-benchmark /usr/local/bin/valkey-benchmark
tar -czf {remote_tar} -C src valkey-server valkey-cli valkey-benchmark
rm -rf /tmp/valkey-{VALKEY_VERSION} /tmp/valkey-src.tgz
""", ctx)


def ensure_valkey_artifact(client: InstanceInfo, ctx: BootstrapContext, local_tar: Path):
    remote_tar = f"/tmp/valkey-{VALKEY_VERSION}-binaries.tar.gz"
    log("Preparing Valkey binaries artifact on client")
    download_script = f"""
if [ -f {remote_tar} ]; then
  echo "Valkey artifact already present on client"
  exit 0
fi
WORK=/tmp/valkey-prebuilt
rm -rf "$WORK"
mkdir -p "$WORK"
cd "$WORK"
if curl -fL -o valkey-prebuilt.tgz '{VALKEY_BIN_URL}'; then
  tar -xzf valkey-prebuilt.tgz
  server=$(find . -type f -name valkey-server | head -n1)
  cli=$(find . -type f -name valkey-cli | head -n1)
  bench=$(find . -type f -name valkey-benchmark | head -n1)
  if [ -z "$server" ] || [ -z "$cli" ] || [ -z "$bench" ]; then
    echo "Prebuilt archive missing expected binaries" >&2
    exit 2
  fi
  cp "$server" ./valkey-server
  cp "$cli" ./valkey-cli
  cp "$bench" ./valkey-benchmark
  tar -czf {remote_tar} valkey-server valkey-cli valkey-benchmark
  rm -rf "$WORK"
  exit 0
fi
exit 1
"""
    result = ssh_capture(client, download_script, ctx, strict=False)
    if result.returncode == 1 or result.returncode == 2:
        log("Prebuilt Valkey download failed, falling back to source build.")
        build_valkey_binaries_on_client(client, remote_tar, ctx)
    elif result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, "ssh download prebuilt", result.stdout, result.stderr)
    scp_get(client, remote_tar, local_tar, ctx)


def install_valkey_cli_from_tar(host: InstanceInfo, ctx: BootstrapContext, tar_path: Path):
    remote_tar = f"/tmp/valkey-binaries-{host.role}.tar.gz"
    scp_put(host, tar_path, remote_tar, ctx)
    log(f"Installing Valkey CLI tools on {host.role} from artifact")
    ssh_run(host, f"""
if command -v valkey-cli >/dev/null 2>&1 && command -v valkey-benchmark >/dev/null 2>&1; then
  echo "Valkey CLI already installed on {host.role}"
  rm -f {remote_tar}
  exit 0
fi
sudo tar -xzf {remote_tar} -C /usr/local/bin valkey-cli valkey-benchmark
rm -f {remote_tar}
""", ctx)

def ensure_memtier_tool(host: InstanceInfo, ctx: BootstrapContext):
    log(f"Ensuring memtier_benchmark is available on {host.role}")
    ssh_run(host, f"""
if command -v memtier_benchmark >/dev/null 2>&1; then
  exit 0
fi
if command -v dnf >/dev/null 2>&1; then
  PKG=dnf
else
  PKG=yum
fi
if sudo $PKG -y install memtier-benchmark >/dev/null 2>&1; then
  exit 0
fi
echo "memtier-benchmark package not available via $PKG; building from source"
set -euo pipefail
if command -v dnf >/dev/null 2>&1; then
  sudo dnf -y install libevent-devel pkgconfig autoconf automake libtool make gcc gcc-c++ binutils || true
else
  sudo yum -y install libevent-devel pkgconfig autoconf automake libtool make gcc gcc-c++ binutils || true
fi
WORK=$(mktemp -d /tmp/memtier.XXXX)
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"
curl -L -o memtier-src.tgz '{MEMTIER_SRC_URL}'
tar -xzf memtier-src.tgz
cd memtier_benchmark-{MEMTIER_VERSION}
if [ -x ./build.sh ]; then
  ./build.sh
else
  autoreconf -ivf
  ./configure
  make -j $(nproc)
fi
sudo make install
""", ctx)

def ensure_docker(host: InstanceInfo, ctx: BootstrapContext):
    log(f"Ensuring Docker is installed on {host.role}")
    ssh_run(host, """
if ! command -v docker >/dev/null 2>&1; then
  if command -v amazon-linux-extras >/dev/null 2>&1; then
    sudo amazon-linux-extras enable docker || true
  fi
  sudo dnf -y install docker || sudo yum -y install docker
fi
sudo systemctl enable docker || true
sudo systemctl start docker || true
sudo usermod -aG docker ec2-user || true
""", ctx)


def configure_valkey_container(host: InstanceInfo, ctx: BootstrapContext, cluster_mode: bool):
    log(f"Configuring Valkey Docker service on {host.role}")
    cluster_flag = "yes" if cluster_mode else "no"
    ssh_run(host, f"""
set -euo pipefail
export PATH="/sbin:/usr/sbin:$PATH"
sudo systemctl stop valkey >/dev/null 2>&1 || true
sudo systemctl disable valkey >/dev/null 2>&1 || true
sudo rm -f /etc/systemd/system/valkey.service || true
sudo install -d -m 0755 /etc/valkey
sudo install -d -m 0755 /var/lib/valkey
sudo install -d -m 0755 /var/log/valkey
sudo touch /var/log/valkey/valkey.log
VALKEY_UID=999
VALKEY_GID=999
sudo chown -R $VALKEY_UID:$VALKEY_GID /var/lib/valkey
sudo chown -R $VALKEY_UID:$VALKEY_GID /var/log/valkey
sudo chown $VALKEY_UID:$VALKEY_GID /var/log/valkey/valkey.log
sudo chmod 0664 /var/log/valkey/valkey.log
MAXMEM=$(awk '/MemTotal/ {{printf "%d", int($2 * 1024 * 80 / 100)}}' /proc/meminfo)
TOTAL_CORES=$(nproc)
VALKEY_IO_THREADS=$TOTAL_CORES
VALKEY_CPUSET=""
CPUSET_ARGS=""
if [ "$TOTAL_CORES" -gt 2 ]; then
  VALKEY_IO_THREADS=$((TOTAL_CORES - 2))
  START_CPU=2
  END_CPU=$((TOTAL_CORES - 1))
  if [ "$START_CPU" -le "$END_CPU" ]; then
    if [ "$START_CPU" -eq "$END_CPU" ]; then
      VALKEY_CPUSET="$START_CPU"
    else
      VALKEY_CPUSET="$START_CPU-$END_CPU"
    fi
    CPUSET_ARGS="--cpuset-cpus=$VALKEY_CPUSET"
  fi
  IFACE=$(ip route get 1.1.1.1 2>/dev/null | awk '/dev/ {{for (i=1;i<=NF;i++) if ($i=="dev") {{print $(i+1); exit}}}}')
  if [ -n "$IFACE" ]; then
    sudo ethtool -L "$IFACE" combined 2 || true
    IRQ1=""
    IRQ2=""
    while read -r irq _; do
      irq=${{irq%%:*}}
      irq=${{irq//[[:space:]]/}}
      if [ -z "$IRQ1" ]; then
        IRQ1=$irq
      elif [ -z "$IRQ2" ]; then
        IRQ2=$irq
        break
      fi
    done < <(grep -E "$IFACE" /proc/interrupts || true)
    if [ -n "$IRQ1" ]; then
      MASK=$(printf "%x" $((1<<0)))
      echo "$MASK" | sudo tee /proc/irq/$IRQ1/smp_affinity >/dev/null || true
    fi
    if [ -n "$IRQ2" ]; then
      MASK=$(printf "%x" $((1<<1)))
      echo "$MASK" | sudo tee /proc/irq/$IRQ2/smp_affinity >/dev/null || true
    fi
  fi
fi
if [ "$VALKEY_IO_THREADS" -lt 1 ]; then
  VALKEY_IO_THREADS=1
fi
sudo tee /etc/valkey/valkey.conf >/dev/null <<CONF
port 6379
bind 0.0.0.0
protected-mode no
cluster-enabled {cluster_flag}
cluster-config-file /var/lib/valkey/nodes.conf
cluster-node-timeout 5000
appendonly no
save ""
dir /var/lib/valkey
logfile /var/log/valkey/valkey.log
maxclients 64000
maxmemory $MAXMEM
io-threads $VALKEY_IO_THREADS
io-threads-do-reads yes
CONF
sudo docker rm -f valkey >/dev/null 2>&1 || true
sudo docker pull {VALKEY_DOCKER_IMAGE}
sudo docker run -d --name valkey \
  --network host \
  --restart unless-stopped \
  --ulimit nofile=65535:65535 \
  --user 0:0 \
  $CPUSET_ARGS \
  -v /etc/valkey:/etc/valkey:ro \
  -v /var/lib/valkey:/var/lib/valkey \
  -v /var/log/valkey:/var/log/valkey \
  {VALKEY_DOCKER_IMAGE} valkey-server /etc/valkey/valkey.conf
""", ctx)


def prepare_envoy_host(host: InstanceInfo, ctx: BootstrapContext):
    log(f"Preparing Envoy host {host.role} for Docker")
    ssh_run(host, """
sudo systemctl stop envoy >/dev/null 2>&1 || true
sudo systemctl disable envoy >/dev/null 2>&1 || true
sudo rm -f /etc/systemd/system/envoy.service || true
sudo install -d -m 0755 /etc/envoy
""", ctx)


def validate_envoy_config(host: InstanceInfo, ctx: BootstrapContext):
    ssh_run(host, f"""
sudo docker pull {ENVOY_DOCKER_IMAGE}
sudo docker run --rm \
  -v /etc/envoy/envoy.yaml:/etc/envoy/envoy.yaml:ro \
  {ENVOY_DOCKER_IMAGE} envoy -c /etc/envoy/envoy.yaml --mode validate
""", ctx)


def cache_envoy_symbols(host: InstanceInfo, ctx: BootstrapContext):
    ssh_run(host, """
set -euo pipefail
container=$(sudo docker ps -aqf 'name=^envoy$' | head -n1 || true)
if [ -z "$container" ]; then
  exit 0
fi
tmp=$(mktemp -d /tmp/envoy-syms.XXXX)
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT
sudo docker cp "$container":/usr/local/bin/envoy "$tmp/envoy" >/dev/null 2>&1 || exit 0
buildid=$(readelf -n "$tmp/envoy" 2>/dev/null | awk '/Build ID/ {print $3; exit}')
if [ -z "$buildid" ]; then
  exit 0
fi
dir=/usr/lib/debug/.build-id/${buildid:0:2}
sudo mkdir -p "$dir"
sudo cp "$tmp/envoy" "$dir/${buildid:2}.debug"
""", ctx)


def restart_envoy_container(host: InstanceInfo, ctx: BootstrapContext):
    ssh_run(host, f"""
sudo docker rm -f envoy >/dev/null 2>&1 || true
sudo docker run -d --name envoy \
  --network host \
  --restart unless-stopped \
  --ulimit nofile=65535:65535 \
  -v /etc/envoy/envoy.yaml:/etc/envoy/envoy.yaml:ro \
  {ENVOY_DOCKER_IMAGE} envoy -c /etc/envoy/envoy.yaml --log-level info
""", ctx)
    cache_envoy_symbols(host, ctx)


def wait_for_valkey_nodes(client: InstanceInfo, valkey_hosts, ctx: BootstrapContext, timeout=300):
    host_list = " ".join(host.private_ip for host in valkey_hosts)
    log(f"Waiting for Valkey nodes to accept connections: {host_list}")
    ssh_run(client, f"""
HOSTS=({host_list})
DEADLINE=$(( $(date +%s) + {timeout} ))
while [ ${{#HOSTS[@]}} -gt 0 ]; do
  REMAINING=()
  for H in "${{HOSTS[@]}}"; do
    if valkey-cli -h "$H" PING >/dev/null 2>&1; then
      echo "Valkey node $H is up"
    else
      REMAINING+=("$H")
    fi
  done
  if [ ${{#REMAINING[@]}} -eq 0 ]; then
    break
  fi
  if [ $(date +%s) -ge $DEADLINE ]; then
    echo "Timed out waiting for Valkey nodes: ${{REMAINING[*]}}" >&2
    exit 1
  fi
  HOSTS=("${{REMAINING[@]}}")
  sleep 5
done
""", ctx)


def valkey_cluster_state(client: InstanceInfo, host_ip: str, ctx: BootstrapContext):
    result = ssh_capture(client, f"""
state=$(valkey-cli -h {host_ip} cluster info 2>/dev/null | grep '^cluster_state' | awk -F: '{{print $2}}' | tr -d '\\r')
echo "${{state:-unknown}}"
""", ctx, strict=False)
    if result.returncode != 0:
        return "unknown"
    return result.stdout.strip()


def form_valkey_cluster(client: InstanceInfo, valkey_hosts, ctx: BootstrapContext):
    if len(valkey_hosts) < 3:
        log("Fewer than 3 Valkey nodes detected; cluster creation skipped.")
        return
    primary = valkey_hosts[0].private_ip
    state = valkey_cluster_state(client, primary, ctx)
    if state == "ok":
        log("Valkey cluster already healthy; skipping cluster creation.")
    else:
        log("Creating Valkey cluster")
        peers = " ".join(f"{host.private_ip}:{REDIS_PORT}" for host in valkey_hosts)
        ssh_run(client, f"""
valkey-cli --cluster create {peers} --cluster-replicas 0 --cluster-yes || true
""", ctx)
    ssh_run(client, f"""
valkey-cli -h {primary} cluster info | tee ~/valkey-cluster-info.txt
""", ctx)


def render_envoy_config(valkey_hosts, cluster_mode: bool):
    endpoints = "\n".join(
        f"        - endpoint: {{ address: {{ socket_address: {{ address: {host.private_ip}, port_value: 6379 }} }} }}"
        for host in valkey_hosts
    )
    lb_policy = "CLUSTER_PROVIDED" if cluster_mode else "ROUND_ROBIN"
    if cluster_mode:
        settings_block = """          stat_prefix: redis_stats
          settings:
            op_timeout: 5s
            enable_command_stats: true
            enable_redirection: true
"""
    else:
        settings_block = """          stat_prefix: redis_stats
          settings:
            op_timeout: 5s
            enable_command_stats: true
"""
    cluster_type_block = """
    cluster_type:
      name: envoy.clusters.redis
      typed_config:
        "@type": type.googleapis.com/envoy.extensions.clusters.redis.v3.RedisClusterConfig
        cluster_refresh_rate: 5s
        cluster_refresh_timeout: 3s
        redirect_refresh_interval: 5s
        redirect_refresh_threshold: 5
""" if cluster_mode else ""
    if cluster_mode:
        route_block = """
          prefix_routes:
            routes:
            - prefix: "/"
              cluster: valkey_cluster
"""
    else:
        route_block = """
          prefix_routes:
            catch_all_route:
              cluster: valkey_cluster
"""
    return f"""static_resources:
  listeners:
  - name: valkey_listener
    address:
      socket_address: {{ address: 0.0.0.0, port_value: 6379 }}
    filter_chains:
    - filters:
      - name: envoy.filters.network.redis_proxy
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.redis_proxy.v3.RedisProxy
{settings_block.rstrip()}
{route_block.rstrip()}
  clusters:
  - name: valkey_cluster
    connect_timeout: 1s
    lb_policy: {lb_policy}
    load_assignment:
      cluster_name: valkey_cluster
      endpoints:
      - lb_endpoints:
{endpoints}
{cluster_type_block.rstrip()}
admin:
  address:
    socket_address: {{ address: 0.0.0.0, port_value: 9901 }}
"""


def deploy_envoy_config(envoy_hosts, valkey_hosts, ctx: BootstrapContext, cluster_mode: bool):
    config_text = render_envoy_config(valkey_hosts, cluster_mode)
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "envoy.yaml"
        path.write_text(config_text)
        for host in envoy_hosts:
            log(f"Pushing envoy.yaml to {host.role}")
            scp_put(host, path, "/tmp/envoy.yaml", ctx)
            ssh_run(host, """
sudo mv /tmp/envoy.yaml /etc/envoy/envoy.yaml
sudo chmod 0644 /etc/envoy/envoy.yaml
""", ctx)
            validate_envoy_config(host, ctx)
            restart_envoy_container(host, ctx)


def capture_envoy_perf(envoy: InstanceInfo, ctx: BootstrapContext, duration: int = 60):
    log(f"Capturing perf profile on {envoy.role} for {duration}s")
    ssh_run(envoy, f"""
sudo pkill perf >/dev/null 2>&1 || true
pid=$(pgrep envoy | head -n1 || true)
if [ -n "$pid" ]; then
  data=/tmp/perf-envoy.data
  sudo perf record -F 199 -g --call-graph dwarf -e cycles:u -e cycles:k -o "$data" -p "$pid" -- sleep {duration} || true
  cd ~
  if [ ! -d FlameGraph ]; then
    git clone https://github.com/brendangregg/FlameGraph.git || true
  fi
  sudo perf script -i "$data" | ~/FlameGraph/stackcollapse-perf.pl --kernel | ~/FlameGraph/flamegraph.pl > ~/envoy-profile.svg || true
  sudo rm -f "$data"
fi
""", ctx)


def start_envoy_highload(client: InstanceInfo, envoy: InstanceInfo, ctx: BootstrapContext,
                         run_seconds: int = 180, clients: int = 600, threads: int = 12, pipeline: int = 32):
    target = envoy.private_ip or envoy.public_ip
    if not target:
        raise RuntimeError(f"{envoy.role} has no reachable IP for high-load benchmark.")
    log(f"Starting memtier high-load against {envoy.role} ({target}) "
        f"[threads={threads}, clients={clients}, pipeline={pipeline}, duration={run_seconds}s]")
    cmd = (
        f"memtier_benchmark -s {target} -p {REDIS_PORT} --protocol redis --cluster-mode "
        f"--clients {clients} --threads {threads} --data-size 64 --key-maximum=1000000 "
        f"--key-pattern=P:P --ratio=1:1 --pipeline {pipeline} --hide-histogram --test-time {run_seconds}"
    )
    ssh_run(client, f"""
command -v memtier_benchmark >/dev/null 2>&1
rm -f ~/memtier-benchmark.log
cat <<'SCRIPT' >/tmp/run-memtier.sh
#!/usr/bin/env bash
set -euo pipefail
{cmd}
SCRIPT
chmod +x /tmp/run-memtier.sh
nohup /tmp/run-memtier.sh > ~/memtier-benchmark.log 2>&1 &
echo $! > ~/memtier-benchmark.pid
""", ctx)


def wait_for_envoy_highload(client: InstanceInfo, ctx: BootstrapContext, poll_seconds: int = 5, timeout_seconds: int = 600):
    max_checks = max(1, timeout_seconds // poll_seconds)
    ssh_run(client, f"""
checks=0
while pgrep -f memtier_benchmark >/dev/null 2>&1; do
  checks=$((checks+1))
  if [ $checks -ge {max_checks} ]; then
    echo "Timed out waiting for memtier benchmark to finish" >&2
    exit 1
  fi
  sleep {poll_seconds}
done
rm -f ~/memtier-benchmark.pid
""", ctx)


def stop_envoy_highload(client: InstanceInfo, ctx: BootstrapContext):
    ssh_run(client, """
if pgrep -f memtier_benchmark >/dev/null 2>&1; then
  pkill -f memtier_benchmark >/dev/null 2>&1 || true
fi
rm -f ~/memtier-benchmark.pid
""", ctx, strict=False)


def log_envoy_cpu(envoy: InstanceInfo, ctx: BootstrapContext):
    log(f"Sampling CPU on {envoy.role} during high-load run")
    ssh_run(envoy, """
if command -v mpstat >/dev/null 2>&1; then
  mpstat -P ALL 1 10
else
  top -b -n5 -d 1 | grep envoy || true
fi
""", ctx, strict=False)


def collect_memtier_log(client: InstanceInfo, ctx: BootstrapContext, bucket: str):
    log("Collecting memtier benchmark log from client")
    tmp_dir = Path(tempfile.mkdtemp())
    tmp_log = tmp_dir / f"memtier-benchmark-{int(time.time())}.log"
    url = "Unavailable"
    try:
        scp_get(client, "~/memtier-benchmark.log", tmp_log, ctx)
        if tmp_log.exists():
            key = f"benchmarks/{tmp_log.name}"
            url = upload_file_to_s3(tmp_log, bucket, key)
    except subprocess.CalledProcessError:
        log("Warning: Failed to retrieve/upload memtier benchmark log")
    finally:
        if tmp_log.exists():
            tmp_log.unlink(missing_ok=True)
        try:
            tmp_dir.rmdir()
        except OSError:
            pass
    return url


def upload_envoy_flamegraph(envoy: InstanceInfo, ctx: BootstrapContext, bucket: str):
    log(f"Uploading envoy flamegraph from {envoy.role}")
    tmp_dir = Path(tempfile.mkdtemp())
    tmp_svg = tmp_dir / f"envoy-profile-{int(time.time())}.svg"
    url = "Unavailable"
    try:
        scp_get(envoy, "~/envoy-profile.svg", tmp_svg, ctx)
        if tmp_svg.exists():
            key = f"profiles/{tmp_svg.name}"
            url = upload_file_to_s3(tmp_svg, bucket, key)
    except subprocess.CalledProcessError:
        log("Warning: Failed to retrieve/upload envoy-profile.svg")
    finally:
        if tmp_svg.exists():
            tmp_svg.unlink(missing_ok=True)
        try:
            tmp_dir.rmdir()
        except OSError:
            pass
    return url


def create_cloudwatch_dashboard(client_entries, valkey_entries, envoy_entries):
    def cpu_widget(title, entries):
        metrics = []
        for role, iid in entries:
            metrics.append(["AWS/EC2", "CPUUtilization", "InstanceId", iid, {"label": role, "stat": "Average"}])
        return {
            "type": "metric",
            "width": 12,
            "height": 6,
            "properties": {
                "region": REGION,
                "stat": "Average",
                "view": "timeSeries",
                "stacked": False,
                "period": 60,
                "title": title,
                "metrics": metrics,
            },
        }

    def network_widget(title, entries):
        metrics = []
        for role, iid in entries:
            metrics.append(["AWS/EC2", "NetworkIn", "InstanceId", iid, {"stat": "Sum", "label": f"{role} in"}])
            metrics.append([".", "NetworkOut", ".", ".", {"stat": "Sum", "label": f"{role} out"}])
        return {
            "type": "metric",
            "width": 12,
            "height": 6,
            "properties": {
                "region": REGION,
                "stat": "Sum",
                "view": "timeSeries",
                "stacked": False,
                "period": 60,
                "title": f"{title} Network",
                "metrics": metrics,
            },
        }

    def counts_widget(valkeys, envoys, base_instance_id):
        metrics = [
            ["AWS/EC2", "CPUUtilization", "InstanceId", base_instance_id, {"id": "m_base", "visible": False}],
            [{"expression": f"0*m_base+{valkeys}", "label": "Valkey shards", "id": "e1"}],
            [{"expression": f"0*m_base+{envoys}", "label": "Envoy instances", "id": "e2"}],
        ]
        return {
            "type": "metric",
            "width": 12,
            "height": 4,
            "properties": {
                "region": REGION,
                "view": "singleValue",
                "title": "Cluster Topology",
                "metrics": metrics,
            },
        }

    dash_name = f"{STACK}-ops"
    widgets = []
    widgets.append(cpu_widget("Client CPU", client_entries))
    widgets.append(network_widget("Client", client_entries))
    widgets.append(cpu_widget("Valkey CPU", valkey_entries))
    widgets.append(network_widget("Valkey", valkey_entries))
    widgets.append(cpu_widget("Envoy CPU", envoy_entries))
    widgets.append(network_widget("Envoy", envoy_entries))
    widgets.append(counts_widget(len(valkey_entries), len(envoy_entries), client_entries[0][1]))
    body = {"widgets": widgets}
    cw = aws_session().client("cloudwatch", region_name=REGION, config=BOTO_CONFIG)
    try:
        cw.put_dashboard(DashboardName=dash_name, DashboardBody=json.dumps(body))
    except botocore.exceptions.ClientError:
        pass
    return dash_name, f"https://console.aws.amazon.com/cloudwatch/home?region={REGION}#dashboards:name={dash_name}"


def bootstrap_cluster(args, provisioned):
    if args.skip_bootstrap:
        log("Skipping bootstrap (--skip-bootstrap).")
        return
    key_path = Path(args.ssh_key_path or DEFAULT_SSH_KEY_PATH).expanduser().resolve()
    if not key_path.exists():
        raise SystemExit(f"ERROR: SSH key file not found: {key_path}")
    need_cmd("ssh")
    need_cmd("scp")

    ssh_cidr = args.ssh_cidr or my_public_cidr()
    log(f"Bootstrap starting for stack: {STACK} in {REGION}")
    log(f"SSH source CIDR: {ssh_cidr}")

    role_info = gather_instance_info(provisioned["instances"])
    client = role_info["client"]
    ctx = BootstrapContext(ssh_key_path=key_path, self_cidr=ssh_cidr, client=client)
    valkey_roles = sorted(
        [role for role in role_info if role.startswith("valkey-")],
        key=lambda r: int(r.rsplit("-", 1)[1]),
    )
    if not valkey_roles:
        raise RuntimeError("No Valkey instances discovered; re-run provisioning.")
    cluster_mode = provisioned.get("cluster_mode")
    if cluster_mode is None:
        cluster_mode = len(valkey_roles) >= 3

    def ensure_ip(host, kind):
        value = getattr(host, kind)
        if not value:
            raise RuntimeError(f"{host.role} missing {kind} IP")
        return value

    log("Discovered hosts/IPs:")
    envoy_roles = sorted(
        [role for role in role_info if role.startswith("envoy-")],
        key=lambda r: int(r.rsplit("-", 1)[1]),
    )
    if not envoy_roles:
        raise RuntimeError("No Envoy instances discovered; re-run provisioning.")
    ordered_roles = ["client"] + envoy_roles + valkey_roles
    for role in ordered_roles:
        host = role_info[role]
        log(f"  {role}: public={host.public_ip or '-'} private={host.private_ip}")
        ensure_ip(host, "private_ip")

    if not client.public_ip:
        raise RuntimeError("Client instance missing public IP; required for SSH bastion.")

    # Refresh SG SSH rules
    log(f"Refreshing SSH rules to {ssh_cidr}")
    refresh_ssh_rule(provisioned["security_groups"]["client"], ssh_cidr)
    ensure_ingress_all_between(provisioned["security_groups"]["envoy"], provisioned["security_groups"]["valkey"])
    ensure_ingress_all_between(provisioned["security_groups"]["client"], provisioned["security_groups"]["envoy"])
    ensure_ingress_all_between(provisioned["security_groups"]["client"], provisioned["security_groups"]["valkey"])
    ensure_sg_ingress_from_group(provisioned["security_groups"]["envoy"], provisioned["security_groups"]["client"], "tcp", SSH_PORT, SSH_PORT)
    ensure_sg_ingress_from_group(provisioned["security_groups"]["envoy"], provisioned["security_groups"]["client"], "udp", 5201, 5201)
    ensure_sg_ingress_from_group(provisioned["security_groups"]["envoy"], provisioned["security_groups"]["client"], "tcp", 5201, 5201)
    ensure_sg_ingress_from_group(provisioned["security_groups"]["valkey"], provisioned["security_groups"]["client"], "tcp", SSH_PORT, SSH_PORT)
    ensure_sg_ingress_from_group(provisioned["security_groups"]["valkey"], provisioned["security_groups"]["client"], "tcp", REDIS_PORT, REDIS_PORT)
    ensure_ingress_tcp_cidr(provisioned["security_groups"]["envoy"], REDIS_PORT, f"{role_info['client'].private_ip}/32")

    hosts_for_base = [role_info["client"]]
    hosts_for_base.extend(role_info[r] for r in envoy_roles + valkey_roles)

    # Basic SSH + deps
    for host in hosts_for_base:
        log(f"Testing SSH to {host.role}")
        ssh_run(host, "echo ok", ctx)
        install_base(host, ctx)

    valkey_hosts = [role_info[r] for r in valkey_roles]
    envoy_hosts = [role_info[r] for r in envoy_roles]
    client_entries = [("client", client.instance_id)]
    valkey_entries = [(host.role, host.instance_id) for host in valkey_hosts]
    envoy_entries = [(host.role, host.instance_id) for host in envoy_hosts]

    for host in valkey_hosts + envoy_hosts:
        ensure_docker(host, ctx)

    with tempfile.TemporaryDirectory() as tmpdir:
        local_tar = Path(tmpdir) / f"valkey-{VALKEY_VERSION}-binaries.tar.gz"
        ensure_valkey_artifact(client, ctx, local_tar)
        install_valkey_cli_from_tar(client, ctx, local_tar)
        ensure_memtier_tool(client, ctx)

    for host in valkey_hosts:
        configure_valkey_container(host, ctx, cluster_mode)

    for host in envoy_hosts:
        prepare_envoy_host(host, ctx)

    wait_for_valkey_nodes(client, valkey_hosts, ctx)
    if cluster_mode:
        form_valkey_cluster(client, valkey_hosts, ctx)
    else:
        log("Single-node Valkey deployment requested; skipping cluster create.")
    deploy_envoy_config(envoy_hosts, valkey_hosts, ctx, cluster_mode)

    nlb_dns = provisioned.get("nlb_dns")
    if nlb_dns:
        log(f"NLB DNS: {nlb_dns}")
    bucket = f"{STACK}-flamegraphs"
    ensure_bucket_with_lifecycle(bucket)
    log("Artifacts bucket ready (memtier logs + flamegraphs will expire in 7 days).")
    log("Use run_benchmark.py to drive Envoy load and capture new flamegraphs when needed.")

    dash_name, dash_url = create_cloudwatch_dashboard(client_entries, valkey_entries, envoy_entries)

    envoy_admin_host = envoy_hosts[0]
    if envoy_admin_host.public_ip:
        envoy_admin_line = f"http://{envoy_admin_host.public_ip}:9901"
    else:
        envoy_admin_line = f"http://{envoy_admin_host.private_ip}:9901 (reachable from client)"
    valkey_nodes = ", ".join(host.private_ip for host in valkey_hosts)
    log("Bootstrap complete.")
    log(f"Client SSH: ssh -i {key_path} ec2-user@{client.public_ip}")
    log(f"Valkey nodes (private): {valkey_nodes}")
    log(f"Envoy admin ({envoy_admin_host.role}): {envoy_admin_line}")
    log("Post-deployment validation: run ./valkey_validate.py for benchmarks & health checks.")
    log(f"CloudWatch dashboard ({dash_name}): {dash_url}")

# -------------
# Main flow
# -------------
def ensure_keypair_accessible():
    attempts = 0
    while True:
        try:
            ec2().describe_key_pairs(KeyNames=[KEY_NAME])
            return
        except botocore.exceptions.NoCredentialsError:
            raise SystemExit("ERROR: AWS credentials not found. Export AWS credentials or run setup-aws-creds.sh before re-running.")
        except botocore.exceptions.ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "RequestExpired" and attempts < 3:
                attempts += 1
                log("AWS request expired while checking KeyPair; retrying...")
                time.sleep(3)
                continue
            if code == "InvalidKeyPair.NotFound":
                raise SystemExit(f"ERROR: KeyPair '{KEY_NAME}' missing in {REGION}. Create/import it and re-run.")
            if code in {"AuthFailure", "UnauthorizedOperation", "UnrecognizedClientException"}:
                raise SystemExit(f"ERROR: AWS credentials are invalid or expired ({code}). Refresh credentials and try again.")
            raise


def main(args):
    if args.cleanup:
        cleanup_stack()
        return
    log(f"Starting (apply) for stack: {STACK} in {REGION}")
    ensure_keypair_accessible()
    log(f"Ensuring KeyPair '{KEY_NAME}' exists (will NOT delete in cleanup)")
    log(f"REUSED  keypair: {KEY_NAME}")

    vpc_id = ensure_vpc()
    igw_id = ensure_igw(vpc_id)

    subnet_public = ensure_subnet(vpc_id, f"{STACK}-subnet-public", PUB_CIDR, public=True)
    subnet_private = ensure_subnet(vpc_id, f"{STACK}-subnet-private", PRIV_CIDR, public=False)

    eipalloc = ensure_eip()
    nat_id = ensure_nat(subnet_public, eipalloc)

    rtb_pub = ensure_public_rtb(vpc_id, igw_id, subnet_public)
    rtb_priv = ensure_private_rtb(vpc_id, nat_id, subnet_private)

    # Security groups
    sg_client = ensure_sg(vpc_id, f"{STACK}-client", "client")
    sg_envoy  = ensure_sg(vpc_id, f"{STACK}-envoy",  "envoy")
    sg_valkey = ensure_sg(vpc_id, f"{STACK}-valkey", "valkey")

    # SSH rule for current public IP -> client
    mycidr = args.ssh_cidr or my_public_cidr()
    refresh_ssh_rule(sg_client, mycidr)
    log(f"Client SG SSH restricted to {mycidr}")

    # Allow all internal traffic between Envoy and Valkey (both directions + within groups)
    ensure_ingress_all_between(sg_envoy, sg_valkey)

    # Allow SSH and Redis from the client SG into Envoy/Valkey nodes (bastion + bootstrap access)
    ensure_sg_ingress_from_group(sg_envoy, sg_client, "tcp", SSH_PORT, SSH_PORT)
    ensure_sg_ingress_from_group(sg_valkey, sg_client, "tcp", SSH_PORT, SSH_PORT)
    ensure_sg_ingress_from_group(sg_valkey, sg_client, "tcp", REDIS_PORT, REDIS_PORT)

    if args.valkey_nodes < 1:
        raise SystemExit("ERROR: --valkey-nodes must be at least 1.")
    if 1 < args.valkey_nodes < 3:
        raise SystemExit("ERROR: --valkey-nodes supports 1 (standalone) or >=3 (clustered) nodes.")
    if args.envoy_nodes < 1:
        raise SystemExit("ERROR: --envoy-nodes must be at least 1.")
    cluster_mode = args.valkey_nodes >= 3

    # Instances
    client_id = ensure_instance(f"{STACK}-client","client",CLIENT_TYPE,subnet_public,sg_client,True)
    envoy_instances = []
    for idx in range(1, args.envoy_nodes + 1):
        role = f"envoy-{idx}"
        iid = ensure_instance(f"{STACK}-{role}", role, ENVOY_TYPE, subnet_private, sg_envoy, False)
        envoy_instances.append((role, iid))
    valkey_instances = []
    for idx in range(1, args.valkey_nodes + 1):
        role = f"valkey-{idx}"
        iid = ensure_instance(f"{STACK}-{role}", role, VALKEY_TYPE, subnet_private, sg_valkey, False)
        valkey_instances.append((role, iid))

    client_pub_ip = instance_public_ip(client_id)
    client_priv_ip = instance_private_ip(client_id)
    log(f"Client public IP: {client_pub_ip}")

    # NLB (internal) for Envoy on 6379
    nlb_name = f"{STACK}-nlb"
    lb_arn = ensure_nlb(nlb_name, subnet_private)
    tg_arn = ensure_tg(vpc_id, REDIS_PORT)
    register_targets(tg_arn, [iid for _, iid in envoy_instances], REDIS_PORT)
    ensure_listener(lb_arn, tg_arn, REDIS_PORT)
    ensure_ingress_tcp_cidr(sg_envoy, REDIS_PORT, f"{client_priv_ip}/32")

    # Final summary
    log("=== SUMMARY ===")
    log(f"VPC: {vpc_id}")
    log(f"Subnets: public={subnet_public}, private={subnet_private}")
    log(f"IGW: {igw_id}, EIP: {eipalloc}, NAT: {nat_id}")
    log(f"RTBs: public={rtb_pub}, private={rtb_priv}")
    log(f"SGs: client={sg_client}, envoy={sg_envoy}, valkey={sg_valkey}")
    instance_log_parts = [f"client={client_id}"] + [f"{role}={iid}" for role, iid in envoy_instances + valkey_instances]
    log(f"Instances: {', '.join(instance_log_parts)}")
    log(f"NLB: {lb_arn} (internal), TG: {tg_arn} (port {REDIS_PORT})")
    log("Created/reused resources are fully tagged for idempotency and future cleanup.")
    log("Only the client has a public IP. SSH is allowed from your current IP to the client; client can SSH to Envoy/Valkey inside the VPC.")

    instance_map = {"client": client_id}
    instance_map.update(envoy_instances)
    instance_map.update(valkey_instances)

    resources = {
        "instances": instance_map,
        "security_groups": {
            "client": sg_client,
            "envoy": sg_envoy,
            "valkey": sg_valkey,
        },
        "client_id": client_id,
        "nlb_dns": get_nlb_dns(lb_arn),
        "valkey_count": args.valkey_nodes,
        "envoy_count": args.envoy_nodes,
        "cluster_mode": cluster_mode,
    }
    bootstrap_cluster(args, resources)
    log("Done.")


if __name__ == "__main__":
    parser = parse_args()
    cli_args = parser.parse_args()
    configure_from_args(cli_args)
    main(cli_args)
