# Valkey Bench

Utilities for provisioning, validating, and benchmarking the Valkey load-test stack. The scripts expect an AWS profile that can create EC2/ELB resources and a pre-uploaded SSH key pair.

## Contents
- `valkey_setup.py` – provisions networking + EC2 resources, installs dependencies, and wires Envoy/Valkey with the desired mode (standalone or cluster).
- `valkey_validate.py` – inspects the deployed stack, runs health checks/benchmarks, prints manual test commands, and surfaces RTT + cost info.
- `valkey_benchmark.py` – runs `valkey-benchmark` (or memtier) from the client host while optionally capturing perf/FlameGraph data from Envoy.

## Prerequisites
- Python 3.9+ with `boto3` available.
- AWS CLI v2 configured with a profile (e.g., `sandbox`) pointing at the target account.
- SSH key pair (`valkey-load-test-key`) uploaded to EC2.

### Generating and Uploading `valkey-load-test-key.pem`
```bash
# Generate PEM-formatted key locally
ssh-keygen -t rsa -b 4096 -m PEM -f valkey-load-test-key
chmod 400 valkey-load-test-key.pem

# Upload the public key to EC2 (imports as KeyPair)
aws ec2 import-key-pair \
  --key-name valkey-load-test-key \
  --public-key-material fileb://valkey-load-test-key.pub \
  --profile sandbox --region us-east-1
```
After importing, place `valkey-load-test-key.pem` in the repo root (or wherever you run the scripts) so SSH connections succeed.

## Provisioning the Stack
```bash
AWS_PROFILE=sandbox python3 valkey_setup.py \
  --region us-east-1 \
  --seed vlklt-001 \
  --ssh-private-key-path ./valkey-load-test-key.pem
```
Key flags:
- `--valkey-nodes` / `--envoy-nodes` control topology (≥3 Valkey nodes turns on cluster mode).
- `--skip-bootstrap` to stop after Terraform-style provisioning.
- `--cleanup` to tear everything down.

## Validating the Deployment
```bash
AWS_PROFILE=sandbox python3 valkey_validate.py \
  --ssh-private-key-path ./valkey-load-test-key.pem
```
The validator prints:
- Instance inventory + SSH shortcuts.
- Envoy load-balancer DNS name.
- Manual benchmarking + RTT commands (memtier, `valkey-cli`, `mtr`, FlameGraph capture).
- Keyspace stats, Envoy GET sanity check, lightweight benchmark, RTT results, and cost estimates.

## Running Benchmarks + FlameGraphs
```bash
AWS_PROFILE=sandbox python3 valkey_benchmark.py \
  --ssh-host 35.175.173.51 \
  --ssh-user ec2-user \
  --ssh-key ./valkey-load-test-key.pem \
  --mode proxy \
  --flamegraph-host 10.42.2.22 \
  --flamegraph-user ec2-user \
  --flamegraph-key ./valkey-load-test-key.pem \
  --flamegraph-jump-host 35.175.173.51 \
  --flamegraph-jump-user ec2-user
```
Adjust `--mode valkey` to hit a Valkey node directly; omit the perf flags to skip FlameGraph capture. Results are saved to `valkey-benchmark.log`, with SVGs copied to both the repo and `~/Downloads` and uploaded to the configured S3 bucket.

## Manual Test Snippets
`valkey_validate.py` emits a ready-to-run block:
```bash
VALKEY_IP=10.42.2.47
PROXY_IP=10.42.2.22
NODE_IP=$PROXY_IP   # or $VALKEY_IP for direct mode

# SET-only load (data population)
memtier_benchmark -s $NODE_IP -p 6379 --ratio=1:0 --print-percentiles 50,95,99 \
  --clients=8 --threads=8 -d 1024 --key-maximum=10000000 --distinct-client-seed \
  --test-time 120 --hide-histogram
valkey-cli -h $VALKEY_IP -p 6379 dbsize
valkey-cli -h $VALKEY_IP -p 6379 info memory | grep human

# Mixed SET:GET 1:10
memtier_benchmark -s $NODE_IP -p 6379 --ratio=1:10 --print-percentiles 50,95,99 \
  --clients=8 --threads=8 -d 1024 --key-maximum=10000000 --distinct-client-seed \
  --test-time 120 --hide-histogram

# RTT checks
sudo mtr -n -c 20 -i 0.2 --report --report-wide $PROXY_IP --tcp --psize 1024
ssh -o ProxyCommand="ssh -i valkey-load-test-key.pem -o StrictHostKeyChecking=no \
  -o IdentitiesOnly=yes -o ConnectTimeout=10 ec2-user@35.175.173.51 -W %h:%p" \
  -i valkey-load-test-key.pem ec2-user@$PROXY_IP \
  'sudo mtr -n -c 20 -i 0.2 --report --report-wide $VALKEY_IP'

# Envoy perf capture + download
ssh -i valkey-load-test-key.pem -o ProxyCommand="ssh -i valkey-load-test-key.pem \
  -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -o ConnectTimeout=10 \
  ec2-user@35.175.173.51 -W %h:%p" ec2-user@$PROXY_IP 'bash -lc "set -euo pipefail; \
  sudo rm -f /tmp/perf-valkey.data /tmp/valkey-flamegraph.svg && \
  sudo perf record -F 99 -a -g -o /tmp/perf-valkey.data -- sleep 30 && \
  sudo perf script -i /tmp/perf-valkey.data | ~/FlameGraph/stackcollapse-perf.pl --kernel | \
  ~/FlameGraph/flamegraph.pl > /tmp/valkey-flamegraph.svg"'
scp -i valkey-load-test-key.pem -o ProxyCommand="ssh -i valkey-load-test-key.pem \
  -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -o ConnectTimeout=10 \
  ec2-user@35.175.173.51 -W %h:%p" ec2-user@$PROXY_IP:/tmp/valkey-flamegraph.svg ~/Downloads/valkey-flamegraph.svg
```
Customize IPs and paths as needed; the validator will always print the current values.
