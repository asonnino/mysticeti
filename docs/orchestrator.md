# Geo-Replicated Testbeds

The `orchestrator` crate provides facilities for deploying the codebase on a set of cloud instances
and running benchmarks against the deployed network.

The instructions below walk through a full run on [Amazon Web Services
(AWS)](https://aws.amazon.com), which is the primary supported target. Additional cloud providers
can be added by implementing the [`ServerProviderClient`](../crates/orchestrator/src/provider.rs)
trait.

All orchestrator functionality is driven through the `remote-testbed` subcommand of the `replica`
binary; every invocation takes `--settings-path` pointing at the testbed settings file described
below.

## 1. Cloud Provider Credentials

The orchestrator creates, starts, stops, and destroys instances on your behalf, so it needs
programmatic access to your cloud account.

Create the file `~/.aws/credentials` with your [access key ID and secret access
key](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-creds):

```text
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
```

Do not specify an AWS region in this file — the orchestrator manages multiple regions
programmatically.

## 2. Testbed Configuration

Create a `settings.yml` describing the testbed and pass its path to every command via
`--settings-path`. A minimal starting point, copied from
[`assets/settings-aws-template.yml`](../crates/orchestrator/assets/settings-aws-template.yml)
(see [`assets/settings-custom-template.yml`](../crates/orchestrator/assets/settings-custom-template.yml)
for the custom, bring-your-own-machines provider):

```yaml
testbed_id: "${USER}-mysticeti"
cloud_provider: !aws
  specs: m5d.8xlarge
  token_file: "/Users/${USER}/.aws/credentials"
ssh_private_key_file: "/Users/${USER}/.ssh/aws"
regions:
  - us-west-1
  - eu-west-1
repository:
  url: https://github.com/asonnino/mysticeti.git
  commit: main
```

`cloud_provider` is a tagged enum: serde_yaml expects the YAML tag form
(`!aws` / `!custom`) with the provider-specific fields nested beneath it, not a
plain string or a `aws:`/`custom:` map.

Every field in [`Settings`](../crates/orchestrator/src/settings.rs) is documented in its doc
comments. The required fields are shown above; the rest have sensible defaults. Notable optional
overrides include `node_parameters_path` and `client_parameters_path` (see
[`assets/node-parameters.yml`](../crates/orchestrator/assets/node-parameters.yml) and
[`assets/client-parameters.yml`](../crates/orchestrator/assets/client-parameters.yml) for examples)
and `benchmark_duration` (`0` runs indefinitely).

If the repository is private, embed a [GitHub personal access
token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens)
in the URL:

```yaml
repository:
  url: https://YOUR_ACCESS_TOKEN@github.com/asonnino/mysticeti.git
  commit: main
```

## 3. Managing the Testbed

The `remote-testbed` subcommands handle the lifecycle of cloud instances:

```bash
# Create N instances in each region listed in settings.yml.
cargo run remote-testbed --settings-path settings.yml create --instances 2

# Show the current state of all instances (green = up, red = stopped)
# and the SSH commands to reach them.
cargo run remote-testbed --settings-path settings.yml status

# Start up to N instances per region on an existing testbed.
cargo run remote-testbed --settings-path settings.yml start --instances 10

# Stop all instances (preserves disks; start again to resume).
cargo run remote-testbed --settings-path settings.yml stop

# Terminate all instances and release resources.
cargo run remote-testbed --settings-path settings.yml destroy
```

`create` also accepts `--region` to target a single region.

## 4. Running Benchmarks

Once the testbed is up, the `benchmark` subcommand installs the codebase on the remote instances,
starts one replica (and, by default, one load generator) per instance, and collects performance
measurements by scraping the Prometheus metrics exposed on each node.

```bash
# Benchmark a committee of 10 replicas under a 200 tx/s load.
cargo run remote-testbed --settings-path settings.yml benchmark --committee 10 --loads 200
```

`--loads` accepts a comma-separated list (e.g. `--loads 100,200,300`); the orchestrator runs one
benchmark per load and saves each as its own measurements file under `results_dir` (default:
`./results`). A load of `0` is special: the
orchestrator boots the replicas without any load generator, which is useful for testing with
external clients.

Two flags exist for iterative debugging: `--skip-testbed-update` skips pulling the latest commit,
and `--skip-testbed-configuration` skips rewriting config files on the instances. Both are unsafe in
general — use them only when you know the testbed state is already correct.

## 5. Faults

The `faults` field of `settings.yml` controls how the orchestrator injects replica failures during a
benchmark. Two modes are available:

```yaml
# Permanently crash a fixed number of replicas from the start.
faults: !Permanent
  faults: 1

# Progressively crash and recover up to `max_faults` replicas,
# taking one action every `interval` seconds.
faults: !CrashRecovery
  max_faults: 2
  interval:
    secs: 60
    nanos: 0
```

Permanent faults are useful for measuring steady-state throughput under static failures.
Crash-recovery exercises the protocol's behaviour around recovery transitions; the crash/recovery
schedule is reported in the benchmark summary.

`crash_order` controls which replicas the schedule crashes first. It applies to both modes:

```yaml
# Default: crash replicas in selection order. Nodes are picked round-robin across
# `regions`, so successive crashes cycle through the regions.
crash_order: round-robin

# Crash region by region, in the order of `regions`: the first-listed region is
# exhausted before the second is touched, and the last-listed region (typically the
# most remote one) survives as long as possible.
crash_order: region-order
```

The chosen order is recorded in the `parameters` block of each measurements file.

## 6. Monitoring

When `monitoring: true` (the default), the orchestrator deploys a
[Prometheus](https://prometheus.io) + [Grafana](https://grafana.com) stack on a dedicated instance.
Its address is printed on stdout when the benchmark starts (for example `http://3.83.97.12:3000`).
Log in with `admin` / `admin`, then either build a [new
dashboard](https://grafana.com/docs/grafana/latest/getting-started/build-first-dashboard/) or
[import](https://grafana.com/docs/grafana/latest/dashboards/manage-dashboards/#import-a-dashboard)
the example at
[`crates/orchestrator/assets/grafana-dashboard.json`](../crates/orchestrator/assets/grafana-dashboard.json).

The scrape interval is controlled by `scrape_interval` (default 15s). If `log_processing: true`, the
orchestrator also downloads per-instance log files to `logs_dir` after each run.

## 7. Inspecting Results

At the end of a run, the printed summary table reports each benchmark's name, node count, duration,
and consistency outcome. The detailed performance data — every Prometheus sample collected during
the run, including throughput rates and latency percentiles — is saved as a
YAML measurements collection under `results_dir` (one `measurements-<parameters>.yaml` file per
benchmark), keyed by metric name with the full label map of each sample for post-hoc filtering.

The replica metrics collected on every scrape are:

- `benchmark_duration`: seconds since the replica started, advanced only while transactions
  commit.
- `latency_s` (p50/p90/p99, `_count`, `_sum`) and `latency_squared_s`: submission-to-commit
  latency of every committed transaction, measured against the timestamp the load generator
  embeds in each transaction. Includes queuing until the transaction is included in a block.
- `block_latency_s` (p50/p90/p99, `_count`, `_sum`) and `block_latency_squared_s`:
  proposal-to-commit latency of every committed block, labelled `kind=leader` for the sub-DAG's
  leader and `kind=non-leader` otherwise. Computed from the proposer's block timestamp, so it is
  subject to cross-replica clock skew (a few milliseconds under NTP).
- `committed_leaders_total`: decided leaders per `authority` and `commit_type` (`fast-commit`,
  `slow-commit`, `indirect-commit-certificate`, `indirect-commit-weak`, `direct-skip`,
  `indirect-skip`). Single-path protocols only ever emit `slow-commit` and
  `indirect-commit-certificate`.

Counters are stored as `rate(..[1m])`, so a sample's `value` is a per-second rate; the `_count`
and `_sum` rates give the mean, and the squared counters the standard deviation, when the
histogram buckets are too coarse.
