# Geo-Replicated Testbeds

The `orchestrator` crate provides facilities for deploying the codebase on a set of cloud instances
and running benchmarks against the deployed network.

The instructions below walk through a full run on [Amazon Web Services
(AWS)](https://aws.amazon.com), which is the primary supported target. Additional cloud providers
can be added by implementing the [`ServerProviderClient`](../crates/orchestrator/src/client/mod.rs)
trait.

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

Create a `settings.yml` describing the testbed. The orchestrator reads this file (from
`crates/orchestrator/assets/settings.yml` by default, or from any path passed via
`--settings-path`). A minimal starting point, copied from
[`assets/settings-template.yml`](../crates/orchestrator/assets/settings-template.yml):

```yaml
testbed_id: "${USER}-mysticeti"
cloud_provider: aws
token_file: "/Users/${USER}/.aws/credentials"
ssh_private_key_file: "/Users/${USER}/.ssh/aws"
regions:
  - us-west-1
  - eu-west-1
specs: m5d.8xlarge
repository:
  url: https://github.com/asonnino/mysticeti.git
  commit: main
```

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

The `testbed` subcommand group handles the lifecycle of cloud instances:

```bash
# Create N instances in each region listed in settings.yml.
cargo run --bin orchestrator -- testbed deploy --instances 2

# Show the current state of all instances (green = up, red = stopped).
cargo run --bin orchestrator -- testbed status

# Start up to N instances per region on an existing testbed.
cargo run --bin orchestrator -- testbed start --instances 10

# Stop all instances (preserves disks; start again to resume).
cargo run --bin orchestrator -- testbed stop

# Terminate all instances and release resources.
cargo run --bin orchestrator -- testbed destroy
```

`deploy` also accepts `--region` to target a single region.

## 4. Running Benchmarks

Once the testbed is up, the `benchmark` subcommand installs the codebase on the remote instances,
starts one replica (and, by default, one load generator) per instance, and collects performance
measurements by scraping the Prometheus metrics exposed on each node.

```bash
# Benchmark a committee of 10 replicas under a 200 tx/s load.
cargo run --bin orchestrator -- benchmark --committee 10 --loads 200
```

`--loads` accepts multiple values; the orchestrator runs one benchmark per load and saves each as
its own measurements file under `results_dir` (default: `./results`). A load of `0` is special: the
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

## 7. Inspecting Past Results

Each benchmark saves a JSON measurements collection under `results_dir`. To re-print a summary for a
saved run:

```bash
cargo run --bin orchestrator -- summarize --path ./results/measurements-...json
```

The summary reports TPS, average latency, and latency standard deviation per workload, together with
the node count, fault configuration, offered load, and run duration.
