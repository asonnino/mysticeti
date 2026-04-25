// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::IpAddr, path::PathBuf};

use clap::Parser;
use dag::authority::Authority;
use tracing_subscriber::filter::LevelFilter;

/// Mysticeti consensus replica.
#[derive(Parser)]
#[command(author, version, propagate_version = true)]
pub struct Args {
    /// Log level (trace, debug, info, warn, error). Overrides the per-command default.
    /// RUST_LOG env var takes precedence over this.
    #[arg(long, global = true)]
    pub log_level: Option<LevelFilter>,

    /// Write logs to this file instead of stderr.
    #[arg(long, global = true, value_name = "FILE")]
    pub log_file: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Parser)]
pub enum Command {
    /// Generate test genesis files: one public replica config (identities, stakes, and parameters)
    /// plus a private config per replica (keys and storage paths). Keys are written in plaintext.
    TestGenesis(TestGenesisArgs),

    /// Run a single replica from config files.
    Run(RunArgs),

    /// Run a simulated network from a YAML config file.
    Simulate(SimulateArgs),

    /// Deploy a local testbed of replicas on localhost.
    ///
    /// Starts all replicas in a single process with default keys and committee configuration.
    /// Useful for local testing.
    LocalTestbed(LocalTestbedArgs),

    /// Print the startup banner and exit.
    PrintBanner,
}

#[derive(clap::Args)]
pub struct TestGenesisArgs {
    /// IP addresses of all replicas.
    #[arg(long, value_name = "ADDR", value_delimiter = ' ', num_args(3..))]
    pub ips: Vec<IpAddr>,
    /// Working directory where files will be generated.
    #[arg(long, value_name = "DIR", default_value = "genesis")]
    pub working_directory: PathBuf,
    /// Path to custom replica parameters (YAML). Uses defaults if omitted.
    #[arg(long, value_name = "FILE")]
    pub replica_parameters_path: Option<PathBuf>,
}

#[derive(clap::Args)]
pub struct RunArgs {
    /// Authority index of this node.
    #[arg(long, value_name = "INT")]
    pub authority: Authority,
    /// Path to the public replica config file (YAML: identities, stakes, and parameters).
    /// The committee is derived from this file's identifiers + stakes.
    #[arg(long, value_name = "FILE")]
    pub public_config_path: String,
    /// Path to the private replica config file (YAML, includes keys).
    #[arg(long, value_name = "FILE")]
    pub private_config_path: String,
    /// Path to the load generator config file (YAML). Omit to run without the built-in load
    /// generator and expose the transaction channel for external submission instead.
    #[arg(long, value_name = "FILE")]
    pub load_generator_config_path: Option<String>,
}

#[derive(clap::Args)]
pub struct SimulateArgs {
    /// Path to the simulation config (YAML). Uses defaults if omitted.
    #[arg(long, value_name = "FILE", conflicts_with = "dump_config")]
    pub config_path: Option<PathBuf>,
    /// Print the default configuration to stdout and exit.
    #[arg(long, conflicts_with = "config_path")]
    pub dump_config: bool,
    /// Directory to collect tracing logs and per-run artefacts (`config.yaml`,
    /// `meta.yaml`, `metrics.prom`). Multi-run suites get one subdirectory per run
    /// (named after the run, or by index if unnamed).
    #[arg(long, value_name = "DIR", conflicts_with = "dump_config")]
    pub output_dir: Option<PathBuf>,
    /// Also write each run's committed sub-DAG to `<output_dir>/<run>/dag.ndjson`
    /// (one committed sub-DAG per line). Requires `--output-dir`. Off by default —
    /// DAG dumps can be many GB.
    #[arg(long, conflicts_with = "dump_config", requires = "output_dir")]
    pub export_dag: bool,
}

#[derive(clap::Args)]
pub struct LocalTestbedArgs {
    /// Number of replicas in the testbed.
    #[arg(long, value_name = "INT", default_value_t = 4)]
    pub committee_size: usize,
    /// Path to custom replica parameters (YAML). Uses defaults if omitted.
    #[arg(long, value_name = "FILE")]
    pub replica_parameters_path: Option<PathBuf>,
    /// Path to custom load generator config (YAML). Uses defaults if omitted.
    #[arg(long, value_name = "FILE")]
    pub load_generator_config_path: Option<PathBuf>,
    /// Run for this many seconds, then collect results and shut down. Ignored if
    /// `--perpetual` is set.
    #[arg(
        long,
        value_name = "SECS",
        default_value_t = 20,
        conflicts_with = "perpetual"
    )]
    pub duration: u64,
    /// Run forever; collect results and shut down on Ctrl-C. Sending Ctrl-C twice
    /// aborts immediately without a summary.
    #[arg(long, conflicts_with = "duration")]
    pub perpetual: bool,
    /// Heartbeat cadence, in seconds, used in `--perpetual` mode to print live
    /// aggregated stats to stderr. No effect under `--duration`.
    #[arg(long, value_name = "SECS", default_value_t = 5)]
    pub heartbeat_interval: u64,
    /// Directory for tracing logs and run artefacts (`config.yaml`, `meta.yaml`,
    /// `metrics.prom`). Re-runs replace the artefacts atomically; replica WALs are
    /// kept in anonymous tempfiles, not written here. Omit to skip artefact export
    /// (banner / heartbeat / summary still print).
    #[arg(long, value_name = "DIR")]
    pub output_dir: Option<PathBuf>,
    /// Also write the committed sub-DAG to `<output_dir>/dag.ndjson` (one committed
    /// sub-DAG per line). Requires `--output-dir`. Off by default — DAG dumps can
    /// be many GB.
    #[arg(long, requires = "output_dir")]
    pub export_dag: bool,
}
