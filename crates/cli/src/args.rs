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
    TestGenesis {
        /// IP addresses of all replicas.
        #[arg(long, value_name = "ADDR", value_delimiter = ' ', num_args(3..))]
        ips: Vec<IpAddr>,
        /// Working directory where files will be generated.
        #[arg(long, value_name = "DIR", default_value = "genesis")]
        working_directory: PathBuf,
        /// Path to custom replica parameters (YAML). Uses defaults if omitted.
        #[arg(long, value_name = "FILE")]
        replica_parameters_path: Option<PathBuf>,
    },

    /// Run a single replica from config files.
    Run {
        /// Authority index of this node.
        #[arg(long, value_name = "INT")]
        authority: Authority,
        /// Path to the public replica config file (YAML: identities, stakes, and parameters).
        /// The committee is derived from this file's identifiers + stakes.
        #[arg(long, value_name = "FILE")]
        public_config_path: String,
        /// Path to the private replica config file (YAML, includes keys).
        #[arg(long, value_name = "FILE")]
        private_config_path: String,
        /// Path to the load generator config file (YAML). Omit to run without the built-in load
        /// generator and expose the transaction channel for external submission instead.
        #[arg(long, value_name = "FILE")]
        load_generator_config_path: Option<String>,
    },

    /// Run a simulated network from a YAML config file.
    Simulate {
        /// Path to the simulation config (YAML). Uses defaults if omitted.
        #[arg(long, value_name = "FILE", conflicts_with = "dump_config")]
        config_path: Option<PathBuf>,
        /// Print the default configuration to stdout and exit.
        #[arg(long, conflicts_with = "config_path")]
        dump_config: bool,
    },

    /// Deploy a local testbed of replicas on localhost.
    ///
    /// Starts all replicas in a single process with default keys and committee configuration.
    /// Useful for local testing.
    LocalTestbed {
        /// Number of replicas in the testbed.
        #[arg(long, value_name = "INT", default_value_t = 4)]
        committee_size: usize,
        /// Path to custom replica parameters (YAML). Uses defaults if omitted.
        #[arg(long, value_name = "FILE")]
        replica_parameters_path: Option<PathBuf>,
        /// Path to custom load generator config (YAML). Uses defaults if omitted.
        #[arg(long, value_name = "FILE")]
        load_generator_config_path: Option<PathBuf>,
    },

    /// Print the startup banner and exit.
    PrintBanner,
}
