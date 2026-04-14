// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::IpAddr, path::PathBuf};

use clap::Parser;
use dag::types::AuthorityIndex;
use tracing_subscriber::filter::LevelFilter;

/// Mysticeti consensus replica.
#[derive(Parser)]
#[command(author, version, propagate_version = true)]
pub struct Args {
    /// Log level (trace, debug, info, warn, error). Overrides the
    /// per-command default. RUST_LOG env var takes precedence over this.
    #[arg(long, global = true)]
    pub log_level: Option<LevelFilter>,

    #[command(subcommand)]
    pub operation: Operation,
}

#[derive(Parser)]
pub enum Operation {
    /// Generate test genesis files (committee, public config, and
    /// private keys) for all validators. All keys are written to disk
    /// in plaintext — do NOT use in production.
    TestGenesis {
        /// IP addresses of all validators.
        #[arg(long, value_name = "ADDR", value_delimiter = ' ', num_args(4..))]
        ips: Vec<IpAddr>,
        /// Working directory where files will be generated.
        #[arg(long, value_name = "DIR", default_value = "genesis")]
        working_directory: PathBuf,
        /// Path to custom node parameters (YAML). Uses defaults if omitted.
        #[arg(long, value_name = "FILE")]
        node_parameters_path: Option<PathBuf>,
    },

    /// Run a single replica from config files.
    Run {
        /// Authority index of this node.
        #[arg(long, value_name = "INT")]
        authority: AuthorityIndex,
        /// Path to the committee file (YAML).
        #[arg(long, value_name = "FILE")]
        committee_path: String,
        /// Path to the public node config file (YAML).
        #[arg(long, value_name = "FILE")]
        public_config_path: String,
        /// Path to the private node config file (YAML, includes keys).
        #[arg(long, value_name = "FILE")]
        private_config_path: String,
        /// Path to the client parameters file (YAML).
        #[arg(long, value_name = "FILE")]
        client_parameters_path: String,
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

    /// Deploy a local testbed of validators on localhost.
    ///
    /// Starts all validators in a single process with default keys
    /// and committee configuration. Useful for local testing.
    LocalTestbed {
        /// Path to testbed config (YAML). Uses defaults if omitted.
        #[arg(long, value_name = "FILE", conflicts_with = "dump_config")]
        config_path: Option<PathBuf>,
        /// Print the default configuration to stdout and exit.
        #[arg(long, conflicts_with = "config_path")]
        dump_config: bool,
    },

    /// Print the startup banner and exit.
    PrintBanner,
}
