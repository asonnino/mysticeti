// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    sync::Arc,
};

use clap::{command, Parser};
use eyre::{eyre, Context, Result};
use mysticeti_core::{
    aux_node::{
        aux_config::{AuxNodeParameters, AuxNodePublicConfig, AuxiliaryCommittee},
        aux_validator::AuxiliaryValidator,
    },
    committee::Committee,
    config::{ClientParameters, ImportExport, NodeParameters, NodePrivateConfig, NodePublicConfig},
    types::AuthorityIndex,
    validator::Validator,
};
use tracing_subscriber::{filter::LevelFilter, fmt, EnvFilter};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(subcommand)]
    operation: Operation,
}

#[derive(Parser)]
enum Operation {
    /// Generate a committee file, parameters files and the private config files of all validators
    /// from a list of initial peers. This is only suitable for benchmarks as it exposes all keys.
    BenchmarkGenesis {
        /// The list of ip addresses of the all validators.
        #[clap(long, value_name = "ADDR", value_delimiter = ' ', num_args(4..))]
        ips: Vec<IpAddr>,
        /// The working directory where the files will be generated.
        #[clap(long, value_name = "FILE", default_value = "genesis")]
        working_directory: PathBuf,
        /// Path to the file holding the node parameters. If not provided, default parameters are used.
        #[clap(long, value_name = "FILE")]
        node_parameters_path: Option<PathBuf>,
        /// The number of auxiliary validators. Should be less than the number of ips.
        #[clap(long, value_name = "INT", default_value = "0")]
        aux_committee_size: usize,
        /// Path to the file holding the aux node parameters. If not provided, default parameters are used.
        #[clap(long, value_name = "FILE")]
        aux_node_parameters_path: Option<PathBuf>,
    },
    /// Run a validator node.
    Run {
        /// The authority index of this node.
        #[clap(long, value_name = "INT")]
        authority: AuthorityIndex,
        /// Path to the file holding the public committee information.
        #[clap(long, value_name = "FILE")]
        committee_path: String,
        /// Path to the file holding the public validator configurations (such as network addresses).
        #[clap(long, value_name = "FILE")]
        public_config_path: String,
        /// Path to the file holding the private validator configurations (including keys).
        #[clap(long, value_name = "FILE")]
        private_config_path: String,
        /// Path to the file holding the client parameters (for benchmarks).
        #[clap(long, value_name = "FILE")]
        client_parameters_path: String,

        /// Path to the file holding the public aux committee information.
        #[clap(long, value_name = "FILE")]
        aux_committee_path: Option<String>,
        /// Path to the file holding the public aux validator configurations (such as network addresses).
        #[clap(long, value_name = "FILE")]
        aux_public_config_path: Option<String>,
    },
    /// Deploy a local validator for test. Dryrun mode uses default keys and committee configurations.
    DryRun {
        /// The authority index of this node.
        #[clap(long, value_name = "INT")]
        authority: AuthorityIndex,
        /// The number of authorities in the committee.
        #[clap(long, value_name = "INT")]
        committee_size: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Nice colored error messages.
    color_eyre::install()?;
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    fmt().with_env_filter(filter).init();

    // Parse the command line arguments.
    match Args::parse().operation {
        Operation::BenchmarkGenesis {
            ips,
            working_directory,
            node_parameters_path,
            aux_committee_size,
            aux_node_parameters_path,
        } => benchmark_genesis(
            ips,
            working_directory,
            node_parameters_path,
            aux_committee_size,
            aux_node_parameters_path,
        )?,
        Operation::Run {
            authority,
            committee_path,
            aux_committee_path,
            public_config_path,
            private_config_path,
            client_parameters_path,
            aux_public_config_path,
        } => {
            run(
                authority,
                committee_path,
                aux_committee_path,
                public_config_path,
                private_config_path,
                client_parameters_path,
                aux_public_config_path,
            )
            .await?
        }
        Operation::DryRun {
            authority,
            committee_size,
        } => dryrun(authority, committee_size).await?,
    }

    Ok(())
}

fn benchmark_genesis(
    mut ips: Vec<IpAddr>,
    working_directory: PathBuf,
    node_parameters_path: Option<PathBuf>,
    aux_committee_size: usize,
    aux_node_parameters_path: Option<PathBuf>,
) -> Result<()> {
    tracing::info!("Generating benchmark genesis files");
    fs::create_dir_all(&working_directory).wrap_err(format!(
        "Failed to create directory '{}'",
        working_directory.display()
    ))?;

    assert!(!ips.is_empty(), "At least one IP address is required");
    assert!(
        aux_committee_size < ips.len(),
        "Too many auxiliary validators"
    );

    let core_ips = ips
        .drain(0..ips.len() - aux_committee_size)
        .collect::<Vec<_>>();
    tracing::info!("Core committee IPs: {:?}", core_ips);
    let aux_ips = ips;
    tracing::info!("Auxiliary committee IPs: {:?}", aux_ips);

    // Generate the core committee file.
    let core_committee_size = core_ips.len();
    let mut core_committee_path = working_directory.clone();
    core_committee_path.push(Committee::DEFAULT_FILENAME);
    Committee::new_for_benchmarks(core_committee_size)
        .print(&core_committee_path)
        .wrap_err("Failed to print committee file")?;
    tracing::info!(
        "Generated committee file: {}",
        core_committee_path.display()
    );

    // Generate the public node config file.
    let node_parameters = match node_parameters_path {
        Some(path) => NodeParameters::load(&path).wrap_err(format!(
            "Failed to load parameters file '{}'",
            path.display()
        ))?,
        None => NodeParameters::default(),
    };

    let node_public_config = NodePublicConfig::new_for_benchmarks(core_ips, Some(node_parameters));
    let mut node_public_config_path = working_directory.clone();
    node_public_config_path.push(NodePublicConfig::DEFAULT_FILENAME);
    node_public_config
        .print(&node_public_config_path)
        .wrap_err("Failed to print parameters file")?;
    tracing::info!(
        "Generated public node config file: {}",
        node_public_config_path.display()
    );

    // Generate the private node config files.
    let node_private_configs =
        NodePrivateConfig::new_for_benchmarks(&working_directory, core_committee_size);
    for (i, private_config) in node_private_configs.into_iter().enumerate() {
        fs::create_dir_all(&private_config.storage_path)
            .expect("Failed to create storage directory");
        let path = working_directory.join(NodePrivateConfig::default_filename(i as AuthorityIndex));
        private_config
            .print(&path)
            .wrap_err("Failed to print private config file")?;
        tracing::info!("Generated private config file: {}", path.display());
    }

    // Generate the aux committee file.
    let mut aux_committee_path = working_directory.clone();
    aux_committee_path.push(AuxiliaryCommittee::DEFAULT_FILENAME);
    AuxiliaryCommittee::new_for_benchmarks(aux_committee_size)
        .print(&aux_committee_path)
        .wrap_err("Failed to print aux committee file")?;
    tracing::info!(
        "Generated aux committee file: {}",
        aux_committee_path.display()
    );

    // Generate the public node config file.
    let aux_node_parameters = match aux_node_parameters_path {
        Some(path) => AuxNodeParameters::load(&path).wrap_err(format!(
            "Failed to load parameters file '{}'",
            path.display()
        ))?,
        None => AuxNodeParameters::default(),
    };

    let aux_node_public_config =
        AuxNodePublicConfig::new_for_benchmarks(aux_ips, Some(aux_node_parameters));
    let mut aux_node_public_config_path = working_directory.clone();
    aux_node_public_config_path.push(AuxNodePublicConfig::DEFAULT_FILENAME);
    aux_node_public_config
        .print(&aux_node_public_config_path)
        .wrap_err("Failed to print aux parameters file")?;
    tracing::info!(
        "Generated public aux node config file: {}",
        aux_node_public_config_path.display()
    );

    // Generate the aux private node config files.
    let aux_node_private_configs = NodePrivateConfig::new_for_benchmarks_with_offset(
        &working_directory,
        aux_committee_size,
        AuxiliaryCommittee::AUX_AUTHORITY_INDEX_OFFSET,
    );
    for (i, private_config) in aux_node_private_configs.into_iter().enumerate() {
        fs::create_dir_all(&private_config.storage_path)
            .expect("Failed to create storage directory");
        let path = working_directory.join(NodePrivateConfig::default_filename(
            (i + AuxiliaryCommittee::AUX_AUTHORITY_INDEX_OFFSET) as AuthorityIndex,
        ));
        private_config
            .print(&path)
            .wrap_err("Failed to print aux private config file")?;
        tracing::info!("Generated aux private config file: {}", path.display());
    }

    Ok(())
}

/// Boot a single validator node.
async fn run(
    authority: AuthorityIndex,
    core_committee_path: String,
    aux_committee_path: Option<String>,
    public_config_path: String,
    private_config_path: String,
    client_parameters_path: String,
    aux_public_config_path: Option<String>,
) -> Result<()> {
    tracing::info!("Starting validator {authority}");

    let core_committee = Committee::load(&core_committee_path).wrap_err(format!(
        "Failed to load committee file '{core_committee_path}'"
    ))?;
    let core_committee = Arc::new(core_committee);

    let public_config = NodePublicConfig::load(&public_config_path).wrap_err(format!(
        "Failed to load parameters file '{public_config_path}'"
    ))?;
    let private_config = NodePrivateConfig::load(&private_config_path).wrap_err(format!(
        "Failed to load private configuration file '{private_config_path}'"
    ))?;
    let client_parameters = ClientParameters::load(&client_parameters_path).wrap_err(format!(
        "Failed to load client parameters file '{client_parameters_path}'"
    ))?;

    let aux_committee = match aux_committee_path {
        Some(path) => Arc::new(
            AuxiliaryCommittee::load(&path)
                .wrap_err(format!("Failed to load aux committee file '{path}'"))?,
        ),
        None => AuxiliaryCommittee::new_for_benchmarks(0),
    };
    let aux_public_config = match aux_public_config_path {
        Some(path) => AuxNodePublicConfig::load(&path)
            .wrap_err(format!("Failed to load aux parameters file '{path}'"))?,
        None => AuxNodePublicConfig::new_for_tests(0),
    };

    // Boot the validator node.
    if core_committee.known_authority(authority) {
        let validator = Validator::start(
            authority,
            core_committee,
            aux_committee,
            public_config.clone(),
            private_config,
            client_parameters,
            aux_public_config,
        )
        .await?;
        let (network_result, _metrics_result) = validator.await_completion().await;
        network_result.expect("Validator crashed");
    } else if aux_committee.exists(authority) {
        let validator = AuxiliaryValidator::start(
            authority,
            core_committee,
            public_config,
            private_config,
            client_parameters,
            aux_public_config,
        )
        .await?;
        let result = validator.await_completion().await?;
        result.expect("Auxiliary Validator crashed");
    } else {
        return Err(eyre!("Unknown authority {authority}"));
    }

    Ok(())
}

async fn dryrun(authority: AuthorityIndex, committee_size: usize) -> Result<()> {
    tracing::warn!(
        "Starting validator {authority} in dryrun mode (committee size: {committee_size})"
    );
    let ips = vec![IpAddr::V4(Ipv4Addr::LOCALHOST); committee_size];
    let committee = Committee::new_for_benchmarks(committee_size);
    let client_parameters = ClientParameters::default();
    let node_parameters = NodeParameters::default();
    let public_config = NodePublicConfig::new_for_benchmarks(ips, Some(node_parameters));

    let aux_committee = AuxiliaryCommittee::new_for_benchmarks(0);
    let aux_public_config = AuxNodePublicConfig::new_for_tests(0);

    let working_dir = PathBuf::from(format!("dryrun-validator-{authority}"));
    let mut all_private_config =
        NodePrivateConfig::new_for_benchmarks(&working_dir, committee_size);
    let private_config = all_private_config.remove(authority as usize);
    match fs::remove_dir_all(&working_dir) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(e).wrap_err(format!(
                "Failed to remove directory '{}'",
                working_dir.display()
            ))
        }
    }
    match fs::create_dir_all(&private_config.storage_path) {
        Ok(_) => {}
        Err(e) => {
            return Err(e).wrap_err(format!(
                "Failed to create directory '{}'",
                working_dir.display()
            ))
        }
    }

    let validator = Validator::start(
        authority,
        committee,
        aux_committee,
        public_config,
        private_config,
        client_parameters,
        aux_public_config,
    )
    .await?;
    let (network_result, _metrics_result) = validator.await_completion().await;
    network_result.expect("Validator crashed");

    Ok(())
}
