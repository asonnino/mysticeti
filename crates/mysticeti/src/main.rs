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
    /// Deploy a local testbed.
    Testbed {
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
        } => benchmark_genesis(ips, working_directory, node_parameters_path)?,
        Operation::Run {
            authority,
            committee_path,
            public_config_path,
            private_config_path,
            client_parameters_path,
        } => {
            run(
                authority,
                committee_path,
                public_config_path,
                private_config_path,
                client_parameters_path,
            )
            .await?
        }
        Operation::Testbed { committee_size } => testbed(committee_size).await?,
        Operation::DryRun {
            authority,
            committee_size,
        } => dryrun(authority, committee_size).await?,
    }

    Ok(())
}

fn benchmark_genesis(
    ips: Vec<IpAddr>,
    working_directory: PathBuf,
    node_parameters_path: Option<PathBuf>,
) -> Result<()> {
    tracing::info!("Generating benchmark genesis files");
    fs::create_dir_all(&working_directory).wrap_err(format!(
        "Failed to create directory '{}'",
        working_directory.display()
    ))?;

    // Generate the committee file.
    let committee_size = ips.len();
    let mut committee_path = working_directory.clone();
    committee_path.push(Committee::DEFAULT_FILENAME);
    Committee::new_for_benchmarks(committee_size)
        .print(&committee_path)
        .wrap_err("Failed to print committee file")?;
    tracing::info!("Generated committee file: {}", committee_path.display());

    // Generate the public node config file.
    let node_parameters = match node_parameters_path {
        Some(path) => NodeParameters::load(&path).wrap_err(format!(
            "Failed to load parameters file '{}'",
            path.display()
        ))?,
        None => NodeParameters::default(),
    };

    let node_public_config = NodePublicConfig::new_for_benchmarks(ips, Some(node_parameters));
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
        NodePrivateConfig::new_for_benchmarks(&working_directory, committee_size);
    for (i, private_config) in node_private_configs.into_iter().enumerate() {
        fs::create_dir_all(&private_config.storage_path)
            .expect("Failed to create storage directory");
        let path = working_directory.join(NodePrivateConfig::default_filename(i as AuthorityIndex));
        private_config
            .print(&path)
            .wrap_err("Failed to print private config file")?;
        tracing::info!("Generated private config file: {}", path.display());
    }

    Ok(())
}

// /// Generate all the genesis files required for benchmarks.
// fn benchmark_genesis(
//     ips: Vec<IpAddr>,
//     working_directory: PathBuf,
//     disable_pipeline: bool,
//     number_of_leaders: usize,
// ) -> Result<()> {
//     tracing::info!("Generating benchmark genesis files");
//     fs::create_dir_all(&working_directory).wrap_err(format!(
//         "Failed to create directory '{}'",
//         working_directory.display()
//     ))?;

//     let committee_size = ips.len();
//     let mut committee_path = working_directory.clone();
//     committee_path.push(Committee::DEFAULT_FILENAME);
//     Committee::new_for_benchmarks(committee_size)
//         .print(&committee_path)
//         .wrap_err("Failed to print committee file")?;
//     tracing::info!("Generated committee file: {}", committee_path.display());

//     let mut parameters_path = working_directory.clone();
//     parameters_path.push(ValidatorPublicParameters::DEFAULT_FILENAME);
//     ValidatorPublicParameters::new_for_tests(ips)
//         .with_pipeline(!disable_pipeline)
//         .with_number_of_leaders(number_of_leaders)
//         .print(&parameters_path)
//         .wrap_err("Failed to print parameters file")?;
//     tracing::info!(
//         "Generated (public) parameters file: {}",
//         parameters_path.display()
//     );

//     for i in 0..committee_size {
//         let mut path = working_directory.clone();
//         path.push(PrivateConfig::default_filename(i as AuthorityIndex));
//         let parent_directory = path.parent().unwrap();
//         fs::create_dir_all(parent_directory).wrap_err(format!(
//             "Failed to create directory '{}'",
//             parent_directory.display()
//         ))?;
//         PrivateConfig::new_for_benchmarks(parent_directory, i as AuthorityIndex)
//             .print(&path)
//             .wrap_err("Failed to print private config file")?;
//         tracing::info!("Generated private config file: {}", path.display());
//     }

//     Ok(())
// }

/// Boot a single validator node.
async fn run(
    authority: AuthorityIndex,
    committee_path: String,
    public_config_path: String,
    private_config_path: String,
    client_parameters_path: String,
) -> Result<()> {
    tracing::info!("Starting validator {authority}");

    let committee = Committee::load(&committee_path)
        .wrap_err(format!("Failed to load committee file '{committee_path}'"))?;
    let public_config = NodePublicConfig::load(&public_config_path).wrap_err(format!(
        "Failed to load parameters file '{public_config_path}'"
    ))?;
    let private_config = NodePrivateConfig::load(&private_config_path).wrap_err(format!(
        "Failed to load private configuration file '{private_config_path}'"
    ))?;
    let client_parameters = ClientParameters::load(&client_parameters_path).wrap_err(format!(
        "Failed to load client parameters file '{client_parameters_path}'"
    ))?;

    let committee = Arc::new(committee);

    let network_address = public_config
        .network_address(authority)
        .ok_or(eyre!("No network address for authority {authority}"))
        .wrap_err("Unknown authority")?;
    let mut binding_network_address = network_address;
    binding_network_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

    let metrics_address = public_config
        .metrics_address(authority)
        .ok_or(eyre!("No metrics address for authority {authority}"))
        .wrap_err("Unknown authority")?;
    let mut binding_metrics_address = metrics_address;
    binding_metrics_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

    // Boot the validator node.
    let validator = Validator::start(
        authority,
        committee,
        &public_config,
        private_config,
        client_parameters,
    )
    .await?;
    let (network_result, _metrics_result) = validator.await_completion().await;
    network_result.expect("Validator crashed");
    Ok(())
}

async fn testbed(committee_size: usize) -> Result<()> {
    tracing::info!("Starting testbed with committee size {committee_size}");

    todo!();

    // let committee = Committee::new_for_benchmarks(committee_size);
    // let public_config = NodePublicConfig::new_for_tests(committee_size);
    // let client_parameters = ClientParameters::default();

    // let dir = PathBuf::from("local-testbed");
    // match fs::remove_dir_all(&dir) {
    //     Ok(_) => {}
    //     Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
    //     Err(e) => {
    //         return Err(e).wrap_err(format!("Failed to remove directory '{}'", dir.display()))
    //     }
    // }
    // match fs::create_dir_all(&dir) {
    //     Ok(_) => {}
    //     Err(e) => {
    //         return Err(e).wrap_err(format!("Failed to create directory '{}'", dir.display()))
    //     }
    // }

    // let mut handles = Vec::new();
    // for i in 0..committee_size {
    //     let authority = i as AuthorityIndex;
    //     let private_config = NodePrivateConfig::new_for_benchmarks(&dir, authority);

    //     let validator = Validator::start(
    //         authority,
    //         committee.clone(),
    //         &public_config,
    //         private_config,
    //         client_parameters.clone(),
    //     )
    //     .await?;
    //     handles.push(validator.await_completion());
    // }

    // future::join_all(handles).await;
    // Ok(())
}

async fn dryrun(authority: AuthorityIndex, committee_size: usize) -> Result<()> {
    tracing::warn!(
        "Starting validator {authority} in dryrun mode (committee size: {committee_size})"
    );

    todo!()

    // let committee = Committee::new_for_benchmarks(committee_size);
    // let public_config = NodePublicConfig::new_for_tests(committee_size);
    // let client_parameters = ClientParameters::default();

    // let dir = PathBuf::from(format!("dryrun-validator-{authority}"));
    // match fs::remove_dir_all(&dir) {
    //     Ok(_) => {}
    //     Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
    //     Err(e) => {
    //         return Err(e).wrap_err(format!("Failed to remove directory '{}'", dir.display()))
    //     }
    // }
    // match fs::create_dir_all(&dir) {
    //     Ok(_) => {}
    //     Err(e) => {
    //         return Err(e).wrap_err(format!("Failed to create directory '{}'", dir.display()))
    //     }
    // }
    // let private_config = NodePrivateConfig::new_for_benchmarks(&dir, authority);

    // Validator::start(
    //     authority,
    //     committee.clone(),
    //     &public_config,
    //     private_config,
    //     client_parameters,
    // )
    // .await?
    // .await_completion()
    // .await
    // .0
    // .expect("Validator failed");
}
