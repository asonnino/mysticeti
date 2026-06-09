// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use eyre::{Context, Result};
use orchestrator::{
    provider::{aws::AwsClient, custom::CustomClient},
    settings::{CloudProvider, Settings},
};
use tracing_subscriber::filter::LevelFilter;

use crate::{
    args::RemoteTestbedArgs, remote::RemoteTestbedDriver, terminal::stderr_supports_color,
    tracing::ReplicaTracing,
};

pub async fn remote_testbed(
    args: RemoteTestbedArgs,
    log_level: Option<LevelFilter>,
    log_file: Option<PathBuf>,
) -> Result<()> {
    // Match the local-testbed default: route INFO chatter to the log file when
    // one is configured, but keep stderr quiet (WARN) by default so the banner
    // and progress lines stay readable.
    let default_level = if log_file.is_some() {
        LevelFilter::INFO
    } else {
        LevelFilter::WARN
    };
    let level = log_level.unwrap_or(default_level);
    let _guard = ReplicaTracing::new(level).with_log_file(log_file).setup()?;

    let color = stderr_supports_color();
    let settings_path = args.settings_path.display().to_string();
    let settings = Settings::load(&settings_path).wrap_err("Failed to load settings")?;

    match &settings.cloud_provider {
        CloudProvider::Aws(_) => {
            let client = AwsClient::new(settings.clone()).await;
            RemoteTestbedDriver::new(settings, client, color)
                .await?
                .run(args.command)
                .await
        }
        CloudProvider::Custom(_) => {
            let client = CustomClient::new(settings.clone());
            RemoteTestbedDriver::new(settings, client, color)
                .await?
                .run(args.command)
                .await
        }
    }
}
