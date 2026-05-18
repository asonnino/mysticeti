// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Display;

use serde::Serialize;

use crate::{
    error::{CloudProviderError, CloudProviderResult},
    provider::{Instance, InstanceStatus, ServerProviderClient},
    settings::{CloudProvider, CustomConfig, Settings},
};

/// A client for the custom "manual" provider. The user is expected to
/// provision the machines outside of the orchestrator and only provide their
/// IP addresses through the settings file. As a consequence, every method
/// that would normally create, destroy, start, or stop instances returns a
/// [`CloudProviderError::Unsupported`] error — the orchestrator's CLI surfaces
/// that error verbatim so the user understands why their action did nothing.
pub struct CustomClient {
    instances: Vec<Instance>,
    ssh_username: String,
}

impl CustomClient {
    /// Panics if `settings.cloud_provider` is not [`CloudProvider::Custom`];
    /// the caller must match the variant first.
    pub fn new(settings: Settings) -> Self {
        let CloudProvider::Custom(CustomConfig {
            ssh_username,
            instances: machines,
        }) = settings.cloud_provider
        else {
            panic!("CustomClient::new called with a non-Custom cloud_provider");
        };

        let instances = machines
            .into_iter()
            .enumerate()
            .map(|(i, machine)| Instance {
                id: format!("custom-{i}"),
                region: machine.region,
                main_ip: machine.ip,
                tags: vec![settings.testbed_id.clone()],
                // Custom machines have no provider-managed instance type; the
                // user pre-provisions them. We surface a placeholder so the
                // status table reads coherently.
                specs: "custom".into(),
                status: InstanceStatus::Active,
            })
            .collect();

        Self {
            instances,
            ssh_username,
        }
    }
}

impl Display for CustomClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Custom ({} instances)", self.instances.len())
    }
}

impl ServerProviderClient for CustomClient {
    fn username(&self) -> &str {
        &self.ssh_username
    }

    async fn list_instances(&self) -> CloudProviderResult<Vec<Instance>> {
        Ok(self.instances.clone())
    }

    async fn start_instances<'a, I>(&self, _instances: I) -> CloudProviderResult<()>
    where
        I: Iterator<Item = &'a Instance> + Send,
    {
        Err(CloudProviderError::Unsupported(
            "the custom provider cannot start instances; the user manages them manually".into(),
        ))
    }

    async fn stop_instances<'a, I>(&self, _instances: I) -> CloudProviderResult<()>
    where
        I: Iterator<Item = &'a Instance> + Send,
    {
        Err(CloudProviderError::Unsupported(
            "the custom provider cannot stop instances; the user manages them manually".into(),
        ))
    }

    async fn create_instance<S>(&self, _region: S) -> CloudProviderResult<Instance>
    where
        S: Into<String> + Serialize + Send,
    {
        Err(CloudProviderError::Unsupported(
            "the custom provider cannot create instances; provision them manually and list \
                their IPs in the settings file"
                .into(),
        ))
    }

    async fn delete_instance(&self, _instance: Instance) -> CloudProviderResult<()> {
        Err(CloudProviderError::Unsupported(
            "the custom provider cannot delete instances; destroy them manually".into(),
        ))
    }

    async fn register_ssh_public_key(&self, _public_key: String) -> CloudProviderResult<()> {
        // SSH key registration is the user's responsibility on a custom testbed.
        // This silently succeeds so that `Testbed::new` (which always calls it)
        // works for every command, not just lifecycle ones.
        Ok(())
    }

    async fn instance_setup_commands(&self) -> CloudProviderResult<Vec<String>> {
        Ok(Vec::new())
    }
}
