// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt::Display, time::Duration};

use futures::future::try_join_all;
use tokio::time;

use crate::{
    error::{TestbedError, TestbedResult},
    provider::{Instance, ServerProviderClient},
    settings::Settings,
    ssh::SshConnection,
};

/// Snapshot of the testbed handed back by [`Testbed::status`]. The consumer
/// renders it however it likes — the library no longer touches stdout.
pub struct TestbedStatus {
    /// Cloud-provider client summary (e.g. `"AWS EC2 client v1.x"` or
    /// `"Custom (N instances)"`).
    pub client_summary: String,
    /// Repository URL the testbed is wired to deploy from.
    pub repository_url: String,
    /// Commit pinned in [`Settings::repository`].
    pub repository_commit: String,
    /// Count of currently-running instances across all regions.
    pub active_count: usize,
    /// Per-region instance lists, in the order declared in [`Settings::regions`].
    pub regions: Vec<RegionStatus>,
}

pub struct RegionStatus {
    pub region: String,
    pub instances: Vec<InstanceEntry>,
}

pub struct InstanceEntry {
    /// Ready-to-paste SSH command for this instance.
    pub connect_command: String,
    /// Whether the cloud provider reports the instance as running.
    pub active: bool,
}

/// Represents a testbed running on a cloud provider.
pub struct Testbed<C> {
    /// The testbed's settings.
    settings: Settings,
    /// The client interfacing with the cloud provider.
    client: C,
    /// The state of the testbed (reflecting accurately the state of the machines).
    instances: Vec<Instance>,
}

impl<C: ServerProviderClient> Testbed<C> {
    /// Create a new testbed instance with the specified settings and client.
    pub async fn new(settings: Settings, client: C) -> TestbedResult<Self> {
        let public_key = settings.load_ssh_public_key()?;
        client.register_ssh_public_key(public_key).await?;
        let instances = client.list_instances().await?;

        Ok(Self {
            settings,
            client,
            instances,
        })
    }

    /// Return the username to connect to the instances through ssh.
    pub fn username(&self) -> &str {
        self.client.username()
    }

    /// Return the list of instances of the testbed.
    pub fn instances(&self) -> Vec<Instance> {
        self.instances
            .iter()
            .filter(|x| self.settings.filter_instance(x))
            .cloned()
            .collect()
    }

    /// Return the list of provider-specific instance setup commands.
    pub async fn setup_commands(&self) -> TestbedResult<Vec<String>> {
        self.client
            .instance_setup_commands()
            .await
            .map_err(TestbedError::from)
    }

    /// Snapshot the current state of the testbed for the caller to render.
    ///
    /// Skips terminated instances (they have no SSH endpoint) and preserves the
    /// region ordering declared in [`Settings::regions`] so the rendered output
    /// stays stable across calls.
    pub fn status(&self) -> TestbedStatus
    where
        C: Display,
    {
        let private_key_file = self.settings.ssh_private_key_file.display().to_string();
        let username = self.client.username();

        let regions = self
            .settings
            .regions
            .iter()
            .map(|region| {
                let instances = self
                    .instances
                    .iter()
                    .filter(|instance| {
                        self.settings.filter_instance(instance)
                            && &instance.region == region
                            && !instance.is_terminated()
                    })
                    .map(|instance| InstanceEntry {
                        connect_command: format!(
                            "ssh -i {private_key_file} {username}@{}",
                            instance.main_ip
                        ),
                        active: instance.is_active(),
                    })
                    .collect();
                RegionStatus {
                    region: region.clone(),
                    instances,
                }
            })
            .collect();

        let active_count = self
            .instances
            .iter()
            .filter(|instance| self.settings.filter_instance(instance) && instance.is_active())
            .count();

        TestbedStatus {
            client_summary: self.client.to_string(),
            repository_url: self.settings.repository.url.to_string(),
            repository_commit: self.settings.repository.commit.clone(),
            active_count,
            regions,
        }
    }

    /// Populate the testbed by creating the specified amount of instances per region. The total
    /// number of instances created is thus the specified amount x the number of regions.
    pub async fn create(&mut self, quantity: usize, region: Option<String>) -> TestbedResult<()> {
        let instances = match region {
            Some(x) => {
                try_join_all((0..quantity).map(|_| self.client.create_instance(x.clone()))).await?
            }
            None => {
                try_join_all(self.settings.regions.iter().flat_map(|region| {
                    (0..quantity).map(|_| self.client.create_instance(region.clone()))
                }))
                .await?
            }
        };

        // Wait until the instances are booted.
        if cfg!(not(test)) {
            self.wait_until_reachable(instances.iter()).await?;
        }
        self.instances = self.client.list_instances().await?;
        Ok(())
    }

    /// Destroy all instances of the testbed.
    pub async fn destroy(&mut self) -> TestbedResult<()> {
        try_join_all(
            self.instances
                .drain(..)
                .map(|instance| self.client.delete_instance(instance)),
        )
        .await?;
        Ok(())
    }

    /// Start the specified number of instances in each region. Returns an error if there are not
    /// enough available instances.
    pub async fn start(&mut self, quantity: usize) -> TestbedResult<()> {
        // Gather available instances.
        let mut available = Vec::new();
        for region in &self.settings.regions {
            available.extend(
                self.instances
                    .iter()
                    .filter(|x| {
                        x.is_inactive() && &x.region == region && self.settings.filter_instance(x)
                    })
                    .take(quantity)
                    .cloned()
                    .collect::<Vec<_>>(),
            );
        }

        // Start instances.
        self.client.start_instances(available.iter()).await?;

        // Wait until the instances are started.
        if cfg!(not(test)) {
            self.wait_until_reachable(available.iter()).await?;
        }
        self.instances = self.client.list_instances().await?;
        Ok(())
    }

    /// Stop all instances of the testbed.
    pub async fn stop(&mut self) -> TestbedResult<()> {
        // Stop all instances.
        self.client
            .stop_instances(self.instances.iter().filter(|i| i.is_active()))
            .await?;

        // Wait until the instances are stopped.
        loop {
            let instances = self.client.list_instances().await?;
            if instances.iter().all(|x| x.is_inactive()) {
                self.instances = instances;
                break;
            }
        }
        Ok(())
    }

    /// Wait until all specified instances are ready to accept ssh connections.
    async fn wait_until_reachable<'a, I>(&self, instances: I) -> TestbedResult<()>
    where
        I: Iterator<Item = &'a Instance> + Clone,
    {
        let instances_ids: Vec<_> = instances.map(|x| x.id.clone()).collect();

        let mut interval = time::interval(Duration::from_secs(5));
        interval.tick().await; // The first tick returns immediately.

        loop {
            interval.tick().await;
            let instances = self.client.list_instances().await?;
            let futures = instances
                .iter()
                .filter(|x| instances_ids.contains(&x.id))
                .map(|instance| {
                    let private_key_file = self.settings.ssh_private_key_file.clone();
                    SshConnection::new(
                        instance.ssh_address(),
                        self.client.username(),
                        private_key_file,
                    )
                });
            if try_join_all(futures).await.is_ok() {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{provider::test_client::TestClient, settings::Settings, testbed::Testbed};

    #[tokio::test]
    async fn create() {
        let settings = Settings::new_for_test();
        let client = TestClient::new();
        let mut testbed = Testbed::new(settings, client).await.unwrap();

        testbed.create(5, None).await.unwrap();

        assert_eq!(testbed.instances.len(), 5 * testbed.settings.regions.len());
        for (i, instance) in testbed.instances.iter().enumerate() {
            assert_eq!(i.to_string(), instance.id);
        }
    }

    #[tokio::test]
    async fn destroy() {
        let settings = Settings::new_for_test();
        let client = TestClient::new();
        let mut testbed = Testbed::new(settings, client).await.unwrap();

        testbed.destroy().await.unwrap();

        assert_eq!(testbed.instances.len(), 0);
    }

    #[tokio::test]
    async fn start() {
        let settings = Settings::new_for_test();
        let client = TestClient::new();
        let mut testbed = Testbed::new(settings, client).await.unwrap();
        testbed.create(5, None).await.unwrap();
        testbed.stop().await.unwrap();

        let result = testbed.start(2).await;

        assert!(result.is_ok());
        for region in &testbed.settings.regions {
            let active = testbed
                .instances
                .iter()
                .filter(|x| x.is_active() && &x.region == region)
                .count();
            assert_eq!(active, 2);

            let inactive = testbed
                .instances
                .iter()
                .filter(|x| x.is_inactive() && &x.region == region)
                .count();
            assert_eq!(inactive, 3);
        }
    }

    #[tokio::test]
    async fn stop() {
        let settings = Settings::new_for_test();
        let client = TestClient::new();
        let mut testbed = Testbed::new(settings, client).await.unwrap();
        testbed.create(5, None).await.unwrap();
        testbed.start(2).await.unwrap();

        testbed.stop().await.unwrap();

        assert!(testbed.instances.iter().all(|x| x.is_inactive()))
    }
}
