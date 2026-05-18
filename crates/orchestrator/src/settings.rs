// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    env,
    fmt::Display,
    fs,
    net::Ipv4Addr,
    path::{Path, PathBuf},
    time::Duration,
};

use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, DurationSeconds, serde_as};

use crate::{
    error::{SettingsError, SettingsResult},
    faults::FaultsType,
    provider::Instance,
};

/// The git repository holding the codebase.
#[serde_as]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Repository {
    /// The url of the repository.
    #[serde_as(as = "DisplayFromStr")]
    pub url: Url,
    /// The commit (or branch name) to deploy.
    pub commit: String,
}

impl Default for Repository {
    fn default() -> Self {
        Self {
            url: Url::parse("https://example.com/author/repo").unwrap(),
            commit: "main".into(),
        }
    }
}

impl Repository {
    /// Set the commit to 'unknown'. This options is useful when the orchestrator cannot
    /// be certain of the commit that is running on the instances. This is a failsafe against
    /// reporting wrong commit values in the measurements.
    pub fn set_unknown_commit(&mut self) {
        self.commit = "unknown".into();
    }

    /// Remove the Github access token from the repository url.
    pub fn remove_access_token(&mut self) {
        self.url.set_password(None).unwrap();
        self.url.set_username("").unwrap();
    }
}

/// AWS-specific configuration carried inside [`CloudProvider::Aws`].
#[derive(Serialize, Deserialize, Clone, Default, PartialEq, Eq, Debug)]
pub struct AwsConfig {
    /// The EC2 instance type, e.g. `m5d.8xlarge`.
    pub specs: String,
    /// The path to the AWS credentials file.
    #[serde(skip_serializing)]
    pub token_file: PathBuf,
}

/// A single pre-provisioned machine for the custom provider, tagged with the
/// region it belongs to (which must appear in [`Settings::regions`]).
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct CustomInstance {
    pub region: String,
    pub ip: Ipv4Addr,
}

/// Custom-provider configuration: the user has pre-provisioned the machines
/// and provides their addresses.
#[derive(Serialize, Deserialize, Clone, Default, PartialEq, Eq, Debug)]
pub struct CustomConfig {
    /// The SSH username configured on the pre-provisioned machines.
    pub ssh_username: String,
    /// The pre-provisioned machines.
    pub instances: Vec<CustomInstance>,
}

/// The list of supported cloud providers.
///
/// Each variant carries its own configuration so that the type system rules
/// out invalid combinations (e.g. a custom testbed without an IP list, or
/// AWS-specific fields stuck on a non-AWS settings file).
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum CloudProvider {
    #[serde(alias = "aws")]
    Aws(AwsConfig),
    #[serde(alias = "custom")]
    Custom(CustomConfig),
}

impl Default for CloudProvider {
    fn default() -> Self {
        Self::Aws(AwsConfig::default())
    }
}

/// The testbed settings. Those are topically specified in a file.
#[serde_as]
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Settings {
    /// The testbed unique id. This allows multiple users to run concurrent testbeds on the
    /// same cloud provider's account without interference with each others.
    pub testbed_id: String,
    /// The cloud provider hosting the testbed (plus its provider-specific config).
    pub cloud_provider: CloudProvider,
    /// The ssh private key to access the instances.
    #[serde(skip_serializing)]
    pub ssh_private_key_file: PathBuf,
    /// The corresponding ssh public key registered on the instances. If not specified. the
    /// public key defaults the same path as the private key with an added extension 'pub'.
    pub ssh_public_key_file: Option<PathBuf>,
    /// The list of regions hosting the testbed. Shared by both providers: for
    /// AWS these are real cloud regions; for the custom provider they are
    /// user-chosen labels that group pre-provisioned machines.
    pub regions: Vec<String>,
    /// The details of the git reposit to deploy.
    pub repository: Repository,
    /// The path to the node's configuration file. If not specified, the orchestrator uses the
    /// default configurations.
    pub node_parameters_path: Option<String>,
    /// The path to the client's configuration file. If not specified, the orchestrator uses the
    /// default configurations.
    pub client_parameters_path: Option<String>,
    /// The duration of the benchmark. The orchestrator stops the benchmark after this duration.
    /// If this value is set to zero, the orchestrator runs the benchmark indefinitely.
    #[serde(default = "defaults::default_benchmark_duration")]
    #[serde_as(as = "DurationSeconds")]
    pub benchmark_duration: Duration,
    /// The default faults type to apply to the testbed's nodes.
    #[serde(default = "defaults::default_faults_type")]
    pub faults: FaultsType,
    /// The working directory on the remote instance (containing all configuration files).
    #[serde(default = "defaults::default_working_dir")]
    pub working_dir: PathBuf,
    /// The directory (on the local machine) where to save benchmarks measurements.
    #[serde(default = "defaults::default_results_dir")]
    pub results_dir: PathBuf,
    /// The directory (on the local machine) where to download logs files from the instances.
    #[serde(default = "defaults::default_logs_dir")]
    pub logs_dir: PathBuf,
    /// Whether to use NVMe drives for data storage (if available).
    #[serde(default = "defaults::default_use_nvme")]
    pub nvme: bool,
    /// The interval between measurements collection.
    #[serde(default = "defaults::default_scrape_interval")]
    #[serde_as(as = "DurationSeconds")]
    pub scrape_interval: Duration,
    /// Whether to downloading and analyze the client and node log files.
    #[serde(default = "defaults::default_log_processing")]
    pub log_processing: bool,
    /// Number of instances running only load generators (not nodes). If this value is set
    /// to zero, the orchestrator runs a load generate collocated with each node.
    #[serde(default = "defaults::default_dedicated_clients")]
    pub dedicated_clients: usize,
    /// Whether to start a grafana and prometheus instance on a dedicate machine.
    #[serde(default = "defaults::default_monitoring")]
    pub monitoring: bool,
    /// The timeout duration for ssh commands (in seconds).
    #[serde(default = "defaults::default_ssh_timeout")]
    #[serde_as(as = "DurationSeconds")]
    pub ssh_timeout: Duration,
    /// The number of times the orchestrator should retry an ssh command.
    #[serde(default = "defaults::default_ssh_retries")]
    pub ssh_retries: usize,
}

mod defaults {
    use std::{path::PathBuf, time::Duration};

    use crate::faults::FaultsType;

    pub fn default_benchmark_duration() -> Duration {
        Duration::from_secs(0)
    }

    pub fn default_faults_type() -> FaultsType {
        FaultsType::default()
    }

    pub fn default_working_dir() -> PathBuf {
        ["~", "working_dir"].iter().collect()
    }

    pub fn default_results_dir() -> PathBuf {
        [".", "results"].iter().collect()
    }

    pub fn default_logs_dir() -> PathBuf {
        [".", "logs"].iter().collect()
    }

    pub fn default_use_nvme() -> bool {
        true
    }

    pub fn default_scrape_interval() -> Duration {
        Duration::from_secs(15)
    }

    pub fn default_log_processing() -> bool {
        false
    }

    pub fn default_dedicated_clients() -> usize {
        0
    }

    pub fn default_monitoring() -> bool {
        true
    }

    pub fn default_ssh_timeout() -> Duration {
        Duration::from_secs(30)
    }

    pub fn default_ssh_retries() -> usize {
        3
    }
}

impl Settings {
    /// Load the settings from a json file.
    pub fn load<P>(path: P) -> SettingsResult<Self>
    where
        P: AsRef<Path> + Display + Clone,
    {
        let reader = || -> Result<Self, Box<dyn std::error::Error>> {
            let data = fs::read(path.clone())?;
            let data = Self::resolve_env(&path, std::str::from_utf8(&data)?)?;
            let settings: Settings = serde_yaml::from_slice(data.as_bytes())?;

            fs::create_dir_all(&settings.results_dir)?;
            fs::create_dir_all(&settings.logs_dir)?;

            Ok(settings)
        };

        reader().map_err(|e| SettingsError::InvalidSettings {
            file: path.to_string(),
            message: e.to_string(),
        })
    }

    // Resolves ${ENV} into it's value for each env variable.
    fn resolve_env<P>(path: P, s: &str) -> SettingsResult<String>
    where
        P: AsRef<Path> + Display + Clone,
    {
        let mut s = s.to_string();
        for (name, value) in env::vars() {
            s = s.replace(&format!("${{{}}}", name), &value);
        }
        if s.contains("${") {
            return Err(SettingsError::InvalidSettings {
                file: path.to_string(),
                message: format!("Unresolved env variables {s} in the settings file"),
            });
        }
        Ok(s)
    }

    /// Whether the given instance belongs to the active testbed. The region
    /// check applies to every provider; AWS additionally requires the instance
    /// type to match the configured specs (a single AWS account may host
    /// machines from unrelated testbeds, so the type narrows the match).
    pub fn filter_instance(&self, instance: &Instance) -> bool {
        if !self.regions.contains(&instance.region) {
            return false;
        }
        match &self.cloud_provider {
            CloudProvider::Aws(c) => {
                instance.specs.to_lowercase().replace('.', "")
                    == c.specs.to_lowercase().replace('.', "")
            }
            CloudProvider::Custom(_) => true,
        }
    }

    /// Get the name of the repository (from its url).
    pub fn repository_name(&self) -> String {
        self.repository
            .url
            .path_segments()
            .expect("Url should already be checked when loading settings")
            .collect::<Vec<_>>()[1]
            .split('.')
            .next()
            .unwrap()
            .to_string()
    }

    /// Load the ssh public key from file.
    pub fn load_ssh_public_key(&self) -> SettingsResult<String> {
        let ssh_public_key_file = self.ssh_public_key_file.clone().unwrap_or_else(|| {
            let mut private = self.ssh_private_key_file.clone();
            private.set_extension("pub");
            private
        });
        match fs::read_to_string(&ssh_public_key_file) {
            Ok(token) => Ok(token.trim_end_matches('\n').to_string()),
            Err(e) => Err(SettingsError::SshPublicKeyFileError {
                file: ssh_public_key_file.display().to_string(),
                message: e.to_string(),
            }),
        }
    }

    /// Test settings for unit tests.
    #[cfg(test)]
    pub fn new_for_test() -> Self {
        // Create a temporary public key file.
        let mut path = tempfile::tempdir().unwrap().keep();
        path.push("test_public_key.pub");
        let public_key = "This is a fake public key for tests";
        fs::write(&path, public_key).unwrap();

        // Return set settings.
        Self {
            testbed_id: "testbed".into(),
            cloud_provider: CloudProvider::Aws(AwsConfig {
                specs: "test-specs".into(),
                token_file: "/path/to/token/file".into(),
            }),
            regions: vec!["test-region".into()],
            ssh_private_key_file: "/path/to/private/key/file".into(),
            ssh_public_key_file: Some(path),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod test {
    use reqwest::Url;

    use crate::settings::Settings;

    #[test]
    fn load_ssh_public_key() {
        let settings = Settings::new_for_test();
        let public_key = settings.load_ssh_public_key().unwrap();
        assert_eq!(public_key, "This is a fake public key for tests");
    }

    #[test]
    fn repository_name() {
        let mut settings = Settings::new_for_test();
        settings.repository.url = Url::parse("https://example.com/author/name").unwrap();
        assert_eq!(settings.repository_name(), "name");
    }

    #[test]
    fn remove_access_token() {
        let mut settings = Settings::new_for_test();
        settings.repository.url = Url::parse("https://TOKEN@example.com/author/name").unwrap();
        settings.repository.remove_access_token();
        assert_eq!(
            settings.repository.url,
            Url::parse("https://example.com/author/name").unwrap()
        );
    }
}
