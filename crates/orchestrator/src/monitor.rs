// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;

use crate::{
    benchmark::Parameters,
    error::MonitorResult,
    protocol::ProtocolMetrics,
    provider::Instance,
    ssh::{CommandContext, SshConnectionManager},
};

pub(crate) struct Monitor {
    instance: Instance,
    clients: Vec<Instance>,
    nodes: Vec<Instance>,
    ssh_manager: SshConnectionManager,
}

impl Monitor {
    /// Create a new monitor.
    pub(crate) fn new(
        instance: Instance,
        clients: Vec<Instance>,
        nodes: Vec<Instance>,
        ssh_manager: SshConnectionManager,
    ) -> Self {
        Self {
            instance,
            clients,
            nodes,
            ssh_manager,
        }
    }

    /// Dependencies to install.
    pub(crate) fn dependencies() -> Vec<String> {
        let mut commands: Vec<String> = Vec::new();
        commands.extend(Prometheus::install_commands().into_iter().map(String::from));
        commands.extend(Grafana::install_commands().into_iter().map(String::from));
        commands.extend(NodeExporter::install_commands());
        commands
    }

    /// Start a prometheus instance on the dedicated motoring machine.
    pub(crate) async fn start_prometheus<P: ProtocolMetrics>(
        &self,
        protocol_commands: &P,
        parameters: &Parameters<P>,
    ) -> MonitorResult<()> {
        // Configure and reload prometheus.
        let instance = [self.instance.clone()];
        let commands = Prometheus::setup_commands(
            self.nodes.clone(),
            self.clients.clone(),
            protocol_commands,
            parameters,
        );
        self.ssh_manager
            .execute(instance, commands, CommandContext::default())
            .await?;

        Ok(())
    }

    /// Start grafana on the dedicated motoring machine.
    pub(crate) async fn start_grafana(&self) -> MonitorResult<()> {
        // Configure and reload grafana.
        let instance = std::iter::once(self.instance.clone());
        let commands = Grafana::setup_commands();
        self.ssh_manager
            .execute(instance, commands, CommandContext::default())
            .await?;

        Ok(())
    }

    /// The public address of the grafana instance.
    pub(crate) fn grafana_address(&self) -> String {
        format!("http://{}:{}", self.instance.main_ip, Grafana::DEFAULT_PORT)
    }

    /// The public address of the prometheus instance, used by the consumer to
    /// query metrics over PromQL.
    pub(crate) fn prometheus_address(&self) -> String {
        format!(
            "http://{}:{}",
            self.instance.main_ip,
            Prometheus::DEFAULT_PORT
        )
    }
}

/// Generate the commands to setup prometheus on the given instances.
pub(crate) struct Prometheus;

impl Prometheus {
    /// The default prometheus configuration path.
    const DEFAULT_PROMETHEUS_CONFIG_PATH: &'static str = "/etc/prometheus/prometheus.yml";
    /// The default prometheus port.
    pub(crate) const DEFAULT_PORT: u16 = 9090;

    /// The commands to install prometheus.
    pub(crate) fn install_commands() -> Vec<&'static str> {
        vec![
            "sudo apt-get -y install prometheus",
            "sudo chmod 777 -R /var/lib/prometheus/ /etc/prometheus/",
        ]
    }

    /// Generate the commands to update the prometheus configuration and restart prometheus.
    pub(crate) fn setup_commands<I, P>(
        nodes: I,
        _clients: I,
        protocol: &P,
        parameters: &Parameters<P>,
    ) -> String
    where
        I: IntoIterator<Item = Instance>,
        P: ProtocolMetrics,
    {
        // Generate the prometheus configuration.
        let mut config = vec![Self::global_configuration()];

        let nodes_metrics_path = protocol.nodes_metrics_path(nodes, parameters);
        for (i, (_, nodes_metrics_path)) in nodes_metrics_path.into_iter().enumerate() {
            let id = format!("node-{i}");
            let scrape_config = Self::scrape_configuration(&id, &nodes_metrics_path);
            config.push(scrape_config);
        }

        // NOTE: Hack to avoid clients metrics.
        // let clients_metrics_path = protocol.clients_metrics_path(clients, parameters);
        // for (i, (_, client_metrics_path)) in clients_metrics_path.into_iter().enumerate() {
        //     let id = format!("client-{i}");
        //     let scrape_config = Self::scrape_configuration(&id, &client_metrics_path);
        //     config.push(scrape_config);
        // }

        // Make the command to configure and restart prometheus.
        format!(
            "sudo echo \"{}\" > {} && sudo service prometheus restart",
            config.join("\n"),
            Self::DEFAULT_PROMETHEUS_CONFIG_PATH
        )
    }

    /// Generate the global prometheus configuration.
    /// NOTE: The configuration file is a yaml file so spaces are important.
    fn global_configuration() -> String {
        [
            "global:",
            "  scrape_interval: 5s",
            "  evaluation_interval: 5s",
            "scrape_configs:",
        ]
        .join("\n")
    }

    /// Generate the prometheus configuration from the given metrics path.
    /// NOTE: The configuration file is a yaml file so spaces are important.
    fn scrape_configuration(id: &str, nodes_metrics_path: &str) -> String {
        let parts: Vec<_> = nodes_metrics_path.split('/').collect();
        let address = parts[0].parse::<SocketAddr>().unwrap();
        let ip = address.ip();
        let port = address.port();
        let path = parts[1];

        [
            &format!("  - job_name: instance-{id}"),
            &format!("    metrics_path: /{path}"),
            "    static_configs:",
            "      - targets:",
            &format!("        - {ip}:{port}"),
            &format!("  - job_name: instance-node-exporter-{id}"),
            "    static_configs:",
            "      - targets:",
            &format!("        - {ip}:9200"),
        ]
        .join("\n")
    }
}

pub(crate) struct Grafana;

impl Grafana {
    /// The path to the datasources directory.
    const DATASOURCES_PATH: &'static str = "/etc/grafana/provisioning/datasources";
    /// The default grafana port.
    pub(crate) const DEFAULT_PORT: u16 = 3000;

    /// The commands to install grafana.
    pub(crate) fn install_commands() -> Vec<&'static str> {
        vec![
            "sudo apt-get install -y apt-transport-https software-properties-common wget",
            "sudo wget -q -O /etc/apt/keyrings/grafana.key https://apt.grafana.com/gpg.key",
            "(sudo rm /etc/apt/sources.list.d/grafana.list || true)",
            "echo \
                \"deb [signed-by=/etc/apt/keyrings/grafana.key] \
                https://apt.grafana.com stable main\" \
                | sudo tee -a /etc/apt/sources.list.d/grafana.list",
            "sudo apt-get update",
            "sudo apt-get install -y grafana",
            "sudo chmod 777 -R /etc/grafana/",
        ]
    }

    /// Generate the commands to update the grafana datasource and restart grafana.
    pub(crate) fn setup_commands() -> String {
        [
            &format!("sudo rm -rf {}", Self::DATASOURCES_PATH),
            &format!("sudo mkdir -p {}", Self::DATASOURCES_PATH),
            &format!(
                "echo \"{}\" | sudo tee {}/testbed.yml > /dev/null",
                Self::datasource(),
                Self::DATASOURCES_PATH
            ),
            "sudo service grafana-server restart",
        ]
        .join(" && ")
    }

    /// Generate the content of the datasource file for the given instance.
    /// NOTE: The datasource file is a yaml file so spaces are important.
    fn datasource() -> String {
        [
            "apiVersion: 1",
            "deleteDatasources:",
            "  - name: testbed",
            "    orgId: 1",
            "datasources:",
            "  - name: testbed",
            "    type: prometheus",
            "    access: proxy",
            "    orgId: 1",
            &format!("    url: http://localhost:{}", Prometheus::DEFAULT_PORT),
            "    editable: true",
            "    uid: Fixed-UID-testbed",
        ]
        .join("\n")
    }
}
/// Generate the commands to setup node exporter on the given instances.
struct NodeExporter;

impl NodeExporter {
    const RELEASE: &'static str = "1.11.1";
    const DEFAULT_PORT: u16 = 9200;
    const SERVICE_PATH: &'static str = "/etc/systemd/system/node_exporter.service";

    pub(crate) fn install_commands() -> Vec<String> {
        let build = format!("node_exporter-{}.linux-amd64", Self::RELEASE);
        let source = format!(
            "https://github.com/prometheus/node_exporter/releases/download/v{}/{build}.tar.gz",
            Self::RELEASE
        );

        [
            "(sudo systemctl status node_exporter && exit 0)",
            &format!("curl -LO {source}"),
            &format!(
                "tar -xvf node_exporter-{}.linux-amd64.tar.gz",
                Self::RELEASE
            ),
            &format!(
                "sudo mv node_exporter-{}.linux-amd64/node_exporter /usr/local/bin/",
                Self::RELEASE
            ),
            "sudo useradd -rs /bin/false node_exporter || true",
            "sudo chmod 777 -R /etc/systemd/system/",
            &format!(
                "sudo echo \"{}\" > {}",
                Self::service_config(),
                Self::SERVICE_PATH
            ),
            "sudo systemctl daemon-reload",
            "sudo systemctl start node_exporter",
            "sudo systemctl enable node_exporter",
        ]
        .map(|x| x.to_string())
        .to_vec()
    }

    fn service_config() -> String {
        [
            "[Unit]",
            "Description=Node Exporter",
            "After=network.target",
            "[Service]",
            "User=node_exporter",
            "Group=node_exporter",
            "Type=simple",
            &format!(
                "ExecStart=/usr/local/bin/node_exporter --web.listen-address=:{}",
                Self::DEFAULT_PORT
            ),
            "[Install]",
            "WantedBy=multi-user.target",
        ]
        .join("\n")
    }
}
