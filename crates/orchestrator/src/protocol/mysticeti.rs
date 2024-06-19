// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};

use super::{ProtocolCommands, ProtocolMetrics, ProtocolParameters, BINARY_PATH};
use crate::{benchmark::BenchmarkParameters, client::Instance};

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct MysticetiNodeParameters {}

impl ProtocolParameters for MysticetiNodeParameters {}

impl Display for MysticetiNodeParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MysticetiNodeParameters")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct MysticetiClientParameters {}

impl ProtocolParameters for MysticetiClientParameters {}

impl Display for MysticetiClientParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MysticetiClientParameters")
    }
}

pub struct MysticetiProtocol;

impl ProtocolCommands for MysticetiProtocol {
    fn protocol_dependencies(&self) -> Vec<&'static str> {
        // Clang is required to compile rocksdb.
        vec!["sudo apt-get -y install clang"]
    }

    fn db_directories(&self) -> Vec<std::path::PathBuf> {
        // The service binary can delete its own storage directory before booting.
        vec![]
    }

    async fn genesis_command<'a, I>(&self, instances: I, parameters: &BenchmarkParameters) -> String
    where
        I: Iterator<Item = &'a Instance>,
    {
        todo!()
    }

    fn node_command<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        todo!()
    }

    fn client_command<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        todo!()
    }
}

impl ProtocolMetrics for MysticetiProtocol {
    const BENCHMARK_DURATION: &'static str = "benchmark_duration";
    const TOTAL_TRANSACTIONS: &'static str = "total_transactions";
    const LATENCY_BUCKETS: &'static str = "latency_buckets";
    const LATENCY_SUM: &'static str = "latency_sum";
    const LATENCY_SQUARED_SUM: &'static str = "latency_squared_sum";

    fn nodes_metrics_path<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        todo!()
    }

    fn clients_metrics_path<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        todo!()
    }
}
