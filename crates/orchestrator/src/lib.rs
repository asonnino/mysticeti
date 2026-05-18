// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Orchestrator library: cloud-testbed lifecycle (deploy / start / stop /
//! destroy), protocol-agnostic node + client orchestration over SSH,
//! Prometheus-backed metric collection. Consumers compose the phases on
//! [`Orchestrator`] in their own loop and render progress themselves — the
//! library never touches stdout.

pub mod benchmark;
pub mod collector;
pub mod error;
pub mod faults;
pub mod logs;
pub mod monitor;
pub mod orchestrator;
pub mod protocol;
pub mod provider;
pub mod settings;
pub mod ssh;
pub mod testbed;
