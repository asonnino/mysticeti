// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Orchestrator library: cloud-testbed lifecycle (deploy / start / stop /
//! destroy), protocol-agnostic node + client orchestration over SSH,
//! Prometheus-backed metric collection. Consumers compose the phases on
//! [`Orchestrator`] in their own loop and render progress themselves — the
//! library never touches stdout.
//!
//! The mysticeti-specific [`protocol::mysticeti`] impl lives here today; it
//! moves to `crates/cli/` in #101 once the lib API is fully decoupled.

pub mod benchmark;
pub mod collector;
pub mod display;
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

/// Mysticeti-specific aliases used by [`benchmark::BenchmarkParameters`] and
/// the binary entry point. Slated to move to `crates/cli/` in #101 once the
/// lib is fully generic over the protocol parameter types.
pub type Protocol = protocol::mysticeti::MysticetiProtocol;
pub type NodeParameters = protocol::mysticeti::MysticetiNodeParameters;
pub type ClientParameters = protocol::mysticeti::MysticetiClientParameters;
