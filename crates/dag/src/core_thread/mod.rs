// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(any(test, feature = "simulator"))]
mod simulated;
mod spawned;

#[cfg(feature = "simulator")]
pub use simulated::*;
#[cfg(not(feature = "simulator"))]
pub use spawned::*;
