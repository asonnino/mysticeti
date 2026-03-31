// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub trait Ctx: Send + Sync + 'static {}

pub struct TokioCtx;
impl Ctx for TokioCtx {}

pub struct SimulatedCtx;
impl Ctx for SimulatedCtx {}
