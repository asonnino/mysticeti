// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use orchestrator::testbed::TestbedStatus;

use crate::terminal::{BOLD, RESET};

pub trait TestbedStatusRender {
    fn render(&self, color: bool) -> String;
}

impl TestbedStatusRender for TestbedStatus {
    fn render(&self, color: bool) -> String {
        let mut out = format!(
            "Client: {}\nRepo: {} ({})\nInstances active: {}\n",
            self.client_summary, self.repository_url, self.repository_commit, self.active_count
        );
        for region in &self.regions {
            out.push('\n');
            let heading = region.region.to_uppercase();
            if color {
                out.push_str(&format!("{BOLD}[{heading}]{RESET}\n"));
            } else {
                out.push_str(&format!("[{heading}]\n"));
            }
            if region.instances.is_empty() {
                out.push_str("  (no instances)\n");
                continue;
            }
            for (index, instance) in region.instances.iter().enumerate() {
                let marker = if instance.active { "●" } else { "○" };
                out.push_str(&format!(
                    "  {marker} {index:>2}  {}\n",
                    instance.connect_command
                ));
            }
        }
        out
    }
}
