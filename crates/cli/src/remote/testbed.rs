// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use orchestrator::testbed::TestbedStatus;

use crate::terminal::{BOLD, GREEN, RED, RESET};

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
                let glyph = if instance.active { "●" } else { "○" };
                let marker = if color {
                    let color_code = if instance.active { GREEN } else { RED };
                    format!("{color_code}{glyph}{RESET}")
                } else {
                    glyph.to_string()
                };
                out.push_str(&format!(
                    "  {marker} {index:>2}  {}\n",
                    instance.connect_command
                ));
            }
        }
        out
    }
}

#[cfg(test)]
mod test {
    use orchestrator::testbed::{InstanceEntry, RegionStatus, TestbedStatus};

    use crate::terminal::{GREEN, RED};

    use super::TestbedStatusRender;

    fn status() -> TestbedStatus {
        TestbedStatus {
            client_summary: "test".into(),
            repository_url: "https://example.com/author/repo".into(),
            repository_commit: "main".into(),
            active_count: 1,
            regions: vec![RegionStatus {
                region: "us-west-1".into(),
                instances: vec![
                    InstanceEntry {
                        connect_command: "ssh active".into(),
                        active: true,
                    },
                    InstanceEntry {
                        connect_command: "ssh inactive".into(),
                        active: false,
                    },
                ],
            }],
        }
    }

    #[test]
    fn colors_markers_by_activity() {
        let rendered = status().render(true);
        assert!(rendered.contains(&format!("{GREEN}●")));
        assert!(rendered.contains(&format!("{RED}○")));
    }

    #[test]
    fn omits_color_codes_when_disabled() {
        let rendered = status().render(false);
        assert!(!rendered.contains(GREEN));
        assert!(!rendered.contains(RED));
        assert!(rendered.contains("●"));
        assert!(rendered.contains("○"));
    }
}
