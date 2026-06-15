// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use orchestrator::testbed::TestbedStatus;

use crate::terminal::{BOLD, DIM, GREEN, RED, RESET};

pub trait TestbedStatusRender {
    fn render(&self, color: bool) -> String;
}

impl TestbedStatusRender for TestbedStatus {
    fn render(&self, color: bool) -> String {
        let kv = |key: &str, value: &str| {
            if color {
                format!("{DIM}{key}:{RESET} {BOLD}{value}{RESET}\n")
            } else {
                format!("{key}: {value}\n")
            }
        };
        let mut out = String::from("\n");
        out.push_str(&kv("Client", &self.client_summary));
        out.push_str(&kv(
            "Repo",
            &format!("{} ({})", self.repository_url, self.repository_commit),
        ));
        out.push_str(&kv("Instances active", &self.active_count.to_string()));
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

    use crate::terminal::{DIM, GREEN, RED, RESET};

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
    fn dims_header_keys_after_a_blank_line() {
        let rendered = status().render(true);
        assert!(rendered.starts_with('\n'));
        assert!(rendered.contains(&format!("{DIM}Client:{RESET}")));
        assert!(rendered.contains(&format!("{DIM}Instances active:{RESET}")));
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
