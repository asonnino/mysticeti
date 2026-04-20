// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::io::IsTerminal;

use terminal_size::terminal_size;
use unicode_width::UnicodeWidthStr;

macro_rules! display {
    ($($arg:tt)*) => { eprintln!($($arg)*) };
}

const BLUE_FOREGROUND: &str = "\x1b[34m";
const BLUE_BACKGROUND: &str = "\x1b[44m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const RESET: &str = "\x1b[0m";

const ART: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/assets/banner.txt"));

pub struct BannerPrinter {
    protocol: String,
    info: Vec<(String, String)>,
    color: bool,
    inner_width: usize,
    top_border: String,
    bottom_border: String,
    empty_line: String,
}

impl BannerPrinter {
    pub fn new(protocol: &str, info: &[(&str, &str)]) -> Self {
        let width = terminal_size().map(|(w, _)| w.0 as usize).unwrap_or(80);
        let inner_width = width.saturating_sub(4);
        let border_fill = "─".repeat(inner_width + 2);

        Self {
            protocol: protocol.to_string(),
            info: info
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            color: std::io::stderr().is_terminal(),
            inner_width,
            top_border: format!("┌{border_fill}┐"),
            bottom_border: format!("└{border_fill}┘"),
            empty_line: format!("│ {:inner_width$} │", ""),
        }
    }

    pub fn print(&self) {
        display!();
        display!("{}", self.top_border);
        display!("{}", self.empty_line);
        self.print_art();
        display!("{}", self.empty_line);
        self.print_protocol();
        self.print_info();
        display!("{}", self.empty_line);
        display!("{}", self.bottom_border);
        display!();
    }

    fn print_art(&self) {
        for line in ART.lines() {
            let visible_len = UnicodeWidthStr::width(line);
            let padding = self.inner_width.saturating_sub(visible_len);
            if self.color {
                display!("│ {}{:padding$} │", Self::colorize(line), "");
            } else {
                display!("│ {line}{:padding$} │", "");
            }
        }
    }

    fn print_protocol(&self) {
        let visible_len = UnicodeWidthStr::width(self.protocol.as_str());
        let padding = self.inner_width.saturating_sub(visible_len);
        if self.color {
            display!("│ {BOLD}{}{RESET}{:padding$} │", self.protocol, "");
        } else {
            display!("│ {}{:padding$} │", self.protocol, "");
        }
    }

    fn print_info(&self) {
        for (key, value) in &self.info {
            let entry = format!("{key}: {value}");
            let visible_len = UnicodeWidthStr::width(entry.as_str());
            let padding = self.inner_width.saturating_sub(visible_len);
            if self.color {
                display!(
                    "│ {DIM}{key}:{RESET} \
                        {BOLD}{value}{RESET}\
                        {:padding$} │",
                    ""
                );
            } else {
                display!("│ {entry}{:padding$} │", "");
            }
        }
    }

    fn colorize(line: &str) -> String {
        let mut result = String::with_capacity(line.len() * 3);
        let mut in_background = false;
        let mut in_foreground = false;
        for c in line.chars() {
            match c {
                '█' => {
                    if in_foreground {
                        result.push_str(RESET);
                        in_foreground = false;
                    }
                    if !in_background {
                        result.push_str(BLUE_BACKGROUND);
                        in_background = true;
                    }
                    result.push(' ');
                }
                '▄' | '▀' => {
                    if in_background {
                        result.push_str(RESET);
                        in_background = false;
                    }
                    if !in_foreground {
                        result.push_str(BLUE_FOREGROUND);
                        in_foreground = true;
                    }
                    result.push(c);
                }
                _ => {
                    if in_background || in_foreground {
                        result.push_str(RESET);
                        in_background = false;
                        in_foreground = false;
                    }
                    result.push(c);
                }
            }
        }
        if in_background || in_foreground {
            result.push_str(RESET);
        }
        result
    }
}
