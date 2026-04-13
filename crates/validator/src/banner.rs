// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::io::IsTerminal;

const WHITE: &str = "\x1b[1;37m";
const BLUE: &str = "\x1b[34m";
const CYAN: &str = "\x1b[36m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";

pub fn print_banner(subtitle: &str) {
    let color = std::io::stderr().is_terminal();
    let (wh, bl, cy, bo, rs) = if color {
        (WHITE, BLUE, CYAN, BOLD, RESET)
    } else {
        ("", "", "", "", "")
    };

    let width = terminal_width().min(80);
    let banner = format!(
        "\
{wh}        .{rs}\n\
{wh}        \":\"{rs}\n\
{bl}         ___:____     |\"\\/\"|{rs}\n\
{bl}       ,'        `.    \\  /{rs}\n\
{bl}       |  O        \\___/  |{rs}\n\
{cy}     ~^~^~^~^~^~^~^~^~^~^~^~^~{rs}\n\
\n\
{bo}           {subtitle}{rs}"
    );

    let lines: Vec<&str> = banner.lines().collect();
    eprintln!();
    for line in &lines {
        let visible_len = visible_width(line);
        let pad = width.saturating_sub(visible_len) / 2;
        eprintln!("{:pad$}{line}", "");
    }
    eprintln!();
}

fn visible_width(s: &str) -> usize {
    let mut width = 0;
    let mut in_escape = false;
    for c in s.chars() {
        if in_escape {
            if c.is_ascii_alphabetic() {
                in_escape = false;
            }
        } else if c == '\x1b' {
            in_escape = true;
        } else {
            width += 1;
        }
    }
    width
}

fn terminal_width() -> usize {
    std::env::var("COLUMNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(80)
}
