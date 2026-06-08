// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

/// The status of a ssh command running in the background.
#[derive(PartialEq, Eq)]
pub(crate) enum CommandStatus {
    Running,
    Terminated,
}

impl CommandStatus {
    /// Return whether a background command is still running. Returns `Terminated` if the
    /// command is not running in the background.
    pub(crate) fn status(command_id: &str, text: &str) -> Self {
        if text.contains(command_id) {
            Self::Running
        } else {
            Self::Terminated
        }
    }
}

/// The command to execute on all specified remote machines.
#[derive(Clone, Default)]
pub(crate) struct CommandContext {
    /// Whether to run the command in the background (and return immediately). Commands
    /// running in the background are identified by a unique id.
    background: Option<String>,
    /// The path from where to execute the command.
    path: Option<PathBuf>,
    /// The log file to redirect all stdout and stderr.
    log_file: Option<PathBuf>,
}

impl CommandContext {
    /// Create a new ssh command.
    pub(crate) fn new() -> Self {
        Self {
            background: None,
            path: None,
            log_file: None,
        }
    }

    /// Set id of the command and indicate that it should run in the background.
    pub(crate) fn run_background(mut self, id: String) -> Self {
        self.background = Some(id);
        self
    }

    /// Set the path from where to execute the command.
    pub(crate) fn with_execute_from_path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    /// Set the log file where to redirect stdout and stderr.
    pub(crate) fn with_log_file(mut self, path: PathBuf) -> Self {
        self.log_file = Some(path);
        self
    }

    /// Apply the context to a base command.
    pub(crate) fn apply<S: Into<String>>(&self, base_command: S) -> String {
        let mut str = base_command.into();
        if let Some(log_file) = &self.log_file {
            str = format!("{str} |& tee {}", log_file.as_path().display());
        }
        if let Some(id) = &self.background {
            str = format!("tmux new -d -s \"{id}\" \"{str}\"");
        }
        if let Some(exec_path) = &self.path {
            str = format!("(cd {} && {str})", exec_path.as_path().display());
        }
        str
    }
}
