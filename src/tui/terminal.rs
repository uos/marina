//! Terminal ownership for the TUI.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};

use anyhow::{Context as _, Result};

/// Where the TUI draws.
pub type Writer = Box<dyn Write>;

/// Opens the drawing target and silences the inherited stdout and stderr.
pub fn acquire() -> Result<(Writer, Redirect)> {
    match OpenOptions::new().read(true).write(true).open("/dev/tty") {
        Ok(tty) => Ok((Box::new(tty), Redirect::to_null())),
        // No controlling terminal to claim: draw on stdout and leave the
        // standard streams alone. Progress bars may overlap the frame, which
        // still beats refusing to start.
        Err(_) => Ok((Box::new(io::stdout()), Redirect::none())),
    }
}

/// Best-effort write to the terminal, used by the panic hook after the normal
/// teardown path is gone.
pub fn emergency_writer() -> Writer {
    match OpenOptions::new().read(true).write(true).open("/dev/tty") {
        Ok(tty) => Box::new(tty),
        Err(_) => Box::new(io::stderr()),
    }
}

/// Holds the process's real stdout and stderr while `/dev/null` stands in for
/// them. Restoring is idempotent and also runs on drop.
pub struct Redirect {
    #[cfg(unix)]
    saved: Vec<(i32, std::os::fd::OwnedFd)>,
}

impl Redirect {
    fn none() -> Self {
        Self {
            #[cfg(unix)]
            saved: Vec::new(),
        }
    }

    #[cfg(unix)]
    fn to_null() -> Self {
        let Ok(null) = File::open("/dev/null").or_else(|_| File::create("/dev/null")) else {
            return Self::none();
        };

        let mut saved = Vec::new();
        for fd in [libc::STDOUT_FILENO, libc::STDERR_FILENO] {
            match replace_fd(fd, &null) {
                Ok(original) => saved.push((fd, original)),
                Err(error) => log::debug!("could not silence fd {fd}: {error}"),
            }
        }
        Self { saved }
    }

    #[cfg(not(unix))]
    fn to_null() -> Self {
        Self::none()
    }

    /// Puts the real stdout and stderr back.
    pub fn restore(&mut self) {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;

            for (fd, original) in self.saved.drain(..) {
                // SAFETY: `original` is a live descriptor this process owns, and
                // `fd` is one of the standard descriptors it was taken from.
                if unsafe { libc::dup2(original.as_raw_fd(), fd) } < 0 {
                    log::debug!("could not restore fd {fd}: {}", io::Error::last_os_error());
                }
            }
        }
    }
}

impl Drop for Redirect {
    fn drop(&mut self) {
        self.restore();
    }
}

/// Points `fd` at `replacement`, returning the descriptor it used to hold.
#[cfg(unix)]
fn replace_fd(fd: i32, replacement: &File) -> Result<std::os::fd::OwnedFd> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    // SAFETY: `fd` is a standard descriptor of this process; `dup` either
    // returns a fresh owned descriptor or -1.
    let duplicated = unsafe { libc::dup(fd) };
    if duplicated < 0 {
        return Err(io::Error::last_os_error()).context("could not duplicate a standard stream");
    }
    // SAFETY: `duplicated` is a fresh descriptor with no other owner.
    let original = unsafe { OwnedFd::from_raw_fd(duplicated) };

    // SAFETY: both descriptors are open and owned by this process.
    if unsafe { libc::dup2(replacement.as_raw_fd(), fd) } < 0 {
        return Err(io::Error::last_os_error()).context("could not redirect a standard stream");
    }
    Ok(original)
}
