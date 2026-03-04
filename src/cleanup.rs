use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

fn paths() -> &'static Mutex<Vec<PathBuf>> {
    static PATHS: OnceLock<Mutex<Vec<PathBuf>>> = OnceLock::new();
    PATHS.get_or_init(|| Mutex::new(Vec::new()))
}

/// Install a Ctrl-C handler that removes any registered paths and exits.
/// Should be called once at startup.
pub fn init() {
    let _ = ctrlc::set_handler(|| {
        if let Ok(guard) = paths().lock() {
            for path in guard.iter() {
                if path.is_dir() {
                    let _ = std::fs::remove_dir_all(path);
                } else if path.is_file() {
                    let _ = std::fs::remove_file(path);
                }
            }
        }
        std::process::exit(130);
    });
}

/// Register a path for deletion if the process is interrupted.
pub fn register(path: PathBuf) {
    if let Ok(mut guard) = paths().lock() {
        guard.push(path);
    }
}

/// Clear all registered paths (call after a successful operation).
pub fn commit() {
    if let Ok(mut guard) = paths().lock() {
        guard.clear();
    }
}
