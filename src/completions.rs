use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap_complete::Shell;

pub(crate) fn install(bin: &str) -> Result<()> {
    let shell = detect_shell()?;
    let (path, line) = registration(&shell, bin)?;

    if add_line(&path, &line)? {
        println!("Installed {} completions in {}", shell, path.display());
    } else {
        println!(
            "{} completions are already installed in {}",
            shell,
            path.display()
        );
    }
    println!("Restart your shell to activate them.");
    Ok(())
}

fn detect_shell() -> Result<Shell> {
    if let Some(shell) = std::env::var_os("SHELL") {
        if let Some(shell) = Path::new(&shell).file_name().and_then(|name| name.to_str()) {
            if let Some(shell) = shell_from_name(shell) {
                return Ok(shell);
            }
        }
    }

    #[cfg(windows)]
    if std::env::var_os("PSModulePath").is_some() {
        return Ok(Shell::PowerShell);
    }

    bail!(
        "could not detect the current shell; set SHELL to bash, zsh, fish, or elvish, or pass a shell to print its completion script"
    )
}

fn shell_from_name(name: &str) -> Option<Shell> {
    match name.to_ascii_lowercase().as_str() {
        "bash" => Some(Shell::Bash),
        "zsh" => Some(Shell::Zsh),
        "fish" => Some(Shell::Fish),
        "elvish" => Some(Shell::Elvish),
        "pwsh" | "powershell" | "powershell.exe" | "pwsh.exe" => Some(Shell::PowerShell),
        _ => None,
    }
}

fn registration(shell: &Shell, bin: &str) -> Result<(PathBuf, String)> {
    let home = home_dir()?;
    let config_home = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".config"));
    let line = match shell {
        Shell::Bash => format!("source <(COMPLETE=bash {bin})"),
        Shell::Zsh => format!("source <(COMPLETE=zsh {bin})"),
        Shell::Fish => format!("COMPLETE=fish {bin} | source"),
        Shell::Elvish => format!("eval (E:COMPLETE=elvish {bin} | slurp)"),
        Shell::PowerShell => format!(
            "$env:COMPLETE = \"powershell\"; {bin} | Out-String | Invoke-Expression; Remove-Item Env:\\COMPLETE"
        ),
        _ => bail!("shell {shell} is not supported for automatic installation"),
    };
    let path = match shell {
        Shell::Bash => home.join(".bashrc"),
        Shell::Zsh => std::env::var_os("ZDOTDIR")
            .map(PathBuf::from)
            .unwrap_or(home)
            .join(".zshrc"),
        Shell::Fish => config_home
            .join("fish/completions")
            .join(format!("{bin}.fish")),
        Shell::Elvish => config_home.join("elvish/rc.elv"),
        Shell::PowerShell => home.join("Documents/PowerShell/Microsoft.PowerShell_profile.ps1"),
        _ => unreachable!("unsupported shells returned above"),
    };
    Ok((path, line))
}

fn home_dir() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .context("could not determine the home directory")
}

fn add_line(path: &Path, line: &str) -> Result<bool> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("could not create {}", parent.display()))?;
    }
    let current = std::fs::read_to_string(path).unwrap_or_default();
    if current.lines().any(|existing| existing.trim() == line) {
        return Ok(false);
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("could not open {}", path.display()))?;
    if !current.is_empty() && !current.ends_with('\n') {
        writeln!(file)?;
    }
    writeln!(file, "{line}")?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_shell_names_and_paths() {
        assert!(matches!(shell_from_name("bash"), Some(Shell::Bash)));
        assert!(matches!(
            shell_from_name("pwsh.exe"),
            Some(Shell::PowerShell)
        ));
        assert!(shell_from_name("tcsh").is_none());
    }
}
