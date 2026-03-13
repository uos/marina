# Rust (Cargo)

The `marina` crate exposes the same resolve and pull logic used by the CLI as a public async API.

## Add the dependency

~~~toml
[dependencies]
marina = "0.2"
tokio = { version = "1", features = ["rt", "macros"] }
~~~

!!! note "Runtime requirement"

    Marina's async methods can only be driven by a **tokio** runtime. The SSH and HTTP registry drivers use tokio-specific I/O internally. A single-threaded runtime (`#[tokio::main(flavor = "current_thread")]`) is sufficient.

## Resolve a dataset

`Marina::resolve_target` checks whether a dataset is already cached, available locally, or only reachable via a remote registry.

~~~rust
use marina::{Marina, ResolveResult};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let marina = Marina::load()?;

    match marina.resolve_target("outdoor-run:v2", None).await? {
        ResolveResult::LocalPath(p) | ResolveResult::Cached(p) => {
            println!("ready at {}", p.display());
        }
        ResolveResult::RemoteAvailable { bag, registry, .. } => {
            println!("remote: {bag} in {registry}");
        }
        ResolveResult::Ambiguous { candidates } => {
            println!("found in {} registries", candidates.len());
        }
    }
    Ok(())
}
~~~

Pass a registry name as the second argument to restrict the search:

~~~rust
marina.resolve_target("outdoor-run:v2", Some("team-ssh")).await?;
~~~

## Pull a dataset

`pull_exact` downloads a dataset if it is not already cached and returns the local path:

~~~rust
use marina::{Marina, BagRef};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut marina = Marina::load()?;
    let bag: BagRef = "outdoor-run:v2".parse()?;
    let path = marina.pull_exact(&bag, None).await?;
    println!("cached at {}", path.display());
    Ok(())
}
~~~

## Pull with progress reporting

Implement `ProgressSink` to receive phase events during download and decompression:

~~~rust
use marina::{Marina, BagRef, ProgressEvent, ProgressReporter, ProgressSink};

struct MyProgress;

impl ProgressSink for MyProgress {
    fn emit(&mut self, event: ProgressEvent) {
        println!("[{}] {}", event.phase, event.message);
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut marina = Marina::load()?;
    let bag: BagRef = "outdoor-run:v2".parse()?;

    let mut sink = MyProgress;
    let mut reporter = ProgressReporter::new(&mut sink);
    let path = marina.pull_exact_with_progress(&bag, None, &mut reporter).await?;
    println!("done: {}", path.display());
    Ok(())
}
~~~

`WriterProgress` is a built-in sink that writes human-readable output to any `io::Write` (e.g. stdout):

~~~rust
use marina::{Marina, BagRef, ProgressReporter, WriterProgress};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut marina = Marina::load()?;
    let bag: BagRef = "outdoor-run:v2".parse()?;

    let mut stdout = std::io::stdout();
    let mut sink = WriterProgress::new(&mut stdout);
    let mut reporter = ProgressReporter::new(&mut sink);
    marina.pull_exact_with_progress(&bag, None, &mut reporter).await?;
    Ok(())
}
~~~

## API reference

Full rustdoc is published to [docs.rs/marina](https://docs.rs/marina).
