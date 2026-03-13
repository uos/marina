use marina::{BagRef, Marina, ProgressReporter, ResolveResult, WriterProgress};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut marina = Marina::load()?;

    // Resolve a local path or cached/remote reference.
    match marina.resolve_target("tag", None).await? {
        ResolveResult::LocalPath(path) | ResolveResult::Cached(path) => {
            println!("resolved local/cached path: {}", path.display());
        }
        ResolveResult::RemoteAvailable { registry, bag, .. } => {
            println!("remote available: {} in {}", bag, registry);
        }
        ResolveResult::Ambiguous { candidates } => {
            for (registry, bag) in candidates {
                println!("ambiguous: {} in {}", bag, registry);
            }
        }
    }

    // Pull a concrete bag ref.
    let bag: BagRef = "tag:ouster".parse()?;
    let mut out = std::io::stdout();
    let mut sink = WriterProgress::new(&mut out);
    let mut progress = ProgressReporter::new(&mut sink);
    let local = marina
        .pull_exact_with_progress(&bag, None, &mut progress)
        .await?;
    println!("pulled to {}", local.display());

    // Read local catalog.
    for entry in marina.list_cached_bags() {
        println!(
            "{} -> {} ({} bytes)",
            entry.bag,
            entry.local_dir.display(),
            entry.original_bytes,
        );
    }

    Ok(())
}
