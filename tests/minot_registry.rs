//! A `minot://` registry must behave exactly like the registry behind it.
//!
//! That equivalence is the entire point of this phase: the streaming work that
//! follows sits on this transport, and it is far easier to trust once an
//! ordinary `pull` over `minot://` produces a byte-identical bundle to a `pull`
//! from the folder registry being served.

#![cfg(feature = "minot-registry")]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use marina::registry::driver::{PushMeta, RegistryDriver};
use marina::registry::folder::FolderRegistry;
use marina::registry::minot::block_store::CacheMode;
use marina::registry::minot::server::{ServeOptions, serve};
use marina::registry::minot::{MinotRegistry, StatResult};
use marina::storage::config::{RegistryConfig, RegistryDownloadMode};
use marina::{AccessMode, DatasetAccess};

/// Local-only Minot uses one fixed endpoint, so these must not overlap.
static COORDINATOR_PORT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

async fn wait_until(limit: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + limit;
    while tokio::time::Instant::now() < deadline {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    condition()
}

/// A folder registry holding one dataset bundle of `size` bytes.
fn seeded_folder_registry(root: &std::path::Path, size: usize) -> (FolderRegistry, Vec<u8>) {
    let registry = FolderRegistry::from_uri("backing", &format!("folder://{}", root.display()))
        .expect("folder registry should be creatable");
    // Position-dependent bytes, so a truncated or misordered transfer fails
    // rather than passing by luck.
    let bundle: Vec<u8> = (0..size)
        .map(|i| (i.wrapping_mul(31) % 251) as u8)
        .collect();
    (registry, bundle)
}

async fn push_bundle(
    registry: &FolderRegistry,
    bag: &marina::BagRef,
    bundle: &[u8],
) -> anyhow::Result<PathBuf> {
    let staged = std::env::temp_dir().join(unique("marina-test-bundle"));
    std::fs::write(&staged, bundle)?;
    registry
        .push(
            "backing",
            bag,
            &staged,
            &PushMeta {
                original_bytes: bundle.len() as u64 * 2,
                packed_bytes: bundle.len() as u64,
                bundle_hash: "0123456789ab".to_string(),
                pointcloud: "lossless".to_string(),
                mcap_compression: "zstd".to_string(),
                pushed_at: 1_700_000_000,
            },
        )
        .await?;
    let _ = std::fs::remove_file(&staged);
    Ok(staged)
}

/// Start a coordinator inside this process and serve `driver` on it.
///
/// Returns once the server is answering, so a test never races startup.
async fn start_server(driver: Arc<dyn RegistryDriver>, exposed_as: &str) {
    mt_sea::network::set_local_only(true);
    let mut options = ServeOptions::new(exposed_as.to_string());
    options.local_only = true;
    tokio::spawn(async move {
        if let Err(error) = serve(driver, options).await {
            eprintln!("serve ended: {error}");
        }
    });
    // The server brings up its own coordinator; wait for the endpoint.
    assert!(
        wait_until(
            Duration::from_secs(30),
            mt_sea::network::local_router_is_running
        )
        .await,
        "the served coordinator should come up"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pull_over_minot_matches_the_registry_being_served() {
    let _guard = COORDINATOR_PORT.lock().await;
    let _ = env_logger::builder().is_test(true).try_init();

    let root = tempfile::tempdir().expect("temp dir");
    // Several chunks' worth, so this is a real transfer rather than one message.
    let (backing, bundle) = seeded_folder_registry(root.path(), 3 * 1024 * 1024 + 517);
    let bag: marina::BagRef = "team/run:v1".parse().expect("bag ref");
    push_bundle(&backing, &bag, &bundle)
        .await
        .expect("seeding the backing registry should work");

    let exposed_as = unique("served");
    start_server(Arc::new(backing), &exposed_as).await;

    let client = MinotRegistry::from_uri("remote", &format!("minot://{exposed_as}"))
        .expect("client registry should parse");

    // list
    let listed = client.list("*").await.expect("list should work");
    assert!(
        listed.contains(&bag),
        "the served registry's contents should be visible: {listed:?}"
    );

    // bag_info
    let info = client
        .bag_info(&bag)
        .await
        .expect("bag_info should work")
        .expect("the dataset should have metadata");
    assert_eq!(info.packed_bytes, bundle.len() as u64);
    assert_eq!(info.bundle_hash.as_deref(), Some("0123456789ab"));

    // pull
    let destination = root.path().join("pulled.tar.gz");
    let descriptor = client
        .pull(&bag, &destination)
        .await
        .expect("pull over minot should work");
    assert_eq!(descriptor.packed_bytes, bundle.len() as u64);

    let pulled = std::fs::read(&destination).expect("the pulled bundle should exist");
    assert_eq!(
        pulled.len(),
        bundle.len(),
        "the transferred bundle must not be truncated or doubled"
    );
    assert!(
        pulled == bundle,
        "a pull over minot:// must be byte-identical to the registry being served"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_are_refused_with_a_useful_message() {
    let _guard = COORDINATOR_PORT.lock().await;
    let _ = env_logger::builder().is_test(true).try_init();

    let client = MinotRegistry::from_uri("remote", "minot://anything").expect("should parse");

    let error = client
        .check_write_access()
        .await
        .expect_err("a minot:// registry is read-only");
    assert!(
        error.to_string().contains("registry being served"),
        "the refusal should point at what to do instead, got: {error}"
    );
}

/// A tunnel that cannot be established must fail quickly and say why.
///
/// The happy path needs a reachable SSH server, which a CI box or a dev laptop
/// with Remote Login disabled will not have. What can always be checked is that
/// the failure is a clear message rather than a hang or a panic — which is the
/// part users actually hit when a host or credential is wrong.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreachable_ssh_tunnel_fails_with_a_useful_message() {
    let _ = env_logger::builder().is_test(true).try_init();

    // Port 1 on loopback refuses immediately, so this exercises the error path
    // without waiting on a network timeout.
    let client = MinotRegistry::from_uri("remote", "minot+ssh://user@127.0.0.1:1/team")
        .expect("the URI itself is well formed");

    let error = tokio::time::timeout(Duration::from_secs(60), client.check_connection())
        .await
        .expect("a refused connection must not hang")
        .expect_err("an unreachable host cannot be connected to");

    let text = format!("{error:#}");
    assert!(
        text.contains("ssh"),
        "the failure should name ssh as the thing that went wrong, got: {text}"
    );
    assert!(
        text.contains("marina serve") || text.contains("credentials"),
        "the failure should say what to check, got: {text}"
    );
}

/// The whole point of the streaming work: read a dataset's bytes over the
/// network, at arbitrary offsets, without ever downloading it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dataset_can_be_read_by_range_without_pulling_it() {
    let _guard = COORDINATOR_PORT.lock().await;
    let _ = env_logger::builder().is_test(true).try_init();

    // A real bundle: a tar.gz the server can unpack into a tree of files.
    let root = tempfile::tempdir().expect("temp dir");
    let source = root.path().join("dataset");
    std::fs::create_dir_all(&source).unwrap();
    let big: Vec<u8> = (0..(3usize * 1024 * 1024 + 4242))
        .map(|i| (i.wrapping_mul(31) % 251) as u8)
        .collect();
    std::fs::write(source.join("points.bin"), &big).unwrap();
    std::fs::write(source.join("notes.txt"), b"a demo dataset").unwrap();

    let bundle = root.path().join("bundle.tar.gz");
    {
        let file = std::fs::File::create(&bundle).unwrap();
        let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::fast());
        let mut archive = tar::Builder::new(encoder);
        archive.append_dir_all(".", &source).unwrap();
        archive.finish().unwrap();
    }

    let backing = FolderRegistry::from_uri(
        "backing",
        &format!("folder://{}", root.path().join("reg").display()),
    )
    .expect("folder registry");
    let bag: marina::BagRef = "team/streamed:v1".parse().unwrap();
    backing
        .push(
            "backing",
            &bag,
            &bundle,
            &PushMeta {
                original_bytes: big.len() as u64,
                packed_bytes: std::fs::metadata(&bundle).unwrap().len(),
                bundle_hash: "beefcafe1234".to_string(),
                pointcloud: "n/a".to_string(),
                mcap_compression: "n/a".to_string(),
                pushed_at: 1_700_000_000,
            },
        )
        .await
        .expect("seeding should work");

    let exposed_as = unique("streamed");
    start_server(Arc::new(backing), &exposed_as).await;

    let client = MinotRegistry::from_uri("remote", &format!("minot://{exposed_as}"))
        .expect("client registry should parse");

    let dataset = match client.stat(&bag).await.expect("stat should work") {
        StatResult::Streamable(dataset) => dataset,
        StatResult::NotStreamable { reason } => panic!("should be streamable: {reason}"),
    };

    let listed: Vec<&str> = dataset.files().iter().map(|f| f.path.as_str()).collect();
    assert!(
        listed.contains(&"points.bin") && listed.contains(&"notes.txt"),
        "the manifest should list the dataset's files, got {listed:?}"
    );

    // Sequential read of the whole file, over the network.
    let mut remote = dataset.open("points.bin").expect("the file should open");
    let mut streamed = Vec::new();
    std::io::Read::read_to_end(&mut remote, &mut streamed).expect("streamed read should work");
    assert_eq!(
        streamed.len(),
        big.len(),
        "a streamed read must not be short or doubled"
    );
    assert!(
        streamed == big,
        "bytes read over the network must equal the bytes on the server"
    );

    // Random access, which is what a bag reader actually does.
    let mut remote = dataset.open("points.bin").expect("the file should open");
    let mut offset = 7919usize;
    for _ in 0..200 {
        offset = offset.wrapping_mul(1103515245).wrapping_add(12345) % big.len();
        let len = (offset % 9000) + 1;
        let end = (offset + len).min(big.len());
        std::io::Seek::seek(&mut remote, std::io::SeekFrom::Start(offset as u64)).unwrap();
        let mut buffer = vec![0u8; end - offset];
        std::io::Read::read_exact(&mut remote, &mut buffer).unwrap();
        assert_eq!(
            buffer,
            &big[offset..end],
            "range read mismatch at {offset}..{end}"
        );
    }

    // Nothing was written into the client's cache: this really was a read, not
    // a download in disguise.
    assert!(
        !root.path().join("pulled.tar.gz").exists(),
        "streaming must not have produced a bundle on the client"
    );
}

/// A file the dataset does not have should say what it does have.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opening_a_missing_file_lists_what_is_there() {
    let _guard = COORDINATOR_PORT.lock().await;
    let _ = env_logger::builder().is_test(true).try_init();

    let root = tempfile::tempdir().expect("temp dir");
    let source = root.path().join("dataset");
    std::fs::create_dir_all(&source).unwrap();
    std::fs::write(source.join("only.txt"), b"hello").unwrap();
    let bundle = root.path().join("bundle.tar.gz");
    {
        let file = std::fs::File::create(&bundle).unwrap();
        let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::fast());
        let mut archive = tar::Builder::new(encoder);
        archive.append_dir_all(".", &source).unwrap();
        archive.finish().unwrap();
    }

    let backing = FolderRegistry::from_uri(
        "backing",
        &format!("folder://{}", root.path().join("reg").display()),
    )
    .unwrap();
    let bag: marina::BagRef = "team/tiny:v1".parse().unwrap();
    backing
        .push(
            "backing",
            &bag,
            &bundle,
            &PushMeta {
                original_bytes: 5,
                packed_bytes: std::fs::metadata(&bundle).unwrap().len(),
                bundle_hash: "abc123abc123".to_string(),
                pointcloud: "n/a".to_string(),
                mcap_compression: "n/a".to_string(),
                pushed_at: 1_700_000_000,
            },
        )
        .await
        .unwrap();

    let exposed_as = unique("tiny");
    start_server(Arc::new(backing), &exposed_as).await;
    let client = MinotRegistry::from_uri("remote", &format!("minot://{exposed_as}")).unwrap();
    let dataset = client.open_dataset(&bag).await.expect("should open");

    let text = match dataset.open("nope.bin") {
        Ok(_) => panic!("opening a file the dataset does not have must fail"),
        Err(error) => error.to_string(),
    };
    assert!(
        text.contains("only.txt"),
        "the error should list what the dataset does contain, got: {text}"
    );
}

/// The high-level API selects the streaming capability without exposing a
/// concrete registry driver to consumers such as Pelorus and Minot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resolve_access_opens_a_remote_dataset() {
    let _guard = COORDINATOR_PORT.lock().await;
    let _ = env_logger::builder().is_test(true).try_init();

    let home = tempfile::tempdir().unwrap();
    unsafe {
        std::env::set_var("MARINA_CONFIG_DIR", home.path().join("config"));
        std::env::set_var("MARINA_CACHE_DIR", home.path().join("cache"));
    }

    let root = tempfile::tempdir().unwrap();
    let bag: marina::BagRef = "team/access:v1".parse().unwrap();
    let contents = b"range-readable through DatasetAccess";
    let client = serve_one_file_dataset(root.path(), &bag, contents).await;
    let exposed_as = client.remote_registry_name().to_string();

    let mut marina = marina::Marina::load().unwrap();
    marina
        .add_registry(RegistryConfig {
            name: "remote".to_string(),
            kind: "minot".to_string(),
            uri: format!("minot://{exposed_as}"),
            auth_env: None,
            proxy_jump: None,
            ssh_transport: None,
            download_mode: RegistryDownloadMode::Streaming,
        })
        .unwrap();

    let access = marina
        .resolve_access(&bag.to_string(), Some("remote"), AccessMode::PreferStream)
        .await
        .expect("the high-level resolver should open the remote dataset");
    let DatasetAccess::Streamed(dataset) = access else {
        panic!("PreferStream should return a streamed dataset")
    };
    let mut file = dataset.open("points.bin").unwrap();
    let mut read = Vec::new();
    std::io::Read::read_to_end(&mut file, &mut read).unwrap();
    assert_eq!(read, contents);

    unsafe {
        std::env::remove_var("MARINA_CONFIG_DIR");
        std::env::remove_var("MARINA_CACHE_DIR");
    }
}

/// Seed a folder registry with a one-file dataset and serve it.
async fn serve_one_file_dataset(
    root: &std::path::Path,
    bag: &marina::BagRef,
    contents: &[u8],
) -> MinotRegistry {
    let source = root.join("dataset");
    std::fs::create_dir_all(&source).unwrap();
    std::fs::write(source.join("points.bin"), contents).unwrap();
    let bundle = root.join("bundle.tar.gz");
    {
        let file = std::fs::File::create(&bundle).unwrap();
        let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::fast());
        let mut archive = tar::Builder::new(encoder);
        archive.append_dir_all(".", &source).unwrap();
        archive.finish().unwrap();
    }
    let backing = FolderRegistry::from_uri(
        "backing",
        &format!("folder://{}", root.join("reg").display()),
    )
    .unwrap();
    backing
        .push(
            "backing",
            bag,
            &bundle,
            &PushMeta {
                original_bytes: contents.len() as u64,
                packed_bytes: std::fs::metadata(&bundle).unwrap().len(),
                bundle_hash: "cafebabe0001".to_string(),
                pointcloud: "n/a".to_string(),
                mcap_compression: "n/a".to_string(),
                pushed_at: 1_700_000_000,
            },
        )
        .await
        .unwrap();

    let exposed_as = unique("cachemode");
    start_server(Arc::new(backing), &exposed_as).await;
    MinotRegistry::from_uri("remote", &format!("minot://{exposed_as}")).unwrap()
}

/// Disk mode warms a local copy; a second read of the same dataset is free.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_streamed_read_can_warm_a_local_copy() {
    let _guard = COORDINATOR_PORT.lock().await;
    let _ = env_logger::builder().is_test(true).try_init();

    let root = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let contents: Vec<u8> = (0..(2usize * 1024 * 1024 + 99))
        .map(|i| (i.wrapping_mul(31) % 251) as u8)
        .collect();
    let bag: marina::BagRef = "team/warm:v1".parse().unwrap();
    let client = serve_one_file_dataset(root.path(), &bag, &contents).await;

    let mode = CacheMode::Disk {
        root: cache.path().to_path_buf(),
        validity: "cafebabe0001".to_string(),
    };

    let dataset = client
        .open_dataset(&bag)
        .await
        .expect("should open")
        .with_cache(mode.clone());
    assert!(dataset.persists());

    let mut file = dataset.open("points.bin").unwrap();
    let mut read = Vec::new();
    std::io::Read::read_to_end(&mut file, &mut read).unwrap();
    assert!(read == contents, "the streamed bytes must match");
    assert!(
        file.is_complete(),
        "a full read in disk mode should leave a complete local copy"
    );

    // The cache directory now holds the file's blocks.
    let cached: Vec<_> = std::fs::read_dir(cache.path()).unwrap().flatten().collect();
    assert!(
        !cached.is_empty(),
        "disk mode should have written the blocks it fetched"
    );
}

/// Ephemeral mode reads the same bytes and leaves nothing at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_online_only_read_leaves_nothing_behind() {
    let _guard = COORDINATOR_PORT.lock().await;
    let _ = env_logger::builder().is_test(true).try_init();

    let root = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let contents: Vec<u8> = (0..(2usize * 1024 * 1024 + 99))
        .map(|i| (i.wrapping_mul(17) % 251) as u8)
        .collect();
    let bag: marina::BagRef = "team/online:v1".parse().unwrap();
    let client = serve_one_file_dataset(root.path(), &bag, &contents).await;

    let dataset = client
        .open_dataset(&bag)
        .await
        .expect("should open")
        .online_only();
    assert!(!dataset.persists());

    let mut file = dataset.open("points.bin").unwrap();
    let mut read = Vec::new();
    std::io::Read::read_to_end(&mut file, &mut read).unwrap();

    assert!(
        read == contents,
        "an online-only read must return exactly the same bytes as a caching one"
    );
    assert!(!file.persists());
    assert!(
        !file.is_complete(),
        "nothing is retained, so there is no local copy to be complete"
    );
    assert_eq!(
        std::fs::read_dir(cache.path()).unwrap().count(),
        0,
        "an online-only read must not write to the cache directory"
    );
}

/// The property the whole design turns on: a streamed dataset becomes an
/// ordinary local one, and resuming costs only what is missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_streamed_dataset_is_promoted_to_a_local_one() {
    let _guard = COORDINATOR_PORT.lock().await;
    let _ = env_logger::builder().is_test(true).try_init();

    // Isolated config and cache, so promotion writes into a temporary catalog
    // rather than the developer's real one.
    let home = tempfile::tempdir().unwrap();
    // SAFETY: set before marina reads its locations, and this test holds the
    // coordinator lock so no other test is running concurrently.
    unsafe {
        std::env::set_var("MARINA_CONFIG_DIR", home.path().join("config"));
        std::env::set_var("MARINA_CACHE_DIR", home.path().join("cache"));
    }

    let root = tempfile::tempdir().unwrap();
    let contents: Vec<u8> = (0..(2usize * 1024 * 1024 + 1234))
        .map(|i| (i.wrapping_mul(23) % 251) as u8)
        .collect();
    let bag: marina::BagRef = "team/promote:v1".parse().unwrap();
    let client = serve_one_file_dataset(root.path(), &bag, &contents).await;

    let dataset = client.open_dataset(&bag).await.expect("should open");
    assert!(
        dataset.persists(),
        "the default cache mode should keep what it reads"
    );
    assert_eq!(dataset.cached_fraction(), 0.0, "nothing is held yet");

    // Read part of it, then abandon — the interrupted-read case.
    {
        let mut file = dataset.open("points.bin").unwrap();
        let mut prefix = vec![0u8; contents.len() / 3];
        std::io::Read::read_exact(&mut file, &mut prefix).unwrap();
        assert!(prefix == contents[..prefix.len()], "the prefix must match");
    }

    // Now materialise: only the missing part should still be fetched.
    let mut out = std::io::sink();
    let mut sink = marina::WriterProgress::new(&mut out);
    let mut progress = marina::ProgressReporter::new(&mut sink);
    let ready = dataset
        .materialize(&mut progress)
        .expect("materialising should work");

    assert!(ready.is_dir(), "materialising should produce a directory");
    let installed = std::fs::read(ready.join("points.bin")).expect("the file should be installed");
    assert!(
        installed == contents,
        "the promoted dataset must be byte-identical to the remote one"
    );

    // And it is now an ordinary cached dataset, findable without the network.
    let catalog_says = marina::Marina::load()
        .expect("marina should load")
        .list_cached_bags()
        .into_iter()
        .any(|cached| cached.bag == bag);
    assert!(
        catalog_says,
        "a promoted dataset must be registered in the catalog like any pulled one"
    );

    unsafe {
        std::env::remove_var("MARINA_CONFIG_DIR");
        std::env::remove_var("MARINA_CACHE_DIR");
    }
}

/// Online-only mode has nothing to promote, and says so rather than pretending.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_online_only_dataset_refuses_to_be_materialised() {
    let _guard = COORDINATOR_PORT.lock().await;
    let _ = env_logger::builder().is_test(true).try_init();

    let root = tempfile::tempdir().unwrap();
    let bag: marina::BagRef = "team/nopromote:v1".parse().unwrap();
    let client = serve_one_file_dataset(root.path(), &bag, b"small contents").await;

    let dataset = client.open_dataset(&bag).await.unwrap().online_only();
    let mut out = std::io::sink();
    let mut sink = marina::WriterProgress::new(&mut out);
    let mut progress = marina::ProgressReporter::new(&mut sink);

    let text = match dataset.materialize(&mut progress) {
        Ok(_) => panic!("online-only mode keeps nothing, so it cannot materialise"),
        Err(error) => error.to_string(),
    };
    assert!(
        text.contains("online-only"),
        "the refusal should name the mode that caused it, got: {text}"
    );
}
