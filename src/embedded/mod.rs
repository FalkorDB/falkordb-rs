/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Embedded FalkorDB server support.
//!
//! This module provides functionality to spawn and manage an embedded Redis server
//! with the FalkorDB module loaded, allowing for in-process graph database operations.
//!
//! **Note**: This embedded server uses Unix domain sockets and is only supported on
//! Unix-like operating systems (Linux, macOS, BSD). Windows is not currently supported
//! due to limited Unix socket support.

#[cfg(windows)]
compile_error!(
    "The `embedded` feature is only supported on Unix-like systems \
     because it relies on Unix domain sockets. Windows is not supported."
);
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::{fs, thread};

use crate::{FalkorDBError, FalkorResult};

#[cfg(all(feature = "embedded", not(feature = "embedded-bundle")))]
pub mod download;
pub mod provision;

#[cfg(feature = "embedded-bundle")]
pub mod bundle;

// Maximum length for Unix socket paths (typically 104-108 bytes on most Unix systems)
const MAX_SOCKET_PATH_LENGTH: usize = 104;

// How long to allow a module download to run before giving up.
#[cfg(all(feature = "embedded", not(feature = "embedded-bundle")))]
const MODULE_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(300);

// Counter for ensuring unique temp directories across multiple instances
static INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Configuration for an embedded FalkorDB server instance.
///
/// # Module source
///
/// How the FalkorDB **module** (`falkordb.so`) is located depends on the enabled
/// cargo feature:
///
/// * **`embedded`** (runtime download): with `auto_download` enabled (the
///   default), a missing module is downloaded from the official FalkorDB GitHub
///   releases on first start, checksum-verified and cached locally. **Needs
///   network at runtime** unless already cached or found locally.
/// * **`embedded-bundle`** (build-time embed): the module is fetched for the
///   build target at compile time and embedded in the binary, so the running
///   process performs **no network access**. `auto_download` and
///   `falkordb_version` have no effect (the version is chosen at build time via
///   `FALKORDB_EMBEDDED_MODULE_VERSION`); setting `falkordb_version` is rejected.
///
/// In both modes, an explicit `falkordb_module_path` always wins.
///
/// The `redis-server` binary is **never** downloaded: it is always located on
/// the host via `redis_server_path` or `PATH`. Install it from your package
/// manager (e.g. `brew install redis`, `apt-get install redis-server`) or point
/// `redis_server_path` at an existing FalkorDB/Redis install. FalkorDB requires
/// redis-server **8.0 or newer**.
///
/// # License Note
///
/// The FalkorDB module binary is licensed under the Server Side Public License
/// (SSPL) v1. With `embedded-bundle` it is **embedded into your binary**, so you
/// are responsible for complying with its license when distributing. See
/// <https://www.falkordb.com/> for licensing details.
#[derive(Debug, Clone)]
pub struct EmbeddedConfig {
    /// Path to the redis-server executable. If None, searches in PATH.
    pub redis_server_path: Option<PathBuf>,
    /// Path to the FalkorDB module (.so file). If None, searches common
    /// locations and (when `auto_download` is enabled) downloads it; with the
    /// `embedded-bundle` feature, the build-time-embedded module is used.
    pub falkordb_module_path: Option<PathBuf>,
    /// Directory for the database files. If None, creates a temporary directory.
    pub db_dir: Option<PathBuf>,
    /// Database filename.
    pub db_filename: String,
    /// Path to the Unix socket. If None, creates one in a temporary directory.
    pub socket_path: Option<PathBuf>,
    /// Maximum time to wait for server startup.
    pub start_timeout: Duration,
    /// Enable automatic downloading of the missing FalkorDB **module** from the
    /// official FalkorDB releases. Defaults to `true`. When `false`, behaves
    /// like a lookup-only resolver (explicit path or system locations) and
    /// errors if the module is not found. `redis-server` is never downloaded.
    /// **No effect with the `embedded-bundle` feature** (the module is embedded
    /// at build time and never downloaded at runtime).
    pub auto_download: bool,
    /// Override the FalkorDB version to download. If None, uses the version this
    /// client pins (`provision::FALKORDB_VERSION`). Versions other than the
    /// pinned one are downloaded without checksum verification (no pinned hash).
    /// **Rejected with the `embedded-bundle` feature**: the bundled version is
    /// selected at build time via `FALKORDB_EMBEDDED_MODULE_VERSION`.
    pub falkordb_version: Option<String>,
    /// Override the cache directory for downloaded (or build-time-bundled,
    /// then extracted) binaries. If None, uses platform-specific defaults or the
    /// `FALKORDB_RS_CACHE_DIR` env var.
    ///
    /// The extracted module is verified against the bundled bytes before reuse,
    /// but on a shared multi-user host prefer a private `cache_dir` (or
    /// `FALKORDB_RS_CACHE_DIR`): the no-`HOME` fallback under the system temp
    /// directory is world-writable.
    pub cache_dir: Option<PathBuf>,
}

impl Default for EmbeddedConfig {
    fn default() -> Self {
        Self {
            redis_server_path: None,
            falkordb_module_path: None,
            db_dir: None,
            db_filename: "falkordb.rdb".to_string(),
            socket_path: None,
            start_timeout: Duration::from_secs(10),
            auto_download: true,
            falkordb_version: None,
            cache_dir: None,
        }
    }
}

/// Manages an embedded FalkorDB server instance.
///
/// When created, spawns a redis-server process with the FalkorDB module loaded.
/// The server uses a Unix socket for connections and is automatically shut down
/// when the instance is dropped.
pub struct EmbeddedServer {
    /// The Redis server process.
    process: Child,
    /// Path to the Unix socket.
    socket_path: PathBuf,
    /// Directory containing temporary files (if created).
    temp_dir: Option<PathBuf>,
    /// Path to the configuration file.
    config_file: PathBuf,
}

/// Whether a `redis-server --version` output string reports a major version
/// older than the 8.0 the FalkorDB module requires. Output that can't be parsed
/// is treated as new-enough (`false`), leaving the real load attempt to surface
/// any genuine incompatibility. Example input: `Redis server v=8.6.3 sha=…`.
fn redis_version_too_old(version_text: &str) -> bool {
    const MIN_MAJOR: u32 = 8;
    version_text
        .split("v=")
        .nth(1)
        .and_then(|rest| rest.split('.').next())
        .and_then(|major| major.trim().parse::<u32>().ok())
        .is_some_and(|major| major < MIN_MAJOR)
}

impl EmbeddedServer {
    /// Creates and starts a new embedded FalkorDB server.
    ///
    /// # Arguments
    /// * `config` - Configuration for the embedded server
    ///
    /// # Returns
    /// A new [`EmbeddedServer`] instance
    ///
    /// # Errors
    /// Returns an error if:
    /// - redis-server or FalkorDB module cannot be found
    /// - The server process fails to start
    /// - The server doesn't respond within the timeout period
    pub fn start(config: EmbeddedConfig) -> FalkorResult<Self> {
        // Fail fast on an obviously-invalid explicit socket path before doing any
        // environment-dependent work (binary lookup, libomp check, downloads).
        if let Some(ref socket_path) = config.socket_path {
            Self::validate_socket_path_len(socket_path)?;
        }

        // Find redis-server executable
        let redis_server = Self::find_redis_server(&config)?;

        // The FalkorDB module hard-requires redis-server >= 8.0; fail fast with a
        // clear message rather than an opaque startup timeout.
        Self::check_redis_version(&redis_server, config.start_timeout)?;

        // Find FalkorDB module
        let falkordb_module = Self::find_falkordb_module(&config)?;

        // Set up directories and paths
        let (db_dir, temp_dir) = Self::setup_db_dir(&config)?;
        let socket_path = Self::setup_socket_path(&config, temp_dir.as_deref())?;

        // Validate the resolved socket path length (covers the derived-path case).
        Self::validate_socket_path_len(&socket_path)?;

        let config_file = Self::create_config_file(&db_dir, &socket_path, &config.db_filename)?;

        // Capture redis-server's stderr to a log file so a failed module load
        // (wrong arch, missing libomp, redis too old, …) surfaces a real cause
        // instead of only a timeout. Falls back to null if the log can't open.
        // The name is unique so a caller-provided persistent `db_dir` (or several
        // servers sharing one) can't clobber an unrelated file.
        let log_path = db_dir.join(format!(
            "redis-server.{}.{}.log",
            std::process::id(),
            INSTANCE_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        let stderr = fs::File::create(&log_path)
            .map(Stdio::from)
            .unwrap_or_else(|_| Stdio::null());

        // Start redis-server with FalkorDB module (no daemonize to keep process handle valid)
        let mut command = Command::new(&redis_server);
        command
            .arg(&config_file)
            .arg("--loadmodule")
            .arg(&falkordb_module)
            .stdout(Stdio::null())
            .stderr(stderr);

        let mut process = command.spawn().map_err(|e| {
            FalkorDBError::EmbeddedServerError(format!("Failed to start redis-server: {}", e))
        })?;

        // Wait for the socket to be created
        let start_time = std::time::Instant::now();
        while !socket_path.exists() {
            // If redis exited early (e.g. the module failed to load), stop waiting
            // and report its stderr instead of burning the whole timeout.
            let exited = matches!(process.try_wait(), Ok(Some(_)));
            if exited || start_time.elapsed() > config.start_timeout {
                let _ = process.kill();
                let _ = process.wait();
                let reason = if exited {
                    "redis-server exited before the socket was ready"
                } else {
                    "Timed out waiting for server to start"
                };
                return Err(Self::start_error(
                    reason,
                    &log_path,
                    &config_file,
                    temp_dir.as_deref(),
                ));
            }
            thread::sleep(Duration::from_millis(100));
        }

        // Give the server a bit more time to be fully ready
        thread::sleep(Duration::from_millis(500));

        // The socket exists, but make sure redis didn't create it and immediately
        // exit (or that a stale socket wasn't observed) before declaring success.
        if matches!(process.try_wait(), Ok(Some(_))) {
            let _ = process.wait();
            return Err(Self::start_error(
                "redis-server exited immediately after the socket appeared",
                &log_path,
                &config_file,
                temp_dir.as_deref(),
            ));
        }

        // The server is up; the captured log is no longer needed.
        let _ = fs::remove_file(&log_path);

        Ok(Self {
            process,
            socket_path,
            temp_dir,
            config_file,
        })
    }

    /// Read the last few KiB of the redis-server log for inclusion in a startup
    /// error. Returns an empty string when the log is missing or empty.
    fn read_log_tail(log_path: &Path) -> String {
        const MAX: usize = 4096;
        // Decode lossily so a log with stray non-UTF-8 bytes still surfaces its
        // content (as a diagnostic) instead of being dropped entirely.
        let bytes = fs::read(log_path).unwrap_or_default();
        let text = String::from_utf8_lossy(&bytes);
        let trimmed = text.trim();
        if trimmed.len() <= MAX {
            return trimmed.to_string();
        }
        // Keep the last ~MAX bytes, snapped up to a char boundary so multibyte
        // redis output can never panic the slice.
        let mut start = trimmed.len() - MAX;
        while !trimmed.is_char_boundary(start) {
            start += 1;
        }
        format!("…{}", &trimmed[start..])
    }

    /// Build a startup error that includes the redis log tail, cleaning up the
    /// config file, log file and temp dir. The caller is responsible for having
    /// already reaped the redis process.
    fn start_error(
        reason: &str,
        log_path: &Path,
        config_file: &Path,
        temp_dir: Option<&Path>,
    ) -> FalkorDBError {
        let log_tail = Self::read_log_tail(log_path);
        let _ = fs::remove_file(config_file);
        let _ = fs::remove_file(log_path);
        if let Some(temp_dir) = temp_dir {
            let _ = fs::remove_dir_all(temp_dir);
        }
        FalkorDBError::EmbeddedServerError(if log_tail.is_empty() {
            reason.to_string()
        } else {
            format!("{reason}. redis-server output:\n{log_tail}")
        })
    }

    /// Returns the Unix socket path for connecting to this server.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Returns a connection string for this embedded server.
    pub fn connection_string(&self) -> String {
        format!("unix://{}", self.socket_path.display())
    }

    /// The FalkorDB release version of the module embedded at build time (the
    /// `embedded-bundle` feature). Set via the `FALKORDB_EMBEDDED_MODULE_VERSION`
    /// build-time environment variable (defaults to the version this client pins).
    #[cfg(feature = "embedded-bundle")]
    pub fn bundled_module_version() -> &'static str {
        bundle::BUNDLED_MODULE_VERSION
    }

    /// The platform tag (e.g. `linux-x64-glibc`) the build-time bundled module
    /// targets (the `embedded-bundle` feature).
    #[cfg(feature = "embedded-bundle")]
    pub fn bundled_module_platform() -> &'static str {
        bundle::BUNDLED_MODULE_PLATFORM
    }

    /// Returns an error if a Unix socket path exceeds the platform limit.
    fn validate_socket_path_len(socket_path: &Path) -> FalkorResult<()> {
        if socket_path.as_os_str().len() > MAX_SOCKET_PATH_LENGTH {
            return Err(FalkorDBError::EmbeddedServerError(format!(
                "Socket path is too long ({} bytes, max {}). Please specify a shorter path in EmbeddedConfig.",
                socket_path.as_os_str().len(),
                MAX_SOCKET_PATH_LENGTH
            )));
        }
        Ok(())
    }

    fn find_redis_server(config: &EmbeddedConfig) -> FalkorResult<PathBuf> {
        if let Some(ref path) = config.redis_server_path {
            if path.exists() {
                return Ok(path.clone());
            }
            return Err(FalkorDBError::EmbeddedServerError(format!(
                "redis-server not found at: {}",
                path.display()
            )));
        }

        // Try to find in PATH
        which::which("redis-server")
            .map_err(|_| FalkorDBError::EmbeddedServerError(
                "redis-server not found in PATH. Please install Redis or specify the path in EmbeddedConfig".to_string()
            ))
    }

    /// Pre-flight the redis-server version: the FalkorDB module requires
    /// redis-server >= 8.0 and refuses to load on anything older, so checking the
    /// version up front turns an opaque "timed out waiting for server" into an
    /// actionable error. A version that can't be parsed is allowed through (the
    /// real load attempt will still surface any incompatibility).
    fn check_redis_version(
        redis_server: &Path,
        start_timeout: Duration,
    ) -> FalkorResult<()> {
        let Some(text) = Self::redis_version_output(redis_server, start_timeout) else {
            // Couldn't get a version in time; let the start attempt (bounded by
            // `start_timeout`) report any real problem.
            return Ok(());
        };
        if redis_version_too_old(&text) {
            return Err(FalkorDBError::EmbeddedServerError(format!(
                "redis-server at {} reports version {}, but FalkorDB requires redis-server \
                 8.0 or newer. Install a newer redis-server or point \
                 `redis_server_path`/`PATH` at one.",
                redis_server.display(),
                text.trim()
            )));
        }
        Ok(())
    }

    /// Run `redis-server --version`, bounded by the caller's `start_timeout`
    /// (capped at 10s) so a hanging or wrong binary can't block startup beyond
    /// the budget the caller allotted. Returns its stdout, or `None` on spawn
    /// failure / timeout.
    fn redis_version_output(
        redis_server: &Path,
        start_timeout: Duration,
    ) -> Option<String> {
        const VERSION_TIMEOUT_CAP: Duration = Duration::from_secs(10);
        let budget = start_timeout.min(VERSION_TIMEOUT_CAP);
        let mut child = Command::new(redis_server)
            .arg("--version")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;

        let deadline = std::time::Instant::now() + budget;
        loop {
            match child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) if std::time::Instant::now() >= deadline => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
                Ok(None) => thread::sleep(Duration::from_millis(50)),
                Err(_) => return None,
            }
        }
        // `--version` output is tiny, so reading after exit cannot deadlock.
        let output = child.wait_with_output().ok()?;
        Some(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    // `return`s are load-bearing across feature combinations (the download path
    // below is compiled out under `embedded-bundle`), so the bundle branch's
    // explicit return can look "needless" in the bundle-only expansion.
    #[allow(clippy::needless_return)]
    fn find_falkordb_module(config: &EmbeddedConfig) -> FalkorResult<PathBuf> {
        // Resolution order: macOS libomp prerequisite → explicit path → cache
        // (if auto_download) → system locations → download (if auto_download) → error.

        // On macOS the module cannot be loaded without libomp regardless of where
        // it comes from, so this prerequisite is checked even for an explicit path.
        #[cfg(target_os = "macos")]
        {
            #[cfg(feature = "tracing")]
            tracing::debug!("Checking macOS libomp requirement");
            provision::check_macos_libomp()?;
        }

        // Step 1: Explicit path takes precedence over everything else.
        if let Some(ref path) = config.falkordb_module_path {
            #[cfg(feature = "tracing")]
            tracing::debug!("Checking explicit FalkorDB module path: {}", path.display());
            if path.exists() {
                #[cfg(feature = "tracing")]
                tracing::info!("Using explicit FalkorDB module at: {}", path.display());
                return Ok(path.clone());
            }
            return Err(FalkorDBError::EmbeddedServerError(format!(
                "FalkorDB module not found at: {}",
                path.display()
            )));
        }

        // Step 2 (bundle mode): the module was fetched for this target at build
        // time and embedded in the binary. Extract it once and use it — the
        // runtime never touches the network. Authoritative over the download
        // path below, so enabling both features still guarantees offline start.
        #[cfg(feature = "embedded-bundle")]
        {
            if config.falkordb_version.is_some() {
                return Err(FalkorDBError::EmbeddedServerError(
                    "`falkordb_version` has no effect with the `embedded-bundle` feature: the \
                     module version is selected at build time via the \
                     FALKORDB_EMBEDDED_MODULE_VERSION environment variable. Unset \
                     `falkordb_version`, or set `falkordb_module_path` to use a specific module."
                        .to_string(),
                ));
            }
            #[cfg(feature = "tracing")]
            tracing::info!(
                "Using build-time bundled FalkorDB module (version {})",
                bundle::BUNDLED_MODULE_VERSION
            );
            return bundle::materialize_bundled_module(config.cache_dir.as_deref());
        }

        // Steps 3-6 (runtime-download mode only): cache → system → download →
        // error. Compiled out under `embedded-bundle` (which returns above).
        #[cfg(all(feature = "embedded", not(feature = "embedded-bundle")))]
        {
            let version = config
                .falkordb_version
                .as_deref()
                .unwrap_or(provision::FALKORDB_VERSION);

            // Step 3: Reuse a previously downloaded module from the cache (verified
            // against the pinned checksum, so a corrupted cache is not trusted).
            if config.auto_download {
                let platform = provision::Platform::detect();
                if let Ok(Some(cached_path)) = download::cached_verified_module(
                    &platform,
                    version,
                    config.cache_dir.as_deref(),
                ) {
                    #[cfg(feature = "tracing")]
                    tracing::info!("Using cached FalkorDB module at: {}", cached_path.display());
                    return Ok(cached_path);
                }
            }

            // Step 4: Use an already-installed module from a common system location.
            #[cfg(feature = "tracing")]
            tracing::debug!("Searching for FalkorDB module in common system locations");
            let common_paths = [
                PathBuf::from("/usr/lib/redis/modules/falkordb.so"),
                PathBuf::from("/usr/local/lib/redis/modules/falkordb.so"),
                PathBuf::from("/opt/homebrew/lib/redis/modules/falkordb.so"),
                PathBuf::from("./falkordb.so"),
            ];
            for path in common_paths {
                if path.exists() {
                    #[cfg(feature = "tracing")]
                    tracing::info!(
                        "Found FalkorDB module at system location: {}",
                        path.display()
                    );
                    return Ok(path);
                }
            }

            // Step 5: Download from the official releases. The download error is
            // propagated directly so callers see the actionable cause (e.g.
            // unsupported platform, checksum mismatch, or a network failure).
            if config.auto_download {
                #[cfg(feature = "tracing")]
                tracing::info!("FalkorDB module not found locally, downloading version {version}");
                let platform = provision::Platform::detect();
                return download::download_falkordb_module(
                    &platform,
                    version,
                    config.cache_dir.as_deref(),
                    MODULE_DOWNLOAD_TIMEOUT,
                );
            }

            // Step 6: Lookup-only mode and nothing was found.
            Err(FalkorDBError::EmbeddedServerError(
                "FalkorDB module (falkordb.so) not found. Install FalkorDB, set falkordb_module_path \
                 in EmbeddedConfig, or enable auto_download to fetch it automatically."
                    .to_string(),
            ))
        }

        // No module source compiled in (only `embedded-core` is enabled).
        #[cfg(not(any(feature = "embedded", feature = "embedded-bundle")))]
        {
            Err(FalkorDBError::EmbeddedServerError(
                "No embedded FalkorDB module source is available. Set `falkordb_module_path` in \
                 EmbeddedConfig, or enable the `embedded` (runtime download) or `embedded-bundle` \
                 (build-time embed) cargo feature."
                    .to_string(),
            ))
        }
    }

    fn setup_db_dir(config: &EmbeddedConfig) -> FalkorResult<(PathBuf, Option<PathBuf>)> {
        if let Some(ref dir) = config.db_dir {
            if !dir.exists() {
                fs::create_dir_all(dir).map_err(|e| {
                    FalkorDBError::EmbeddedServerError(format!(
                        "Failed to create db directory: {}",
                        e
                    ))
                })?;
                // Set restrictive permissions on Unix
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mut perms = fs::metadata(dir)
                        .map_err(|e| {
                            FalkorDBError::EmbeddedServerError(format!(
                                "Failed to get directory metadata: {}",
                                e
                            ))
                        })?
                        .permissions();
                    perms.set_mode(0o700);
                    fs::set_permissions(dir, perms).map_err(|e| {
                        FalkorDBError::EmbeddedServerError(format!(
                            "Failed to set directory permissions: {}",
                            e
                        ))
                    })?;
                }
            }
            Ok((dir.clone(), None))
        } else {
            // Create a temporary directory with unique name using counter and timestamp
            let temp_base = std::env::temp_dir();
            let instance_id = INSTANCE_COUNTER.fetch_add(1, Ordering::SeqCst);
            let temp_name = format!(
                "falkordb_{}_{}_{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
                instance_id
            );
            let temp_dir = temp_base.join(temp_name);

            fs::create_dir_all(&temp_dir).map_err(|e| {
                FalkorDBError::EmbeddedServerError(format!(
                    "Failed to create temp directory: {}",
                    e
                ))
            })?;

            // Set restrictive permissions
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mut perms = fs::metadata(&temp_dir)
                    .map_err(|e| {
                        FalkorDBError::EmbeddedServerError(format!(
                            "Failed to get directory metadata: {}",
                            e
                        ))
                    })?
                    .permissions();
                perms.set_mode(0o700);
                fs::set_permissions(&temp_dir, perms).map_err(|e| {
                    FalkorDBError::EmbeddedServerError(format!(
                        "Failed to set directory permissions: {}",
                        e
                    ))
                })?;
            }

            Ok((temp_dir.clone(), Some(temp_dir)))
        }
    }

    fn setup_socket_path(
        config: &EmbeddedConfig,
        temp_dir: Option<&Path>,
    ) -> FalkorResult<PathBuf> {
        if let Some(ref path) = config.socket_path {
            Ok(path.clone())
        } else if let Some(temp_dir) = temp_dir {
            Ok(temp_dir.join("falkordb.sock"))
        } else {
            // Use the system temp directory for the socket with unique name
            let temp_base = std::env::temp_dir();
            let instance_id = INSTANCE_COUNTER.fetch_add(1, Ordering::SeqCst);
            let temp_name = format!(
                "falkordb_sock_{}_{}_{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
                instance_id
            );
            let temp_dir = temp_base.join(temp_name);

            fs::create_dir_all(&temp_dir).map_err(|e| {
                FalkorDBError::EmbeddedServerError(format!(
                    "Failed to create temp directory: {}",
                    e
                ))
            })?;

            // Set restrictive permissions
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mut perms = fs::metadata(&temp_dir)
                    .map_err(|e| {
                        FalkorDBError::EmbeddedServerError(format!(
                            "Failed to get directory metadata: {}",
                            e
                        ))
                    })?
                    .permissions();
                perms.set_mode(0o700);
                fs::set_permissions(&temp_dir, perms).map_err(|e| {
                    FalkorDBError::EmbeddedServerError(format!(
                        "Failed to set directory permissions: {}",
                        e
                    ))
                })?;
            }

            Ok(temp_dir.join("falkordb.sock"))
        }
    }

    fn create_config_file(
        db_dir: &Path,
        socket_path: &Path,
        db_filename: &str,
    ) -> FalkorResult<PathBuf> {
        let config_path = db_dir.join("falkordb.conf");
        let config_content = format!(
            r#"
# FalkorDB Embedded Server Configuration
port 0
unixsocket {}
unixsocketperm 700
dir {}
dbfilename {}
save ""
appendonly no
"#,
            socket_path.display(),
            db_dir.display(),
            db_filename
        );

        fs::write(&config_path, config_content).map_err(|e| {
            FalkorDBError::EmbeddedServerError(format!("Failed to write config file: {}", e))
        })?;

        Ok(config_path)
    }
}

impl Drop for EmbeddedServer {
    fn drop(&mut self) {
        // Try to kill the process gracefully
        let _ = self.process.kill();
        let _ = self.process.wait();

        // Remove the config file first
        let _ = fs::remove_file(&self.config_file);

        // Remove the socket file if it exists
        if self.socket_path.exists() {
            let _ = fs::remove_file(&self.socket_path);
        }

        // Finally, clean up the temporary directory (which may contain config and socket)
        if let Some(ref temp_dir) = self.temp_dir {
            let _ = fs::remove_dir_all(temp_dir);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_embedded_config_default() {
        let config = EmbeddedConfig::default();
        assert!(config.redis_server_path.is_none());
        assert!(config.falkordb_module_path.is_none());
        assert!(config.db_dir.is_none());
        assert_eq!(config.db_filename, "falkordb.rdb");
        assert!(config.socket_path.is_none());
        assert_eq!(config.start_timeout, Duration::from_secs(10));
        assert!(config.auto_download);
        assert!(config.falkordb_version.is_none());
        assert!(config.cache_dir.is_none());
    }

    #[test]
    fn test_embedded_config_custom() {
        let config = EmbeddedConfig {
            redis_server_path: Some(PathBuf::from("/custom/redis-server")),
            falkordb_module_path: Some(PathBuf::from("/custom/falkordb.so")),
            db_dir: Some(PathBuf::from("/custom/db")),
            db_filename: "custom.rdb".to_string(),
            socket_path: Some(PathBuf::from("/custom/socket.sock")),
            start_timeout: Duration::from_secs(5),
            auto_download: false,
            falkordb_version: Some("v4.0.0".to_string()),
            cache_dir: Some(PathBuf::from("/custom/cache")),
        };

        assert_eq!(
            config.redis_server_path,
            Some(PathBuf::from("/custom/redis-server"))
        );
        assert_eq!(
            config.falkordb_module_path,
            Some(PathBuf::from("/custom/falkordb.so"))
        );
        assert_eq!(config.db_dir, Some(PathBuf::from("/custom/db")));
        assert_eq!(config.db_filename, "custom.rdb");
        assert_eq!(
            config.socket_path,
            Some(PathBuf::from("/custom/socket.sock"))
        );
        assert_eq!(config.start_timeout, Duration::from_secs(5));
        assert!(!config.auto_download);
        assert_eq!(config.falkordb_version, Some("v4.0.0".to_string()));
        assert_eq!(config.cache_dir, Some(PathBuf::from("/custom/cache")));
    }

    #[test]
    fn test_embedded_config_clone() {
        let config1 = EmbeddedConfig {
            redis_server_path: Some(PathBuf::from("/path/redis")),
            falkordb_module_path: Some(PathBuf::from("/path/falkordb.so")),
            db_dir: Some(PathBuf::from("/path/db")),
            db_filename: "test.rdb".to_string(),
            socket_path: Some(PathBuf::from("/path/socket")),
            start_timeout: Duration::from_secs(15),
            auto_download: false,
            falkordb_version: Some("v4.5.0".to_string()),
            cache_dir: Some(PathBuf::from("/path/cache")),
        };

        let config2 = config1.clone();
        assert_eq!(config1.redis_server_path, config2.redis_server_path);
        assert_eq!(config1.falkordb_module_path, config2.falkordb_module_path);
        assert_eq!(config1.db_dir, config2.db_dir);
        assert_eq!(config1.db_filename, config2.db_filename);
        assert_eq!(config1.socket_path, config2.socket_path);
        assert_eq!(config1.start_timeout, config2.start_timeout);
        assert_eq!(config1.auto_download, config2.auto_download);
        assert_eq!(config1.falkordb_version, config2.falkordb_version);
        assert_eq!(config1.cache_dir, config2.cache_dir);
    }

    #[test]
    fn test_find_redis_server_with_invalid_path() {
        let config = EmbeddedConfig {
            redis_server_path: Some(PathBuf::from("/definitely/does/not/exist/redis-server")),
            ..Default::default()
        };

        let result = EmbeddedServer::find_redis_server(&config);
        assert!(result.is_err());
        if let Err(FalkorDBError::EmbeddedServerError(msg)) = result {
            assert!(msg.contains("redis-server not found"));
        }
    }

    #[test]
    fn test_find_falkordb_module_with_invalid_path() {
        let config = EmbeddedConfig {
            falkordb_module_path: Some(PathBuf::from("/definitely/does/not/exist/falkordb.so")),
            ..Default::default()
        };

        let result = EmbeddedServer::find_falkordb_module(&config);
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        // On macOS the libomp prerequisite is checked before the explicit path,
        // so a missing libomp surfaces first; otherwise the path error is returned.
        if provision::check_macos_libomp().is_ok() {
            assert!(msg.contains("FalkorDB module not found"), "got: {msg}");
        } else {
            assert!(msg.contains("libomp"), "got: {msg}");
        }
    }

    #[test]
    fn test_setup_db_dir_with_custom_path() {
        let temp_dir = std::env::temp_dir().join(format!("test_db_{}", std::process::id()));
        let config = EmbeddedConfig {
            db_dir: Some(temp_dir.clone()),
            ..Default::default()
        };

        let result = EmbeddedServer::setup_db_dir(&config);
        assert!(result.is_ok());

        let (db_dir, temp_dir_opt) = result.unwrap();
        assert_eq!(db_dir, temp_dir);
        assert!(temp_dir_opt.is_none()); // Should not create temp when path is provided

        // Cleanup
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_setup_db_dir_creates_temp() {
        let config = EmbeddedConfig {
            db_dir: None,
            ..Default::default()
        };

        let result = EmbeddedServer::setup_db_dir(&config);
        assert!(result.is_ok());

        let (db_dir, temp_dir_opt) = result.unwrap();
        assert!(db_dir.exists());
        assert!(temp_dir_opt.is_some());
        assert_eq!(temp_dir_opt.as_ref().unwrap(), &db_dir);

        // Cleanup
        let _ = fs::remove_dir_all(&db_dir);
    }

    #[test]
    fn test_setup_socket_path_with_custom_path() {
        let socket_path = PathBuf::from("/custom/path/socket.sock");
        let config = EmbeddedConfig {
            socket_path: Some(socket_path.clone()),
            ..Default::default()
        };

        let result = EmbeddedServer::setup_socket_path(&config, None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), socket_path);
    }

    #[test]
    fn test_setup_socket_path_with_temp_dir() {
        let temp_dir = std::env::temp_dir().join(format!("test_sock_{}", std::process::id()));
        fs::create_dir_all(&temp_dir).unwrap();

        let config = EmbeddedConfig {
            socket_path: None,
            ..Default::default()
        };

        let result = EmbeddedServer::setup_socket_path(&config, Some(&temp_dir));
        assert!(result.is_ok());

        let socket_path = result.unwrap();
        assert_eq!(socket_path, temp_dir.join("falkordb.sock"));

        // Cleanup
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_setup_socket_path_creates_temp() {
        let config = EmbeddedConfig {
            socket_path: None,
            ..Default::default()
        };

        let result = EmbeddedServer::setup_socket_path(&config, None);
        assert!(result.is_ok());

        let socket_path = result.unwrap();
        assert!(socket_path.to_string_lossy().contains("falkordb_sock_"));

        // Cleanup
        if let Some(parent) = socket_path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn test_create_config_file() {
        let temp_dir = std::env::temp_dir().join(format!("test_cfg_{}", std::process::id()));
        fs::create_dir_all(&temp_dir).unwrap();

        let socket_path = temp_dir.join("test.sock");
        let db_filename = "test.rdb";

        let result = EmbeddedServer::create_config_file(&temp_dir, &socket_path, db_filename);
        assert!(result.is_ok());

        let config_path = result.unwrap();
        assert!(config_path.exists());
        assert_eq!(config_path, temp_dir.join("falkordb.conf"));

        // Verify content
        let content = fs::read_to_string(&config_path).unwrap();
        assert!(content.contains("port 0"));
        assert!(content.contains(&socket_path.display().to_string()));
        assert!(content.contains(&temp_dir.display().to_string()));
        assert!(content.contains(db_filename));
        assert!(content.contains("unixsocketperm 700"));
        assert!(content.contains("appendonly no"));

        // Cleanup
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_connection_string_format() {
        // We can't test EmbeddedServer::connection_string directly without starting a server,
        // but we can test the format it should produce
        let socket_path = PathBuf::from("/tmp/test.sock");
        let expected = format!("unix://{}", socket_path.display());
        assert_eq!(expected, "unix:///tmp/test.sock");
    }

    #[test]
    fn test_socket_path_length_validation() {
        // Test that overly long socket paths are rejected
        let very_long_path = "/".to_string() + &"a".repeat(MAX_SOCKET_PATH_LENGTH + 10);
        let config = EmbeddedConfig {
            redis_server_path: Some(PathBuf::from("/bin/sh")), // Use a valid executable
            falkordb_module_path: Some(PathBuf::from("/dev/null")), // Won't actually use this
            socket_path: Some(PathBuf::from(very_long_path)),
            ..Default::default()
        };

        let result = EmbeddedServer::start(config);
        assert!(result.is_err());
        if let Err(FalkorDBError::EmbeddedServerError(msg)) = result {
            assert!(msg.contains("Socket path is too long"));
        }
    }

    #[test]
    fn test_unique_temp_directories() {
        // Test that multiple instances with default config get unique temp directories
        let config1 = EmbeddedConfig {
            db_dir: None,
            ..Default::default()
        };
        let config2 = EmbeddedConfig {
            db_dir: None,
            ..Default::default()
        };

        let result1 = EmbeddedServer::setup_db_dir(&config1);
        let result2 = EmbeddedServer::setup_db_dir(&config2);

        assert!(result1.is_ok());
        assert!(result2.is_ok());

        let (dir1, _) = result1.unwrap();
        let (dir2, _) = result2.unwrap();

        // Directories should be different
        assert_ne!(dir1, dir2);

        // Cleanup
        let _ = fs::remove_dir_all(&dir1);
        let _ = fs::remove_dir_all(&dir2);
    }

    #[test]
    fn test_directory_permissions() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let config = EmbeddedConfig {
                db_dir: None,
                ..Default::default()
            };

            let result = EmbeddedServer::setup_db_dir(&config);
            assert!(result.is_ok());

            let (dir, _) = result.unwrap();
            let metadata = fs::metadata(&dir).unwrap();
            let permissions = metadata.permissions();

            // Verify restrictive permissions (0o700)
            assert_eq!(permissions.mode() & 0o777, 0o700);

            // Cleanup
            let _ = fs::remove_dir_all(&dir);
        }
    }

    #[test]
    #[ignore] // Only run when redis-server and FalkorDB module are available
    fn test_embedded_server_start() {
        let config = EmbeddedConfig::default();
        let server = EmbeddedServer::start(config);

        // Should fail if redis-server or falkordb.so are not available
        if server.is_err() {
            println!("Skipping test: redis-server or FalkorDB module not found");
            return;
        }

        let server = server.unwrap();
        assert!(server.socket_path().exists());
    }

    #[test]
    fn test_embedded_server_start_fails_without_redis_server() {
        let config = EmbeddedConfig {
            redis_server_path: Some(PathBuf::from("/nonexistent/redis-server")),
            falkordb_module_path: Some(PathBuf::from("/nonexistent/falkordb.so")),
            ..Default::default()
        };

        let result = EmbeddedServer::start(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_embedded_server_start_fails_without_falkordb_module() {
        // Create a fake redis-server script for testing
        let temp_dir = std::env::temp_dir().join(format!("test_redis_{}", std::process::id()));
        fs::create_dir_all(&temp_dir).unwrap();
        let fake_redis = temp_dir.join("redis-server");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::write(&fake_redis, "#!/bin/sh\necho 'fake redis'\n").unwrap();
            let mut perms = fs::metadata(&fake_redis).unwrap().permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&fake_redis, perms).unwrap();
        }

        #[cfg(not(unix))]
        {
            fs::write(&fake_redis, "@echo off\necho fake redis\n").unwrap();
        }

        let config = EmbeddedConfig {
            redis_server_path: Some(fake_redis),
            falkordb_module_path: Some(PathBuf::from("/nonexistent/falkordb.so")),
            ..Default::default()
        };

        let result = EmbeddedServer::start(config);
        assert!(result.is_err());

        // Cleanup
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_redis_version_too_old() {
        // Below the required 8.0 → rejected.
        assert!(redis_version_too_old(
            "Redis server v=7.4.2 sha=00000000:0 malloc=libc bits=64"
        ));
        assert!(redis_version_too_old("Redis server v=6.2.0"));
        // 8.0 and newer → accepted.
        assert!(!redis_version_too_old("Redis server v=8.0.0 sha=0"));
        assert!(!redis_version_too_old("Redis server v=8.6.3 sha=0"));
        assert!(!redis_version_too_old("Redis server v=12.1.0"));
        // Unparseable / missing version is treated as new-enough.
        assert!(!redis_version_too_old("fake redis"));
        assert!(!redis_version_too_old(""));
        assert!(!redis_version_too_old("Redis server v=notanumber.0"));
    }

    #[test]
    fn test_check_redis_version_allows_unrunnable_binary() {
        // A binary that can't be spawned can't be version-checked, so the
        // pre-flight allows it through (the start attempt then reports any issue).
        assert!(EmbeddedServer::check_redis_version(
            Path::new("/nonexistent/redis-server"),
            Duration::from_secs(5)
        )
        .is_ok());
    }

    /// A fake `redis-server` whose `--version` reports `version`, made executable
    /// at `<dir>/redis-server`. `server_body` runs when invoked as a server.
    #[cfg(unix)]
    fn write_fake_redis(
        dir: &Path,
        version: &str,
        server_body: &str,
    ) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let fake = dir.join("redis-server");
        let script = format!(
            "#!/bin/sh\ncase \"$1\" in --version) echo '{version}'; exit 0;; esac\n{server_body}\n"
        );
        fs::write(&fake, script).unwrap();
        let mut perms = fs::metadata(&fake).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&fake, perms).unwrap();
        fake
    }

    #[test]
    #[cfg(unix)]
    fn test_start_rejects_old_redis_version() {
        let dir = std::env::temp_dir().join(format!("test_redis_old_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let fake_redis = write_fake_redis(&dir, "Redis server v=7.4.2 sha=0", "exit 0");

        let config = EmbeddedConfig {
            redis_server_path: Some(fake_redis),
            // Never reached: the version pre-flight fails first.
            falkordb_module_path: Some(dir.join("falkordb.so")),
            auto_download: false,
            ..Default::default()
        };
        let err = EmbeddedServer::start(config)
            .err()
            .expect("start should fail on an old redis-server");
        assert!(
            err.to_string().contains("8.0 or newer"),
            "unexpected error: {err}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    #[cfg(unix)]
    fn test_start_surfaces_redis_stderr_on_early_exit() {
        // The macOS module load gates on libomp before the spawn; skip if it is
        // absent (the spawn/stderr path is still exercised on Linux CI).
        #[cfg(target_os = "macos")]
        if provision::check_macos_libomp().is_err() {
            return;
        }

        let dir = std::env::temp_dir().join(format!("test_redis_exit_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let fake_redis = write_fake_redis(
            &dir,
            "Redis server v=8.0.0 sha=0",
            "echo 'FATAL: fake module load failure' >&2\nexit 1",
        );
        // A present (but fake) module so resolution returns it without network.
        let fake_module = dir.join("falkordb.so");
        fs::write(&fake_module, b"\0fake module").unwrap();

        let config = EmbeddedConfig {
            redis_server_path: Some(fake_redis),
            falkordb_module_path: Some(fake_module),
            auto_download: false,
            start_timeout: Duration::from_secs(10),
            ..Default::default()
        };
        let err = EmbeddedServer::start(config)
            .err()
            .expect("start should fail when redis-server exits early");
        let msg = err.to_string();
        assert!(
            msg.contains("exited before the socket was ready"),
            "unexpected error: {msg}"
        );
        assert!(
            msg.contains("fake module load failure"),
            "redis stderr not surfaced: {msg}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    #[cfg(unix)]
    fn test_start_detects_redis_dying_after_socket_appears() {
        #[cfg(target_os = "macos")]
        if provision::check_macos_libomp().is_err() {
            return;
        }

        let dir = std::env::temp_dir().join(format!("test_redis_sockdie_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        // Server mode: create the unix socket from the config, then exit — the
        // start loop must not mistake the lingering socket for a live server.
        let fake_redis = write_fake_redis(
            &dir,
            "Redis server v=8.0.0 sha=0",
            "sock=$(awk '/^unixsocket /{print $2}' \"$1\"); [ -n \"$sock\" ] && : > \"$sock\"; exit 0",
        );
        let fake_module = dir.join("falkordb.so");
        fs::write(&fake_module, b"\0fake module").unwrap();

        let config = EmbeddedConfig {
            redis_server_path: Some(fake_redis),
            falkordb_module_path: Some(fake_module),
            auto_download: false,
            start_timeout: Duration::from_secs(10),
            ..Default::default()
        };
        let err = EmbeddedServer::start(config)
            .err()
            .expect("start should fail when redis dies after creating the socket");
        assert!(
            err.to_string()
                .contains("exited immediately after the socket appeared"),
            "unexpected error: {err}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_read_log_tail() {
        let dir = std::env::temp_dir().join(format!("test_logtail_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();

        // Missing or empty log → empty string.
        assert_eq!(EmbeddedServer::read_log_tail(&dir.join("missing.log")), "");

        // A short log is returned whole; a long one is tail-truncated with a marker.
        let log = dir.join("redis-server.log");
        fs::write(&log, "short message").unwrap();
        assert_eq!(EmbeddedServer::read_log_tail(&log), "short message");

        fs::write(&log, "x".repeat(5000)).unwrap();
        let tail = EmbeddedServer::read_log_tail(&log);
        assert!(
            tail.starts_with('…'),
            "expected truncation marker: {tail:.16}"
        );
        assert!(tail.chars().filter(|&c| c == 'x').count() == 4096);

        // Multibyte content whose tail boundary lands mid-char must not panic.
        fs::write(&log, format!("a{}", "→".repeat(2000))).unwrap();
        let tail = EmbeddedServer::read_log_tail(&log);
        assert!(tail.starts_with('…'));
        assert!(tail.len() <= 4096 + '…'.len_utf8());

        // Non-UTF-8 bytes are decoded lossily, not dropped.
        fs::write(&log, b"\xff\xfe redis fatal \xff").unwrap();
        assert!(EmbeddedServer::read_log_tail(&log).contains("redis fatal"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_find_redis_server_in_path() {
        // Test the PATH lookup when redis_server_path is None
        let config = EmbeddedConfig {
            redis_server_path: None,
            ..Default::default()
        };

        // This will either find redis-server in PATH or error appropriately
        let result = EmbeddedServer::find_redis_server(&config);
        // Can't assert ok/err as it depends on system, but should not panic
        let _ = result;
    }

    #[cfg(all(feature = "embedded", not(feature = "embedded-bundle")))]
    #[test]
    fn test_find_falkordb_module_common_paths() {
        // Test the common-location lookup when falkordb_module_path is None.
        // auto_download is disabled so this stays offline (no network download).
        let config = EmbeddedConfig {
            falkordb_module_path: None,
            auto_download: false,
            ..Default::default()
        };

        // This will search common locations and error if not found
        let result = EmbeddedServer::find_falkordb_module(&config);
        // Can't assert ok/err as it depends on system, but should not panic
        let _ = result;
    }

    #[test]
    fn test_socket_path_public_method() {
        // Test that socket_path() returns the correct path
        // We need to create a minimal mock since we can't start a real server
        let socket_path = PathBuf::from("/tmp/test_socket.sock");

        // We can test the connection_string format
        let conn_str = format!("unix://{}", socket_path.display());
        assert!(conn_str.starts_with("unix://"));
        assert!(conn_str.contains("test_socket.sock"));
    }

    #[test]
    fn test_config_file_content_validation() {
        // Test that create_config_file generates correct content
        let temp_dir = std::env::temp_dir().join(format!("test_config_{}", std::process::id()));
        fs::create_dir_all(&temp_dir).unwrap();

        let socket_path = temp_dir.join("test.sock");
        let db_filename = "custom_test.rdb";

        let result = EmbeddedServer::create_config_file(&temp_dir, &socket_path, db_filename);
        assert!(result.is_ok());

        let config_path = result.unwrap();
        let content = fs::read_to_string(&config_path).unwrap();

        // Validate all required config entries
        assert!(content.contains("port 0"), "Config should disable TCP port");
        assert!(
            content.contains("unixsocket"),
            "Config should specify unix socket"
        );
        assert!(
            content.contains("unixsocketperm 700"),
            "Config should set socket permissions"
        );
        assert!(
            content.contains(&temp_dir.display().to_string()),
            "Config should contain db dir"
        );
        assert!(
            content.contains(db_filename),
            "Config should contain db filename"
        );
        assert!(
            content.contains("save \"\""),
            "Config should disable RDB snapshots"
        );
        assert!(
            content.contains("appendonly no"),
            "Config should disable AOF"
        );

        // Cleanup
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_setup_db_dir_error_handling() {
        // Test error handling when directory creation fails
        // On Unix, trying to create a directory under a file will fail
        let temp_file = std::env::temp_dir().join(format!("test_file_{}", std::process::id()));
        fs::write(&temp_file, "test").unwrap();

        let config = EmbeddedConfig {
            db_dir: Some(temp_file.join("subdir")), // This should fail: can't create dir under file
            ..Default::default()
        };

        let result = EmbeddedServer::setup_db_dir(&config);
        assert!(
            result.is_err(),
            "Should fail when trying to create directory under a file"
        );

        if let Err(FalkorDBError::EmbeddedServerError(msg)) = result {
            assert!(
                msg.contains("Failed to create"),
                "Error should mention creation failure"
            );
        }

        // Cleanup
        let _ = fs::remove_file(&temp_file);
    }

    #[test]
    fn test_multiple_config_instances_independent() {
        // Verify that different config instances are independent
        let config1 = EmbeddedConfig {
            db_filename: "db1.rdb".to_string(),
            start_timeout: Duration::from_secs(5),
            ..Default::default()
        };

        let config2 = EmbeddedConfig {
            db_filename: "db2.rdb".to_string(),
            start_timeout: Duration::from_secs(10),
            ..Default::default()
        };

        assert_ne!(config1.db_filename, config2.db_filename);
        assert_ne!(config1.start_timeout, config2.start_timeout);
    }

    #[test]
    fn test_config_debug_impl() {
        // Verify that Debug trait is implemented correctly
        let config = EmbeddedConfig::default();
        let debug_str = format!("{:?}", config);

        // Should contain field names
        assert!(debug_str.contains("EmbeddedConfig"));
        assert!(debug_str.contains("db_filename"));
    }

    #[test]
    fn test_socket_path_setup_with_various_temp_dir_states() {
        // Test socket path setup with temp_dir = None
        let config = EmbeddedConfig {
            socket_path: None,
            ..Default::default()
        };

        let result = EmbeddedServer::setup_socket_path(&config, None);
        assert!(result.is_ok());
        let path = result.unwrap();
        assert!(path.to_string_lossy().contains("falkordb_sock_"));

        // Cleanup
        if let Some(parent) = path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn test_instance_counter_increments() {
        // Verify that the instance counter actually increments
        let before = INSTANCE_COUNTER.load(Ordering::SeqCst);

        let config1 = EmbeddedConfig {
            db_dir: None,
            ..Default::default()
        };
        let _ = EmbeddedServer::setup_db_dir(&config1);

        let config2 = EmbeddedConfig {
            db_dir: None,
            ..Default::default()
        };
        let _ = EmbeddedServer::setup_db_dir(&config2);

        let after = INSTANCE_COUNTER.load(Ordering::SeqCst);
        assert!(after > before, "Instance counter should increment");
    }

    #[test]
    fn test_config_with_all_none_values() {
        // Test config with all optional values set to None
        let config = EmbeddedConfig {
            redis_server_path: None,
            falkordb_module_path: None,
            db_dir: None,
            db_filename: "test.rdb".to_string(),
            socket_path: None,
            start_timeout: Duration::from_secs(1),
            auto_download: true,
            falkordb_version: None,
            cache_dir: None,
        };

        assert!(config.redis_server_path.is_none());
        assert!(config.falkordb_module_path.is_none());
        assert!(config.db_dir.is_none());
        assert!(config.socket_path.is_none());
        assert!(config.auto_download);
        assert!(config.falkordb_version.is_none());
        assert!(config.cache_dir.is_none());
    }

    #[test]
    fn test_find_redis_server_with_valid_path() {
        // Test with a path that exists (use /bin/sh as a placeholder)
        #[cfg(unix)]
        {
            let config = EmbeddedConfig {
                redis_server_path: Some(PathBuf::from("/bin/sh")),
                ..Default::default()
            };

            let result = EmbeddedServer::find_redis_server(&config);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), PathBuf::from("/bin/sh"));
        }
    }

    #[test]
    fn test_find_falkordb_module_with_valid_path() {
        // Test with a path that exists (use /dev/null as a placeholder)
        #[cfg(unix)]
        {
            let config = EmbeddedConfig {
                falkordb_module_path: Some(PathBuf::from("/dev/null")),
                ..Default::default()
            };

            let result = EmbeddedServer::find_falkordb_module(&config);
            // On macOS the libomp prerequisite is checked even for an explicit
            // path, so without libomp the call fails with that actionable error.
            if provision::check_macos_libomp().is_ok() {
                assert_eq!(result.unwrap(), PathBuf::from("/dev/null"));
            } else {
                let msg = format!("{}", result.unwrap_err());
                assert!(msg.contains("libomp"), "got: {msg}");
            }
        }
    }

    #[cfg(all(feature = "embedded", not(feature = "embedded-bundle")))]
    #[test]
    fn test_find_falkordb_module_error_message_without_auto_download() {
        // With auto_download disabled and no module available, resolution must
        // fail with an actionable message. The exact text depends on the host:
        // macOS without libomp fails the OpenMP prerequisite first, otherwise the
        // message points at the missing module / auto_download.
        let config = EmbeddedConfig {
            falkordb_module_path: None,
            auto_download: false,
            redis_server_path: Some(PathBuf::from("/nonexistent/redis-server")),
            ..Default::default()
        };

        let err_msg = format!(
            "{}",
            EmbeddedServer::find_falkordb_module(&config)
                .expect_err("resolution should fail without a module or auto_download")
        );
        assert!(
            err_msg.contains("enable auto_download")
                || err_msg.contains("not found")
                || err_msg.contains("OpenMP")
                || err_msg.contains("libomp"),
            "error should be actionable: {err_msg}"
        );
    }

    #[cfg(all(feature = "embedded", not(feature = "embedded-bundle")))]
    #[test]
    fn test_find_falkordb_module_auto_download_uses_cache() {
        // With auto_download enabled, a previously cached module is reused
        // without any network access. (A live download is covered by the
        // opt-in `download::tests::test_live_download_real_module`.)
        let platform = provision::Platform::detect();
        if platform.tag().is_err() {
            return; // no asset for this platform; nothing to cache
        }
        // Use an overridden (unpinned) version so the placeholder bytes aren't
        // rejected by checksum verification.
        let version = "v0.0.0-modtest";
        let cache_dir =
            std::env::temp_dir().join(format!("fdb_cache_modtest_{}", std::process::id()));
        let cached = download::cached_module_path(&platform, version, Some(&cache_dir)).unwrap();
        fs::create_dir_all(cached.parent().unwrap()).unwrap();
        fs::write(&cached, b"cached module").unwrap();

        let config = EmbeddedConfig {
            falkordb_module_path: None,
            auto_download: true,
            falkordb_version: Some(version.to_string()),
            cache_dir: Some(cache_dir.clone()),
            ..Default::default()
        };
        let result = EmbeddedServer::find_falkordb_module(&config);

        // On macOS the libomp prerequisite is checked before cache resolution.
        if provision::check_macos_libomp().is_ok() {
            assert_eq!(result.unwrap(), cached);
        } else {
            assert!(
                format!("{}", result.unwrap_err()).contains("libomp"),
                "expected libomp error on macOS without libomp"
            );
        }

        let _ = fs::remove_dir_all(&cache_dir);
    }

    #[cfg(all(feature = "embedded", not(feature = "embedded-bundle")))]
    #[test]
    fn test_find_falkordb_module_checks_common_system_locations() {
        // Test that common system locations are checked
        // Using /dev/null as it exists on all Unix systems
        #[cfg(unix)]
        {
            let config = EmbeddedConfig {
                falkordb_module_path: None,
                auto_download: false,
                redis_server_path: Some(PathBuf::from("/nonexistent/redis-server")),
                ..Default::default()
            };

            // This should fail because /dev/null is not a valid falkordb module
            // but it tests that the search through common locations executes
            let result = EmbeddedServer::find_falkordb_module(&config);
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_embedded_config_with_cache_dir() {
        // Test that EmbeddedConfig properly stores cache_dir
        let cache_dir = PathBuf::from("/tmp/test-cache");
        let config = EmbeddedConfig {
            cache_dir: Some(cache_dir.clone()),
            auto_download: true,
            ..Default::default()
        };

        assert_eq!(config.cache_dir, Some(cache_dir));
        assert!(config.auto_download);
    }

    #[test]
    fn test_embedded_config_with_falkordb_version() {
        // Test that EmbeddedConfig properly stores falkordb_version
        let version = "v5.0.0".to_string();
        let config = EmbeddedConfig {
            falkordb_version: Some(version.clone()),
            ..Default::default()
        };

        assert_eq!(config.falkordb_version, Some(version));
    }

    #[test]
    #[cfg(feature = "embedded-bundle")]
    fn test_bundle_resolution_and_accessors() {
        // Accessors expose the build-time bundle metadata.
        assert!(!EmbeddedServer::bundled_module_version().is_empty());
        assert!(!EmbeddedServer::bundled_module_platform().is_empty());

        // A runtime `falkordb_version` has no meaning in bundle mode → rejected.
        let err = EmbeddedServer::find_falkordb_module(&EmbeddedConfig {
            falkordb_version: Some("v9.9.9".to_string()),
            ..Default::default()
        })
        .unwrap_err();
        assert!(
            err.to_string().contains("embedded-bundle"),
            "unexpected error: {err}"
        );

        // The macOS module load gates on libomp before extraction; skip the
        // extraction assertion if it is absent (covered on Linux CI).
        #[cfg(target_os = "macos")]
        if provision::check_macos_libomp().is_err() {
            return;
        }

        // With no explicit path, resolution extracts the embedded bytes to the
        // (temporary) cache — no network involved.
        let cache = std::env::temp_dir().join(format!("test_bundle_{}", std::process::id()));
        let path = EmbeddedServer::find_falkordb_module(&EmbeddedConfig {
            cache_dir: Some(cache.clone()),
            auto_download: false,
            ..Default::default()
        })
        .unwrap();
        assert!(path.exists());
        let _ = fs::remove_dir_all(&cache);
    }
}
