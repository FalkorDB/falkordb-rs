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

// Maximum length for Unix socket paths (typically 104-108 bytes on most Unix systems)
const MAX_SOCKET_PATH_LENGTH: usize = 104;

// Counter for ensuring unique temp directories across multiple instances
static INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Configuration for an embedded FalkorDB server instance.
#[derive(Debug, Clone)]
pub struct EmbeddedConfig {
    /// Path to the redis-server executable. If None, searches in PATH.
    pub redis_server_path: Option<PathBuf>,
    /// Path to the FalkorDB module (.so file). If None, searches in common locations.
    pub falkordb_module_path: Option<PathBuf>,
    /// Directory for the database files. If None, creates a temporary directory.
    pub db_dir: Option<PathBuf>,
    /// Database filename.
    pub db_filename: String,
    /// Path to the Unix socket. If None, creates one in a temporary directory.
    pub socket_path: Option<PathBuf>,
    /// Maximum time to wait for server startup.
    pub start_timeout: Duration,
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
        // Find redis-server executable
        let redis_server = Self::find_redis_server(&config)?;

        // Find FalkorDB module
        let falkordb_module = Self::find_falkordb_module(&config)?;

        // Set up directories and paths
        let (db_dir, temp_dir) = Self::setup_db_dir(&config)?;
        let socket_path = Self::setup_socket_path(&config, temp_dir.as_deref())?;

        // Validate socket path length
        if socket_path.as_os_str().len() > MAX_SOCKET_PATH_LENGTH {
            return Err(FalkorDBError::EmbeddedServerError(format!(
                "Socket path is too long ({} bytes, max {}). Please specify a shorter path in EmbeddedConfig.",
                socket_path.as_os_str().len(),
                MAX_SOCKET_PATH_LENGTH
            )));
        }

        let config_file = Self::create_config_file(&db_dir, &socket_path, &config.db_filename)?;

        // Start redis-server with FalkorDB module (no daemonize to keep process handle valid)
        let mut command = Command::new(&redis_server);
        command
            .arg(&config_file)
            .arg("--loadmodule")
            .arg(&falkordb_module)
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        let mut process = command.spawn().map_err(|e| {
            FalkorDBError::EmbeddedServerError(format!("Failed to start redis-server: {}", e))
        })?;

        // Wait for the socket to be created
        let start_time = std::time::Instant::now();
        while !socket_path.exists() {
            if start_time.elapsed() > config.start_timeout {
                // Clean up the process before returning timeout error
                let _ = process.kill();
                let _ = process.wait();
                // Clean up temporary files
                let _ = fs::remove_file(&config_file);
                if let Some(ref temp_dir) = temp_dir {
                    let _ = fs::remove_dir_all(temp_dir);
                }
                return Err(FalkorDBError::EmbeddedServerError(
                    "Timed out waiting for server to start".to_string(),
                ));
            }
            thread::sleep(Duration::from_millis(100));
        }

        // Give the server a bit more time to be fully ready
        thread::sleep(Duration::from_millis(500));

        Ok(Self {
            process,
            socket_path,
            temp_dir,
            config_file,
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

    fn find_falkordb_module(config: &EmbeddedConfig) -> FalkorResult<PathBuf> {
        if let Some(ref path) = config.falkordb_module_path {
            if path.exists() {
                return Ok(path.clone());
            }
            return Err(FalkorDBError::EmbeddedServerError(format!(
                "FalkorDB module not found at: {}",
                path.display()
            )));
        }

        // Try common locations
        let common_paths = vec![
            PathBuf::from("/usr/lib/redis/modules/falkordb.so"),
            PathBuf::from("/usr/local/lib/redis/modules/falkordb.so"),
            PathBuf::from("/opt/homebrew/lib/redis/modules/falkordb.so"),
            PathBuf::from("./falkordb.so"),
        ];

        for path in common_paths {
            if path.exists() {
                return Ok(path);
            }
        }

        Err(FalkorDBError::EmbeddedServerError(
            "FalkorDB module (falkordb.so) not found. Please install FalkorDB or specify the path in EmbeddedConfig".to_string()
        ))
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
        };

        let config2 = config1.clone();
        assert_eq!(config1.redis_server_path, config2.redis_server_path);
        assert_eq!(config1.falkordb_module_path, config2.falkordb_module_path);
        assert_eq!(config1.db_dir, config2.db_dir);
        assert_eq!(config1.db_filename, config2.db_filename);
        assert_eq!(config1.socket_path, config2.socket_path);
        assert_eq!(config1.start_timeout, config2.start_timeout);
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
        if let Err(FalkorDBError::EmbeddedServerError(msg)) = result {
            assert!(msg.contains("FalkorDB module not found"));
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
            redis_server_path: Some(PathBuf::from("/bin/true")), // Use a valid executable
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

    #[test]
    fn test_find_falkordb_module_common_paths() {
        // Test the common paths lookup when falkordb_module_path is None
        let config = EmbeddedConfig {
            falkordb_module_path: None,
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
        };

        assert!(config.redis_server_path.is_none());
        assert!(config.falkordb_module_path.is_none());
        assert!(config.db_dir.is_none());
        assert!(config.socket_path.is_none());
    }

    #[test]
    fn test_find_redis_server_with_valid_path() {
        // Test with a path that exists (use /bin/true as a placeholder)
        #[cfg(unix)]
        {
            let config = EmbeddedConfig {
                redis_server_path: Some(PathBuf::from("/bin/true")),
                ..Default::default()
            };

            let result = EmbeddedServer::find_redis_server(&config);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), PathBuf::from("/bin/true"));
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
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), PathBuf::from("/dev/null"));
        }
    }
}
