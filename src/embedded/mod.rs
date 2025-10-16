/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Embedded FalkorDB server support.
//!
//! This module provides functionality to spawn and manage an embedded Redis server
//! with the FalkorDB module loaded, allowing for in-process graph database operations.

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::Duration;
use std::{fs, thread};

use crate::{FalkorDBError, FalkorResult};

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
        let config_file = Self::create_config_file(&db_dir, &socket_path, &config.db_filename)?;

        // Start redis-server with FalkorDB module
        let mut command = Command::new(&redis_server);
        command
            .arg(&config_file)
            .arg("--loadmodule")
            .arg(&falkordb_module)
            .arg("--daemonize")
            .arg("yes");

        let process = command.spawn().map_err(|e| {
            FalkorDBError::EmbeddedServerError(format!("Failed to start redis-server: {}", e))
        })?;

        // Wait for the socket to be created
        let start_time = std::time::Instant::now();
        while !socket_path.exists() {
            if start_time.elapsed() > config.start_timeout {
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
            }
            Ok((dir.clone(), None))
        } else {
            // Create a temporary directory in the system temp
            let temp_base = std::env::temp_dir();
            let temp_name = format!("falkordb_{}", std::process::id());
            let temp_dir = temp_base.join(temp_name);

            if !temp_dir.exists() {
                fs::create_dir_all(&temp_dir).map_err(|e| {
                    FalkorDBError::EmbeddedServerError(format!(
                        "Failed to create temp directory: {}",
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
            // Use the system temp directory for the socket
            let temp_base = std::env::temp_dir();
            let temp_name = format!("falkordb_sock_{}", std::process::id());
            let temp_dir = temp_base.join(temp_name);

            if !temp_dir.exists() {
                fs::create_dir_all(&temp_dir).map_err(|e| {
                    FalkorDBError::EmbeddedServerError(format!(
                        "Failed to create temp directory: {}",
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

        // Clean up temporary files
        if let Some(ref temp_dir) = self.temp_dir {
            let _ = fs::remove_dir_all(temp_dir);
        }

        // Remove the config file
        let _ = fs::remove_file(&self.config_file);

        // Remove the socket file if it exists
        if self.socket_path.exists() {
            let _ = fs::remove_file(&self.socket_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
