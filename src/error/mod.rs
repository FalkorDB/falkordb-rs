#[derive(thiserror::Error, Debug)]
pub enum FalkorDBError {
    #[error("The provided connection info is invalid")]
    InvalidConnectionInfo,
    #[error("The connection returned invalid data for this command")]
    InvalidDataReceived,
    #[error("The provided URL scheme points at a database provider that is currently unavailable, make sure the correct feature is enabled")]
    UnavailableProvider,
    #[error("The number of connections for the client has to be between 1 and 32")]
    InvalidConnectionPoolSize,
    #[error("Attempting to use an empty connection object")]
    EmptyConnection,
    #[error("Parsing error due to invalid types, or argument count")]
    ParsingError,
}
