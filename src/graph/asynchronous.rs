use crate::client::asynchronous::AsyncFalkorClient;

pub struct AsyncGraph<'a> {
    pub(crate) client: &'a AsyncFalkorClient,
}
