/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::client::asynchronous::AsyncFalkorClient;

pub struct AsyncGraph<'a> {
    pub(crate) client: &'a AsyncFalkorClient,
}
