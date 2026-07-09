//! Re-export of the service-layer executor factory.
//!
//! The `AssemblerFactory` and `direct_http_client` mapping now live in
//! `vyane-service` so the REST API and MCP server share them. The CLI's
//! streaming path still calls `direct_http_client` directly (see WP-09's
//! "known seam" note), so the symbol stays reachable via this re-export.

pub use vyane_service::direct_http_client;
