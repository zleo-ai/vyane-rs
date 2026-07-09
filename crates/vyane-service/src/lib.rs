//! # vyane-service
//!
//! The shared service layer that sits between the dispatch kernel and every
//! protocol front-end (CLI, REST API, MCP server). It owns three things that
//! were previously private to the CLI binary:
//!
//! 1. **Config loading** — reading layered TOML files plus a secrets file into
//!    a [`LoadedConfig`] that carries an env-lookup closure (secrets file wins
//!    over real process env).
//! 2. **Selector resolution** — turning a raw selector string (a profile name
//!    or a `provider/model` pair) into a `Vec<BoundTarget>` failover chain.
//!    This is the single chokepoint the dispatch path, the detached worker, and
//!    the workflow engine all share.
//! 3. **High-level operations** — [`VyaneService::dispatch`],
//!    [`VyaneService::broadcast`], [`VyaneService::history`] and
//!    [`VyaneService::sessions`], which compose config + resolution + the kernel
//!    into the operations a front-end actually needs.
//!
//! The CLI is now a thin assembler: it parses arguments, calls into this crate,
//! and formats output. The REST API and MCP server will do the same, sharing
//! identical resolution and dispatch semantics.

mod config;
mod factory;
mod routing;
mod selector;
mod service;
mod task;

pub use config::{LoadedConfig, Runtime, StoragePaths, load_config};
pub use factory::{AssemblerFactory, direct_http_client};
pub use routing::{RouteParams, RouteResult, route_task};
pub use selector::{resolve_target_chain, split_targets};
pub use service::{BroadcastParams, DispatchParams, HistoryFilter, VyaneService};
pub use task::{build_task_spec, parse_labels};
