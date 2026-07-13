//! Bounded delivery orchestration over [`vyane_message`] transactional truth.
//!
//! This crate never keeps a second pending or in-flight queue. A pump claims a
//! bounded batch from `vyane-message`, calls one replay-safe adapter, then
//! settles through the fenced lease receipt. Its EventLog integrations are
//! at-least-once, body-free projections of the transactional message and
//! AgentRun outboxes.

mod adapter;
mod agent_projector;
mod broker;
mod error;
mod projector;
mod supervisor;

pub use adapter::{
    AdapterContext, AdapterFailure, AdapterOutcome, DeliveryAdapter, DeliveryEnvelope, ReplaySafety,
};
pub use agent_projector::{
    AgentEventProjector, DEFAULT_AGENT_EVENT_PROJECTOR, DEFAULT_AGENT_EVENT_STREAM,
};
pub use broker::{
    BrokerScope, MaintenanceReport, MessageBroker, PumpItemResult, PumpItemStatus, PumpOptions,
    PumpReport,
};
pub use error::{BrokerError, Result};
pub use projector::{
    DEFAULT_MESSAGE_EVENT_PROJECTOR, DEFAULT_MESSAGE_EVENT_STREAM, MessageEventProjector,
    ProjectionReport,
};
pub use supervisor::{
    DeliveryLane, DeliveryLoopExit, LoopExit, ResidentBrokerSupervisor, SupervisorExit,
    SupervisorOptions,
};
