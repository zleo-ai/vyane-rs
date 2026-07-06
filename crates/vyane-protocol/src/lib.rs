//! # vyane-protocol
//!
//! Wire-protocol chat clients implementing [`vyane_core::ChatClient`]: OpenAI
//! Chat Completions, Anthropic Messages, and OpenAI Responses.

pub mod anthropic_messages;
pub mod openai_chat;
pub mod openai_responses;
pub mod retry;

mod http;
mod sse;
mod wire;

pub use anthropic_messages::AnthropicMessagesClient;
pub use http::ClientOptions;
pub use openai_chat::OpenAiChatClient;
pub use openai_responses::OpenAiResponsesClient;
pub use retry::RetryConfig;
