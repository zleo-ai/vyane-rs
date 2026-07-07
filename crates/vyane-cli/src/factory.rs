use std::sync::Arc;

use async_trait::async_trait;
use vyane_config::ResolvedConfig;
use vyane_core::{
    AdapterTransport, BoundTarget, ChatClient, EnvPolicy, ErrorKind, Harness, HarnessJob,
    HarnessKind, HarnessOutcome, Protocol, Result, VyaneError,
};
use vyane_harness::{ClaudeCodeHarness, CodexCliHarness};
use vyane_kernel::{Executor, ExecutorFactory};
use vyane_protocol::{
    AnthropicMessagesClient, ClientOptions, OpenAiChatClient, OpenAiResponsesClient, RetryConfig,
};

#[derive(Clone)]
pub struct AssemblerFactory {
    config: ResolvedConfig,
}

impl AssemblerFactory {
    pub fn new(config: ResolvedConfig) -> Self {
        Self { config }
    }
}

impl ExecutorFactory for AssemblerFactory {
    fn make(&self, bound: &BoundTarget) -> Result<Executor> {
        match bound.transport {
            AdapterTransport::DirectHttp => {
                let client = direct_http_client(bound)?;
                Ok(Executor::Chat(client))
            }
            AdapterTransport::CliWrap => {
                let harness_kind = bound.target.harness.clone().ok_or_else(|| {
                    VyaneError::new(
                        ErrorKind::Unsupported,
                        format!(
                            "transport/protocol/harness combo unsupported: {:?} / {} / none",
                            bound.transport, bound.target.protocol
                        ),
                    )
                })?;
                let env = self.config.env_policy_for(bound)?.ok_or_else(|| {
                    VyaneError::new(
                        ErrorKind::Unsupported,
                        format!(
                            "transport/protocol/harness combo unsupported: {:?} / {} / {}",
                            bound.transport, bound.target.protocol, harness_kind
                        ),
                    )
                })?;
                let harness = concrete_harness(harness_kind)?;
                Ok(Executor::Agent(Arc::new(EnvPolicyHarness::new(
                    harness, env,
                ))))
            }
            _ => Err(VyaneError::new(
                ErrorKind::Unsupported,
                format!(
                    "transport/protocol/harness combo unsupported: {:?} / {} / {}",
                    bound.transport,
                    bound.target.protocol,
                    bound
                        .target
                        .harness
                        .as_ref()
                        .map(HarnessKind::as_str)
                        .unwrap_or("none")
                ),
            )),
        }
    }
}

/// Build the direct-HTTP `ChatClient` for a `DirectHttp` `BoundTarget`.
///
/// Shared with the CLI's `--stream` path (`command.rs`), which needs the same
/// `Protocol -> concrete client` mapping outside the `ExecutorFactory` seam:
/// streaming drives the client directly rather than through
/// `Dispatcher::dispatch` (see `docs/plan/WP-09.md`'s "known seam" note).
pub(crate) fn direct_http_client(bound: &BoundTarget) -> Result<Arc<dyn ChatClient>> {
    let endpoint = bound.endpoint.clone().ok_or_else(|| {
        VyaneError::new(
            ErrorKind::Config,
            format!("direct HTTP target {} has no endpoint", bound.target),
        )
    })?;
    let options = ClientOptions {
        retry: RetryConfig::default().without_sleep(),
        request_timeout: None,
    };
    let client: Arc<dyn ChatClient> = match bound.target.protocol {
        Protocol::OpenaiChat => Arc::new(OpenAiChatClient::with_options(endpoint, options)?),
        Protocol::OpenaiResponses => {
            Arc::new(OpenAiResponsesClient::with_options(endpoint, options)?)
        }
        Protocol::AnthropicMessages => {
            Arc::new(AnthropicMessagesClient::with_options(endpoint, options)?)
        }
        _ => {
            return Err(VyaneError::new(
                ErrorKind::Unsupported,
                format!("unsupported direct HTTP protocol {}", bound.target.protocol),
            ));
        }
    };
    Ok(client)
}

fn concrete_harness(kind: HarnessKind) -> Result<Arc<dyn Harness>> {
    match kind {
        HarnessKind::ClaudeCode => Ok(Arc::new(ClaudeCodeHarness::new())),
        HarnessKind::CodexCli => Ok(Arc::new(CodexCliHarness::new())),
        HarnessKind::OpenCode | HarnessKind::Other(_) => Err(VyaneError::new(
            ErrorKind::Unsupported,
            format!("unsupported CLI harness `{kind}`"),
        )),
    }
}

struct EnvPolicyHarness {
    inner: Arc<dyn Harness>,
    env: EnvPolicy,
}

impl EnvPolicyHarness {
    fn new(inner: Arc<dyn Harness>, env: EnvPolicy) -> Self {
        Self { inner, env }
    }
}

#[async_trait]
impl Harness for EnvPolicyHarness {
    async fn available(&self) -> bool {
        self.inner.available().await
    }

    fn kind(&self) -> HarnessKind {
        self.inner.kind()
    }

    async fn run(
        &self,
        mut job: HarnessJob,
        cancel: vyane_core::CancellationToken,
    ) -> Result<HarnessOutcome> {
        job.env = self.env.clone();
        self.inner.run(job, cancel).await
    }
}
