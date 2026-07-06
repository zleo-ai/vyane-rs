use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use vyane_config::ResolvedConfig;
use vyane_core::{
    AdapterTransport, BoundTarget, ChatClient, ChatOutcome, ChatRequest, ErrorKind, Harness,
    HarnessJob, HarnessKind, HarnessOutcome, Protocol, Result, StreamEvent, Target, VyaneError,
};
use vyane_kernel::{Executor, ExecutorFactory};
use vyane_protocol::{
    AnthropicMessagesClient, ClientOptions, OpenAiChatClient, OpenAiResponsesClient, RetryConfig,
};

use crate::harness::{CliHarness, EnvInjectedHarness};

#[derive(Clone)]
pub struct AssemblerFactory {
    config: ResolvedConfig,
    capture: OutputCapture,
}

impl AssemblerFactory {
    pub fn new(config: ResolvedConfig, capture: OutputCapture) -> Self {
        Self { config, capture }
    }
}

impl ExecutorFactory for AssemblerFactory {
    fn make(&self, bound: &BoundTarget) -> Result<Executor> {
        match bound.transport {
            AdapterTransport::DirectHttp => {
                let client = direct_http_client(bound)?;
                Ok(Executor::Chat(self.capture_client(bound, client)))
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
                let inner = CliHarness::for_kind(harness_kind)?;
                let harness = EnvInjectedHarness::new(inner, env);
                Ok(Executor::Agent(Arc::new(CaptureHarness {
                    inner: Arc::new(harness),
                    target_key: target_key(&bound.target),
                    capture: self.capture.clone(),
                })))
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

fn direct_http_client(bound: &BoundTarget) -> Result<Arc<dyn ChatClient>> {
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

impl AssemblerFactory {
    fn capture_client(
        &self,
        bound: &BoundTarget,
        client: Arc<dyn ChatClient>,
    ) -> Arc<dyn ChatClient> {
        Arc::new(CaptureChatClient {
            inner: client,
            target_key: target_key(&bound.target),
            capture: self.capture.clone(),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct OutputCapture {
    inner: Arc<Mutex<BTreeMap<String, VecDeque<String>>>>,
}

impl OutputCapture {
    pub fn push(&self, target_key: &str, text: String) {
        if let Ok(mut guard) = self.inner.lock() {
            guard
                .entry(target_key.to_string())
                .or_default()
                .push_back(text);
        }
    }

    pub fn pop_for_target(&self, target: &Target) -> Option<String> {
        let key = target_key(target);
        self.inner
            .lock()
            .ok()
            .and_then(|mut guard| guard.get_mut(&key).and_then(VecDeque::pop_front))
    }
}

struct CaptureChatClient {
    inner: Arc<dyn ChatClient>,
    target_key: String,
    capture: OutputCapture,
}

#[async_trait]
impl ChatClient for CaptureChatClient {
    fn protocol(&self) -> Protocol {
        self.inner.protocol()
    }

    async fn complete(&self, req: ChatRequest) -> Result<ChatOutcome> {
        let outcome = self.inner.complete(req).await?;
        self.capture.push(&self.target_key, outcome.text.clone());
        Ok(outcome)
    }

    async fn stream(
        &self,
        req: ChatRequest,
    ) -> Result<futures::stream::BoxStream<'static, Result<StreamEvent>>> {
        self.inner.stream(req).await
    }
}

struct CaptureHarness {
    inner: Arc<dyn Harness>,
    target_key: String,
    capture: OutputCapture,
}

#[async_trait]
impl Harness for CaptureHarness {
    fn kind(&self) -> HarnessKind {
        self.inner.kind()
    }

    async fn available(&self) -> bool {
        self.inner.available().await
    }

    async fn run(
        &self,
        job: HarnessJob,
        cancel: vyane_core::CancellationToken,
    ) -> Result<HarnessOutcome> {
        let outcome = self.inner.run(job, cancel).await?;
        self.capture.push(&self.target_key, outcome.text.clone());
        Ok(outcome)
    }
}

fn target_key(target: &Target) -> String {
    format!(
        "{}/{}/{}/{}",
        target.provider,
        target.model,
        target.protocol,
        target
            .harness
            .as_ref()
            .map(HarnessKind::as_str)
            .unwrap_or("none")
    )
}
