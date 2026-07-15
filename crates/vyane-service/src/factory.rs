//! The executor factory: turns a resolved [`BoundTarget`] into a concrete
//! [`Executor`] (HTTP chat client or CLI harness).
//!
//! Lifted verbatim from the old `vyane-cli/src/factory.rs`. The service layer
//! owns it because every front-end (CLI, REST, MCP) must wire the same
//! `Protocol -> client` and `HarnessKind -> harness` mappings.

use std::sync::Arc;

use async_trait::async_trait;
use vyane_config::ResolvedConfig;
use vyane_core::{
    AdapterTransport, AuthorizedToolChatClient, BoundTarget, ChatClient, EnvPolicy, ErrorKind,
    Harness, HarnessExecutionContext, HarnessJob, HarnessKind, HarnessOutcome, HarnessStreamEvent,
    Protocol, Result, VyaneError,
};
use vyane_harness::{ClaudeCodeHarness, CodexCliHarness};
use vyane_kernel::{CapabilityManifest, Executor, ExecutorFactory, IsolationStrength};
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

/// Closed rejection taxonomy for the assembler's pure support matrix.
///
/// This contains no target/config strings so callers can safely map it to a
/// static diagnostic without formatting provider-controlled data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AssemblerSupportError {
    UnsupportedTransport,
    HarnessRequired,
    HarnessNotAllowed,
    UnsupportedHarness,
    UnsupportedProtocol,
}

impl AssemblerSupportError {
    const fn message(self) -> &'static str {
        match self {
            Self::UnsupportedTransport => "target transport is not supported by this assembler",
            Self::HarnessRequired => "CLI transport requires a supported harness",
            Self::HarnessNotAllowed => "direct HTTP transport cannot use a harness",
            Self::UnsupportedHarness => "CLI harness is not supported by this assembler",
            Self::UnsupportedProtocol => {
                "transport, protocol, and harness combination is not supported"
            }
        }
    }
}

/// Pure support matrix shared by construction, capability admission, and
/// static diagnostics. It performs no endpoint access, probing, I/O, or spawn.
pub(crate) fn validate_assembler_combo(
    transport: AdapterTransport,
    protocol: Protocol,
    harness: Option<&HarnessKind>,
) -> std::result::Result<(), AssemblerSupportError> {
    match (transport, harness, protocol) {
        (
            AdapterTransport::DirectHttp,
            None,
            Protocol::OpenaiChat | Protocol::OpenaiResponses | Protocol::AnthropicMessages,
        ) => Ok(()),
        (AdapterTransport::DirectHttp, Some(_), _) => Err(AssemblerSupportError::HarnessNotAllowed),
        (AdapterTransport::CliWrap, None, _) => Err(AssemblerSupportError::HarnessRequired),
        (AdapterTransport::CliWrap, Some(HarnessKind::ClaudeCode), Protocol::AnthropicMessages) => {
            Ok(())
        }
        (
            AdapterTransport::CliWrap,
            Some(HarnessKind::CodexCli),
            Protocol::OpenaiChat | Protocol::OpenaiResponses,
        ) => Ok(()),
        (AdapterTransport::CliWrap, Some(HarnessKind::OpenCode | HarnessKind::Other(_)), _) => {
            Err(AssemblerSupportError::UnsupportedHarness)
        }
        (AdapterTransport::CliWrap, Some(_), _) => Err(AssemblerSupportError::UnsupportedProtocol),
        _ => Err(AssemblerSupportError::UnsupportedTransport),
    }
}

fn assembler_support_error(error: AssemblerSupportError) -> VyaneError {
    VyaneError::new(ErrorKind::Unsupported, error.message())
}

impl ExecutorFactory for AssemblerFactory {
    fn capability_manifest(&self, bound: &BoundTarget) -> CapabilityManifest {
        match (
            validate_assembler_combo(
                bound.transport,
                bound.target.protocol,
                bound.target.harness.as_ref(),
            ),
            bound.transport,
            bound.target.harness.as_ref(),
        ) {
            (
                Ok(()),
                AdapterTransport::CliWrap,
                Some(HarnessKind::ClaudeCode | HarnessKind::CodexCli),
            ) => CapabilityManifest::local_workdir_editing(IsolationStrength::AdapterDelegated),
            // Direct HTTP, remote adapters, and unknown/custom harnesses stay
            // chat-only unless a future trusted implementation explicitly
            // declares and enforces stronger behavior.
            _ => CapabilityManifest::chat_only(),
        }
    }

    fn make(&self, bound: &BoundTarget) -> Result<Executor> {
        validate_assembler_combo(
            bound.transport,
            bound.target.protocol,
            bound.target.harness.as_ref(),
        )
        .map_err(assembler_support_error)?;
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
/// Shared with the CLI's `--stream` path, which needs the same
/// `Protocol -> concrete client` mapping outside the `ExecutorFactory` seam:
/// streaming drives the client directly rather than through
/// `Dispatcher::dispatch` (see `docs/plan/WP-09.md`'s "known seam" note).
pub fn direct_http_client(bound: &BoundTarget) -> Result<Arc<dyn ChatClient>> {
    validate_assembler_combo(
        bound.transport,
        bound.target.protocol,
        bound.target.harness.as_ref(),
    )
    .map_err(assembler_support_error)?;
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

/// Build the guarded tool-chat capability used by an in-process native agent.
///
/// This is intentionally narrower than [`direct_http_client`]: the native loop
/// may only receive the authority-checking trait object, and the initial
/// production support matrix is limited to direct OpenAI Chat endpoints.  The
/// complete matrix is checked before cloning endpoint material or constructing
/// an HTTP client, so unsupported targets cannot cause wire effects.
pub fn authorized_native_client(bound: &BoundTarget) -> Result<Arc<dyn AuthorizedToolChatClient>> {
    validate_authorized_native_combo(
        bound.transport,
        bound.target.protocol,
        bound.target.harness.as_ref(),
    )?;
    let endpoint = bound.endpoint.clone().ok_or_else(|| {
        VyaneError::new(
            ErrorKind::Config,
            "native model target requires a concrete endpoint",
        )
    })?;
    let options = ClientOptions {
        // The protocol client owns a bounded three-attempt retry loop and
        // revalidates native authority before every physical send.
        retry: RetryConfig::default(),
        request_timeout: None,
    };
    Ok(Arc::new(OpenAiChatClient::with_options(endpoint, options)?))
}

fn validate_authorized_native_combo(
    transport: AdapterTransport,
    protocol: Protocol,
    harness: Option<&HarnessKind>,
) -> Result<()> {
    match (transport, harness, protocol) {
        (AdapterTransport::DirectHttp, None, Protocol::OpenaiChat) => Ok(()),
        (AdapterTransport::DirectHttp, Some(_), _) => Err(VyaneError::new(
            ErrorKind::Unsupported,
            "native model target cannot use a CLI harness",
        )),
        (AdapterTransport::DirectHttp, None, _) => Err(VyaneError::new(
            ErrorKind::Unsupported,
            "native model target protocol is not supported",
        )),
        _ => Err(VyaneError::new(
            ErrorKind::Unsupported,
            "native model target transport is not supported",
        )),
    }
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

    async fn run_scoped(
        &self,
        mut job: HarnessJob,
        context: HarnessExecutionContext,
        cancel: vyane_core::CancellationToken,
    ) -> Result<HarnessOutcome> {
        job.env = self.env.clone();
        self.inner.run_scoped(job, context, cancel).await
    }

    async fn run_stream(
        &self,
        mut job: HarnessJob,
        cancel: vyane_core::CancellationToken,
        on_event: Box<dyn FnMut(HarnessStreamEvent) + Send + Sync>,
    ) -> Result<HarnessOutcome> {
        job.env = self.env.clone();
        self.inner.run_stream(job, cancel, on_event).await
    }

    async fn run_stream_scoped(
        &self,
        mut job: HarnessJob,
        context: HarnessExecutionContext,
        cancel: vyane_core::CancellationToken,
        on_event: Box<dyn FnMut(HarnessStreamEvent) + Send + Sync>,
    ) -> Result<HarnessOutcome> {
        job.env = self.env.clone();
        self.inner
            .run_stream_scoped(job, context, cancel, on_event)
            .await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::io::ErrorKind as IoErrorKind;
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;
    use vyane_core::{
        AuthMaterial, AuthStyle, CancellationToken, Endpoint, GenParams, ModelId, ProviderId,
        Sandbox, Secret, Target,
    };

    struct StreamingHarness {
        observed_env: Arc<Mutex<Option<EnvPolicy>>>,
    }

    #[async_trait]
    impl Harness for StreamingHarness {
        fn kind(&self) -> HarnessKind {
            HarnessKind::ClaudeCode
        }

        async fn available(&self) -> bool {
            true
        }

        async fn run(
            &self,
            _job: HarnessJob,
            _cancel: CancellationToken,
        ) -> Result<HarnessOutcome> {
            Err(VyaneError::unsupported(
                "non-streaming path was not expected",
            ))
        }

        async fn run_stream(
            &self,
            job: HarnessJob,
            _cancel: CancellationToken,
            mut on_event: Box<dyn FnMut(HarnessStreamEvent) + Send + Sync>,
        ) -> Result<HarnessOutcome> {
            *self.observed_env.lock().unwrap() = Some(job.env);
            on_event(HarnessStreamEvent::Delta("live".into()));
            Ok(HarnessOutcome {
                text: "final".into(),
                native_session_id: Some("session-1".into()),
                usage: None,
                exit_code: 0,
                duration: Duration::ZERO,
            })
        }
    }

    fn job_with_caller_env() -> HarnessJob {
        HarnessJob {
            prompt: "test".into(),
            model: ModelId::new("model"),
            protocol: Protocol::AnthropicMessages,
            endpoint: None,
            params: GenParams::default(),
            workdir: None,
            sandbox: Sandbox::ReadOnly,
            resume: None,
            env: EnvPolicy::scrubbed().inject("CALLER_ONLY", "must-be-replaced"),
            timeout: None,
            harness_lifecycle_reporter: None,
        }
    }

    fn bound_target(
        transport: AdapterTransport,
        harness: Option<HarnessKind>,
        protocol: Protocol,
    ) -> BoundTarget {
        BoundTarget {
            target: Target {
                provider: ProviderId::new("test"),
                protocol,
                harness,
                model: ModelId::new("model"),
            },
            transport,
            endpoint: None,
            params: GenParams::default(),
        }
    }

    fn endpoint_bound_target(
        transport: AdapterTransport,
        harness: Option<HarnessKind>,
        protocol: Protocol,
        base_url: impl Into<String>,
    ) -> BoundTarget {
        let mut bound = bound_target(transport, harness, protocol);
        bound.endpoint = Some(Endpoint {
            base_url: base_url.into(),
            auth: Some(AuthMaterial {
                style: AuthStyle::Bearer,
                secret: Secret::new("TEST_FACTORY_SECRET_CANARY"),
            }),
        });
        bound
    }

    #[test]
    fn assembler_declares_editing_only_for_builtin_local_harnesses() {
        let factory = AssemblerFactory::new(ResolvedConfig::default());
        for (harness, protocol) in [
            (HarnessKind::ClaudeCode, Protocol::AnthropicMessages),
            (HarnessKind::CodexCli, Protocol::OpenaiResponses),
        ] {
            assert_eq!(
                factory.capability_manifest(&bound_target(
                    AdapterTransport::CliWrap,
                    Some(harness),
                    protocol,
                )),
                CapabilityManifest::local_workdir_editing(IsolationStrength::AdapterDelegated,)
            );
        }
        assert_eq!(
            factory.capability_manifest(&bound_target(
                AdapterTransport::CliWrap,
                Some(HarnessKind::OpenCode),
                Protocol::OpenaiChat,
            )),
            CapabilityManifest::chat_only()
        );
        assert_eq!(
            factory.capability_manifest(&bound_target(
                AdapterTransport::DirectHttp,
                None,
                Protocol::OpenaiChat,
            )),
            CapabilityManifest::chat_only()
        );
    }

    #[test]
    fn pure_support_matrix_matches_concrete_assembler_contract() {
        for (transport, harness, protocol) in [
            (AdapterTransport::DirectHttp, None, Protocol::OpenaiChat),
            (
                AdapterTransport::DirectHttp,
                None,
                Protocol::OpenaiResponses,
            ),
            (
                AdapterTransport::DirectHttp,
                None,
                Protocol::AnthropicMessages,
            ),
            (
                AdapterTransport::CliWrap,
                Some(HarnessKind::ClaudeCode),
                Protocol::AnthropicMessages,
            ),
            (
                AdapterTransport::CliWrap,
                Some(HarnessKind::CodexCli),
                Protocol::OpenaiChat,
            ),
            (
                AdapterTransport::CliWrap,
                Some(HarnessKind::CodexCli),
                Protocol::OpenaiResponses,
            ),
        ] {
            assert_eq!(
                validate_assembler_combo(transport, protocol, harness.as_ref()),
                Ok(())
            );
        }

        for (harness, protocol) in [
            (HarnessKind::OpenCode, Protocol::OpenaiChat),
            (HarnessKind::Other("custom".into()), Protocol::OpenaiChat),
            (HarnessKind::CodexCli, Protocol::AnthropicMessages),
            (HarnessKind::ClaudeCode, Protocol::OpenaiResponses),
        ] {
            assert!(
                validate_assembler_combo(AdapterTransport::CliWrap, protocol, Some(&harness),)
                    .is_err()
            );
        }
    }

    #[test]
    fn make_rejects_known_unsupported_combo_before_endpoint_or_harness_work() {
        let factory = AssemblerFactory::new(ResolvedConfig::default());
        let error = factory
            .make(&bound_target(
                AdapterTransport::CliWrap,
                Some(HarnessKind::CodexCli),
                Protocol::AnthropicMessages,
            ))
            .err()
            .unwrap();

        assert_eq!(error.kind, ErrorKind::Unsupported);
        assert_eq!(
            error.message,
            "transport, protocol, and harness combination is not supported"
        );
    }

    #[test]
    fn authorized_native_factory_accepts_only_direct_openai_chat_with_endpoint() {
        let accepted = endpoint_bound_target(
            AdapterTransport::DirectHttp,
            None,
            Protocol::OpenaiChat,
            "http://127.0.0.1:1",
        );
        assert_eq!(
            authorized_native_client(&accepted).unwrap().protocol(),
            Protocol::OpenaiChat
        );

        let rejected = [
            bound_target(
                AdapterTransport::DirectHttp,
                None,
                Protocol::OpenaiResponses,
            ),
            bound_target(
                AdapterTransport::DirectHttp,
                None,
                Protocol::AnthropicMessages,
            ),
            bound_target(
                AdapterTransport::CliWrap,
                Some(HarnessKind::CodexCli),
                Protocol::OpenaiChat,
            ),
            bound_target(
                AdapterTransport::DirectHttp,
                Some(HarnessKind::Other("custom-harness".into())),
                Protocol::OpenaiChat,
            ),
        ];
        for bound in rejected {
            assert_eq!(
                authorized_native_client(&bound).err().unwrap().kind,
                ErrorKind::Unsupported
            );
        }

        let missing_endpoint =
            bound_target(AdapterTransport::DirectHttp, None, Protocol::OpenaiChat);
        assert_eq!(
            authorized_native_client(&missing_endpoint)
                .err()
                .unwrap()
                .kind,
            ErrorKind::Config
        );
    }

    #[test]
    fn rejected_native_combinations_have_no_wire_effect() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());

        for protocol in [Protocol::OpenaiResponses, Protocol::AnthropicMessages] {
            let bound = endpoint_bound_target(
                AdapterTransport::DirectHttp,
                None,
                protocol,
                endpoint.clone(),
            );
            assert!(authorized_native_client(&bound).is_err());
        }
        let cli = endpoint_bound_target(
            AdapterTransport::CliWrap,
            Some(HarnessKind::CodexCli),
            Protocol::OpenaiChat,
            endpoint,
        );
        assert!(authorized_native_client(&cli).is_err());

        assert_eq!(
            listener.accept().unwrap_err().kind(),
            IoErrorKind::WouldBlock
        );
    }

    #[test]
    fn authorized_native_factory_diagnostics_are_static_and_redacted() {
        let mut bound = endpoint_bound_target(
            AdapterTransport::DirectHttp,
            Some(HarnessKind::Other("TEST_FACTORY_HARNESS_CANARY".into())),
            Protocol::OpenaiChat,
            "https://TEST_FACTORY_ENDPOINT_CANARY.invalid",
        );
        bound.target.provider = ProviderId::new("TEST_FACTORY_PROVIDER_CANARY");
        bound.target.model = ModelId::new("TEST_FACTORY_MODEL_CANARY");

        let error = authorized_native_client(&bound).err().unwrap();
        let diagnostic = format!("{error:?} {error}");
        for canary in [
            "TEST_FACTORY_SECRET_CANARY",
            "TEST_FACTORY_HARNESS_CANARY",
            "TEST_FACTORY_ENDPOINT_CANARY",
            "TEST_FACTORY_PROVIDER_CANARY",
            "TEST_FACTORY_MODEL_CANARY",
        ] {
            assert!(!diagnostic.contains(canary));
        }
        assert_eq!(
            error.message,
            "native model target cannot use a CLI harness"
        );
    }

    #[tokio::test]
    async fn env_policy_wrapper_forwards_streaming_and_replaces_job_env() {
        let observed_env = Arc::new(Mutex::new(None));
        let target_env = EnvPolicy::scrubbed().inject("TARGET_ONLY", "kept");
        let wrapper = EnvPolicyHarness::new(
            Arc::new(StreamingHarness {
                observed_env: Arc::clone(&observed_env),
            }),
            target_env,
        );
        let events = Arc::new(Mutex::new(Vec::new()));

        let outcome = wrapper
            .run_stream(job_with_caller_env(), CancellationToken::new(), {
                let events = Arc::clone(&events);
                Box::new(move |event| {
                    if let HarnessStreamEvent::Delta(text) = event {
                        events.lock().unwrap().push(text);
                    }
                })
            })
            .await
            .unwrap();

        assert_eq!(outcome.text, "final");
        assert_eq!(*events.lock().unwrap(), vec!["live"]);
        let env = observed_env.lock().unwrap().take().unwrap();
        assert_eq!(
            env.inject.get("TARGET_ONLY").map(String::as_str),
            Some("kept")
        );
        assert!(!env.inject.contains_key("CALLER_ONLY"));
    }
}
