//! Runtime authorization for native-loop side effects.
//!
//! The authority is intentionally a live trait object. It is not a durable
//! permit and must be revalidated immediately before every externally visible
//! side effect. Implementations decide how a revoked, expired, or stale
//! execution is rejected.

use async_trait::async_trait;

use crate::Result;

/// One side effect about to be issued by a native tool loop.
///
/// This value carries only ordering coordinates. Prompt text, tool arguments,
/// workspace paths, credentials, and other request contents must stay in the
/// component performing the operation and must never enter authority logs.
/// The type deliberately has no Serde implementation and is not a durable
/// authorization token.
///
/// ```compile_fail
/// use vyane_core::NativeSideEffect;
///
/// let effect = NativeSideEffect::SessionCommit {
///     expected_revision: 3,
/// };
/// let _ = serde_json::to_string(&effect);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NativeSideEffect {
    /// One physical request attempt to the model endpoint.
    ModelSend { turn: u32, wire_attempt: u32 },
    /// One local or remote tool operation selected by the model.
    ToolOperation { turn: u32, ordinal: u32 },
    /// Preparation of one checkpoint before its publication point.
    CheckpointPrepare { sequence: u64 },
    /// Publication of one prepared checkpoint.
    CheckpointPublish { sequence: u64 },
    /// The final revision-fenced session mutation.
    SessionCommit { expected_revision: u64 },
}

/// Live execution authority for a native tool loop.
///
/// A native executor must call [`Self::revalidate`] immediately before each
/// [`NativeSideEffect`]. A successful check authorizes only that exact effect;
/// it cannot be cached, serialized, or reused for a later attempt.
#[async_trait]
pub trait NativeExecutionAuthority: Send + Sync {
    async fn revalidate(&self, effect: NativeSideEffect) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::*;

    #[derive(Default)]
    struct RecordingAuthority {
        effects: Mutex<Vec<NativeSideEffect>>,
    }

    #[async_trait]
    impl NativeExecutionAuthority for RecordingAuthority {
        async fn revalidate(&self, effect: NativeSideEffect) -> Result<()> {
            self.effects
                .lock()
                .expect("recording authority lock")
                .push(effect);
            Ok(())
        }
    }

    #[test]
    fn authority_is_object_safe_and_dispatches_exact_effect() {
        let recorder = Arc::new(RecordingAuthority::default());
        let authority: Arc<dyn NativeExecutionAuthority> = Arc::clone(&recorder) as Arc<_>;
        let effect = NativeSideEffect::ModelSend {
            turn: 4,
            wire_attempt: 2,
        };

        futures::executor::block_on(authority.revalidate(effect)).expect("authority check");

        let effects = recorder.effects.lock().expect("recording authority lock");
        assert_eq!(effects.as_slice(), &[effect]);
    }

    #[test]
    fn debug_output_contains_only_effect_coordinates() {
        let cases = [
            (
                NativeSideEffect::ModelSend {
                    turn: 7,
                    wire_attempt: 5,
                },
                "ModelSend { turn: 7, wire_attempt: 5 }",
            ),
            (
                NativeSideEffect::ToolOperation {
                    turn: 7,
                    ordinal: 3,
                },
                "ToolOperation { turn: 7, ordinal: 3 }",
            ),
            (
                NativeSideEffect::CheckpointPrepare { sequence: 11 },
                "CheckpointPrepare { sequence: 11 }",
            ),
            (
                NativeSideEffect::CheckpointPublish { sequence: 11 },
                "CheckpointPublish { sequence: 11 }",
            ),
            (
                NativeSideEffect::SessionCommit {
                    expected_revision: 13,
                },
                "SessionCommit { expected_revision: 13 }",
            ),
        ];

        for (effect, expected) in cases {
            assert_eq!(format!("{effect:?}"), expected);
        }
    }
}
