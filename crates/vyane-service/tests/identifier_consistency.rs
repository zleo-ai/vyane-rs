//! Every store that shares the `vyane-core` identifier rule agrees on which
//! owners and ids are legal: the same hostile value is rejected by the core
//! rule, `OwnerContext`, the kernel store, the message store, the agent store
//! and the dispatch label parser, and the same canonical value (with an
//! internal space) is accepted by all of them.
#![allow(clippy::unwrap_used)]

// Only `private_tempdir` is needed here.
#[allow(unused_imports)]
mod common;

use std::sync::Arc;

use chrono::Utc;
use vyane_agent::{AgentStore as _, SqliteAgentStore};
use vyane_message::{MessageStore as _, SqliteMessageStore};
use vyane_service::{
    AuthenticatedPrincipal, KernelStore, KernelStoreError, OwnerContextError, OwnerContextFactory,
    PrincipalAuthenticator, PrincipalOwnerResolver, parse_labels,
};

const HOSTILE: &[&str] = &[
    "",
    " ",
    "\t",
    " local",
    "local ",
    "local\n",
    "lo\ncal",
    "lo\0cal",
    "lo\u{1b}[2Jcal",
    "lo\u{7f}cal",
    "lo\u{85}cal",
];

const CANONICAL: &str = "local user";

struct FixedSubject;

impl PrincipalAuthenticator for FixedSubject {
    fn authenticate(&self, _credential: &[u8]) -> anyhow::Result<String> {
        Ok("subject".into())
    }
}

/// Resolves the owner to the credential bytes, so each case can try one value.
struct CredentialOwner(String);

impl PrincipalOwnerResolver for CredentialOwner {
    fn resolve_owner(&self, _principal: &AuthenticatedPrincipal) -> anyhow::Result<String> {
        Ok(self.0.clone())
    }
}

fn owner_context_accepts(owner: &str) -> bool {
    let factory = OwnerContextFactory::new(
        Arc::new(FixedSubject),
        Arc::new(CredentialOwner(owner.to_string())),
    );
    match factory.authenticate(b"credential") {
        Ok(_) => true,
        Err(OwnerContextError::InvalidOwner) => false,
        Err(other) => panic!("unexpected owner context error for {owner:?}: {other}"),
    }
}

#[test]
fn hostile_identifiers_are_rejected_by_every_store_that_shares_the_rule() {
    let dir = common::private_tempdir();
    let kernel = KernelStore::open(dir.path().join("kernel.sqlite")).unwrap();
    let messages = SqliteMessageStore::open(dir.path().join("messages.sqlite")).unwrap();
    let agents = SqliteAgentStore::open(dir.path().join("agents.sqlite")).unwrap();
    let digest = "a".repeat(64);

    for &value in HOSTILE {
        assert!(
            vyane_core::validate_identifier("owner", value, 128).is_err(),
            "core rule accepted {value:?}"
        );
        assert!(
            !owner_context_accepts(value),
            "owner context accepted {value:?}"
        );

        let kernel_owner = kernel
            .apply_effect_once(value, "effect-1", &digest, None, None, Utc::now())
            .unwrap_err();
        assert_eq!(kernel_owner, KernelStoreError::InvalidInput("owner"));
        let kernel_id = kernel
            .apply_effect_once(CANONICAL, value, &digest, None, None, Utc::now())
            .unwrap_err();
        assert_eq!(kernel_id, KernelStoreError::InvalidInput("effect_id"));

        assert!(
            matches!(
                messages.get(value, "message-1"),
                Err(vyane_message::MessageStoreError::InvalidInput(_))
            ),
            "message store accepted owner {value:?}"
        );
        assert!(
            matches!(
                messages.get(CANONICAL, value),
                Err(vyane_message::MessageStoreError::InvalidInput(_))
            ),
            "message store accepted message id {value:?}"
        );

        assert!(
            matches!(
                agents.get_run(value, "run-1"),
                Err(vyane_agent::AgentStoreError::InvalidInput(_))
            ),
            "agent store accepted owner {value:?}"
        );
        assert!(
            matches!(
                agents.get_run(CANONICAL, value),
                Err(vyane_agent::AgentStoreError::InvalidInput(_))
            ),
            "agent store accepted run id {value:?}"
        );

        assert!(
            parse_labels(vec![format!("{value}=x")]).is_err(),
            "label parser accepted key {value:?}"
        );
    }

    assert!(vyane_core::validate_identifier("owner", CANONICAL, 128).is_ok());
    assert!(owner_context_accepts(CANONICAL));
    assert!(
        kernel
            .apply_effect_once(CANONICAL, "effect 1", &digest, None, None, Utc::now())
            .unwrap()
    );
    assert!(messages.get(CANONICAL, "message 1").unwrap().is_none());
    assert!(agents.get_run(CANONICAL, "run 1").unwrap().is_none());
    assert!(parse_labels(vec![format!("{CANONICAL}=x")]).is_ok());
}
