//! Trusted principal to durable-owner binding.
//!
//! These types are intentionally not serializable. Protocol front-ends must
//! authenticate through an application-owned authenticator, then resolve the
//! resulting principal through an application-owned resolver before they can
//! obtain an [`OwnerContext`].

use std::fmt;
use std::sync::Arc;

use anyhow::Result;

const LOCAL_OWNER: &str = "local";
const MAX_OWNER_BYTES: usize = 256;

/// An identity asserted by a completed authentication boundary.
///
/// Construction is deliberately explicit: parsing a request body, task label,
/// or query parameter must never create an authenticated principal implicitly.
#[derive(Clone, PartialEq, Eq)]
pub struct AuthenticatedPrincipal {
    subject: Arc<str>,
}

impl AuthenticatedPrincipal {
    fn new(subject: String) -> std::result::Result<Self, OwnerContextError> {
        validate_identity(&subject).map_err(|()| OwnerContextError::InvalidPrincipal)?;
        Ok(Self {
            subject: Arc::from(subject),
        })
    }

    /// Stable provider subject exposed only to trusted owner resolvers.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }
}

/// Trusted application authentication boundary.
///
/// Protocol handlers provide only the credential bytes they received. They do
/// not get a public constructor that can relabel an arbitrary request field as
/// an authenticated principal.
pub trait PrincipalAuthenticator: Send + Sync {
    fn authenticate(&self, credential: &[u8]) -> Result<String>;
}

impl fmt::Debug for AuthenticatedPrincipal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthenticatedPrincipal")
            .field("subject", &"[redacted]")
            .finish()
    }
}

/// Trusted application policy that maps an authenticated principal to the
/// canonical owner key used by durable stores.
pub trait PrincipalOwnerResolver: Send + Sync {
    fn resolve_owner(&self, principal: &AuthenticatedPrincipal) -> Result<String>;
}

/// Fixed authentication and owner-resolution policy used by a protocol host.
#[derive(Clone)]
pub struct OwnerContextFactory {
    authenticator: Arc<dyn PrincipalAuthenticator>,
    resolver: Arc<dyn PrincipalOwnerResolver>,
}

impl OwnerContextFactory {
    #[must_use]
    pub fn new(
        authenticator: Arc<dyn PrincipalAuthenticator>,
        resolver: Arc<dyn PrincipalOwnerResolver>,
    ) -> Self {
        Self {
            authenticator,
            resolver,
        }
    }

    /// Authenticate a credential and freeze its canonical durable owner.
    pub fn authenticate(
        &self,
        credential: &[u8],
    ) -> std::result::Result<OwnerContext, OwnerContextError> {
        let subject = self
            .authenticator
            .authenticate(credential)
            .map_err(|_| OwnerContextError::AuthenticationFailed)?;
        let principal = AuthenticatedPrincipal::new(subject)?;
        OwnerContext::from_authenticated(&principal, self.resolver.as_ref())
    }
}

impl fmt::Debug for OwnerContextFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerContextFactory")
            .finish_non_exhaustive()
    }
}

/// Opaque, non-serializable durable-owner authority.
///
/// It can only be minted from a verified principal through a trusted resolver,
/// or through the explicit single-user local compatibility path.
#[derive(Clone)]
pub struct OwnerContext {
    owner: Arc<str>,
}

impl OwnerContext {
    fn from_authenticated(
        principal: &AuthenticatedPrincipal,
        resolver: &dyn PrincipalOwnerResolver,
    ) -> std::result::Result<Self, OwnerContextError> {
        let owner = resolver
            .resolve_owner(principal)
            .map_err(|_| OwnerContextError::ResolutionFailed)?;
        validate_identity(&owner).map_err(|()| OwnerContextError::InvalidOwner)?;
        if owner == LOCAL_OWNER {
            return Err(OwnerContextError::ReservedOwner);
        }
        Ok(Self {
            owner: Arc::from(owner),
        })
    }

    /// Explicit compatibility authority for a trusted single-user process.
    #[must_use]
    pub fn single_user_local() -> Self {
        Self {
            owner: Arc::from(LOCAL_OWNER),
        }
    }

    pub(crate) fn owner(&self) -> Arc<str> {
        Arc::clone(&self.owner)
    }
}

impl fmt::Debug for OwnerContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerContext")
            .field("owner", &"[redacted]")
            .finish()
    }
}

/// Closed, value-free failure taxonomy for principal-to-owner binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerContextError {
    AuthenticationFailed,
    InvalidPrincipal,
    ResolutionFailed,
    InvalidOwner,
    ReservedOwner,
}

impl fmt::Display for OwnerContextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::AuthenticationFailed => "principal authentication failed",
            Self::InvalidPrincipal => "authenticated principal is invalid",
            Self::ResolutionFailed => "owner resolution failed",
            Self::InvalidOwner => "resolved owner is invalid",
            Self::ReservedOwner => "resolved owner is reserved",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for OwnerContextError {}

fn validate_identity(value: &str) -> std::result::Result<(), ()> {
    if value.is_empty()
        || value.len() > MAX_OWNER_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use static_assertions::assert_not_impl_any;

    assert_not_impl_any!(AuthenticatedPrincipal: serde::Serialize, serde::de::DeserializeOwned);
    assert_not_impl_any!(OwnerContext: serde::Serialize, serde::de::DeserializeOwned);
    assert_not_impl_any!(OwnerContextError: serde::Serialize, serde::de::DeserializeOwned);

    struct Authenticator;

    impl PrincipalAuthenticator for Authenticator {
        fn authenticate(&self, credential: &[u8]) -> Result<String> {
            String::from_utf8(credential.to_vec()).map_err(anyhow::Error::from)
        }
    }

    struct Resolver;

    impl PrincipalOwnerResolver for Resolver {
        fn resolve_owner(&self, principal: &AuthenticatedPrincipal) -> Result<String> {
            Ok(format!("owner:{}", principal.subject()))
        }
    }

    #[test]
    fn debug_is_redacted_and_resolution_is_explicit() {
        let factory = OwnerContextFactory::new(Arc::new(Authenticator), Arc::new(Resolver));
        let context = factory.authenticate(b"CANARY_SUBJECT").unwrap();
        assert!(!format!("{factory:?}").contains("CANARY_SUBJECT"));
        assert!(!format!("{context:?}").contains("CANARY_SUBJECT"));
        assert_eq!(&*context.owner(), "owner:CANARY_SUBJECT");
    }

    #[test]
    fn invalid_resolver_output_is_rejected() {
        struct InvalidResolver;
        impl PrincipalOwnerResolver for InvalidResolver {
            fn resolve_owner(&self, _: &AuthenticatedPrincipal) -> Result<String> {
                Ok(String::new())
            }
        }
        let factory = OwnerContextFactory::new(Arc::new(Authenticator), Arc::new(InvalidResolver));
        assert!(factory.authenticate(b"subject").is_err());
    }

    #[test]
    fn errors_never_echo_principal_or_resolver_details() {
        struct LeakingResolver;
        impl PrincipalOwnerResolver for LeakingResolver {
            fn resolve_owner(&self, _: &AuthenticatedPrincipal) -> Result<String> {
                anyhow::bail!("CANARY_RESOLVER_ERROR")
            }
        }

        let invalid = AuthenticatedPrincipal::new(" CANARY_PRINCIPAL ".into()).unwrap_err();
        assert!(!format!("{invalid:?} {invalid}").contains("CANARY_PRINCIPAL"));
        let factory = OwnerContextFactory::new(Arc::new(Authenticator), Arc::new(LeakingResolver));
        let error = factory.authenticate(b"subject").unwrap_err();
        let rendered = format!("{error:?} {error}");
        assert!(!rendered.contains("CANARY_RESOLVER_ERROR"));
        assert!(!rendered.contains("subject"));
        assert_eq!(error, OwnerContextError::ResolutionFailed);
    }

    #[test]
    fn identities_reject_whitespace_and_controls() {
        let factory = OwnerContextFactory::new(Arc::new(Authenticator), Arc::new(Resolver));
        for invalid in ["", " leading", "trailing ", "line\nbreak", "nul\0byte"] {
            assert!(factory.authenticate(invalid.as_bytes()).is_err());
        }
    }

    #[test]
    fn authenticated_principals_cannot_enter_the_reserved_local_namespace() {
        struct LocalResolver;
        impl PrincipalOwnerResolver for LocalResolver {
            fn resolve_owner(&self, _: &AuthenticatedPrincipal) -> Result<String> {
                Ok(LOCAL_OWNER.into())
            }
        }
        let factory = OwnerContextFactory::new(Arc::new(Authenticator), Arc::new(LocalResolver));
        assert_eq!(
            factory.authenticate(b"subject").unwrap_err(),
            OwnerContextError::ReservedOwner
        );
        assert_eq!(&*OwnerContext::single_user_local().owner(), LOCAL_OWNER);
    }
}
