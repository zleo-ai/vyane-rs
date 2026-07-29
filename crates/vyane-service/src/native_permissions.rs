//! Frozen native permission vocabulary and monotonic config ceilings.
//!
//! Optional tools remain request opt-ins. User, project, explicit, and
//! managed configuration can only narrow an opt-in request; no config layer
//! grants a capability by itself.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use vyane_config::{
    NativeCommandNetworkRouteConfig, NativePermissionCeiling as ConfigCeiling,
    NativeToolPermissionEffectConfig,
};
use vyane_core::{Sandbox, WebSearchContextSize};
use vyane_harness::native::{
    MAX_NATIVE_TOOL_PERMISSION_LAYERS, NativeCommandNetworkPolicy, NativeCommandNetworkRoute,
    NativeCommandNetworkRule, NativeCommandPolicy, NativeCommandRule, NativeReadPolicy,
    NativeToolPermissionPolicy, NativeToolPermissionRule, NativeWebFetchPolicy,
    NativeWebSearchPolicy, NativeWritePolicy, PermissionEffect,
};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativePermissionSet {
    #[serde(default)]
    pub filesystem_read: NativeReadPolicy,
    #[serde(default)]
    pub filesystem_write: Option<NativeWritePolicy>,
    #[serde(default)]
    pub command_execution: Option<NativeCommandPolicy>,
    #[serde(default)]
    pub command_network: Option<NativeCommandNetworkPolicy>,
    #[serde(default)]
    pub web_search: Option<NativeWebSearchGrant>,
    #[serde(default)]
    pub web_fetch: Option<NativeWebFetchPolicy>,
    /// Per-submission tool decision layer. It can only restrict tools already
    /// registered by the capability fields above.
    #[serde(default)]
    pub tool_policy: Option<NativeToolPermissionPolicy>,
    /// Independent config ceilings accumulated during admission. API callers
    /// cannot inject these; the durable native policy freezes them explicitly.
    #[serde(skip)]
    pub tool_policy_ceilings: Vec<NativeToolPermissionPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeWebSearchGrant {
    pub target: String,
    #[serde(flatten)]
    pub policy: NativeWebSearchPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NativePermissionSetError {
    #[error("native permission policy is invalid")]
    InvalidPolicy,
    #[error("native command networking requires command execution")]
    NetworkWithoutCommand,
    #[error("native filesystem writes exceed the outer sandbox")]
    WriteOutsideSandbox,
    #[error("native permission request exceeds a configured ceiling")]
    ExceedsCeiling,
}

impl NativePermissionSet {
    pub fn validate(&self) -> Result<(), NativePermissionSetError> {
        if self.tool_policy_ceilings.len() + usize::from(self.tool_policy.is_some())
            > MAX_NATIVE_TOOL_PERMISSION_LAYERS
        {
            return Err(NativePermissionSetError::InvalidPolicy);
        }
        self.filesystem_read
            .validate()
            .map_err(|_| NativePermissionSetError::InvalidPolicy)?;
        if let Some(policy) = &self.filesystem_write {
            policy
                .validate()
                .map_err(|_| NativePermissionSetError::InvalidPolicy)?;
        }
        if let Some(policy) = &self.command_execution {
            if !self.filesystem_read.exclude.is_empty() {
                return Err(NativePermissionSetError::InvalidPolicy);
            }
            policy
                .validate()
                .map_err(|_| NativePermissionSetError::InvalidPolicy)?;
        }
        if let Some(policy) = &self.command_network {
            if self.command_execution.is_none() {
                return Err(NativePermissionSetError::NetworkWithoutCommand);
            }
            policy
                .validate()
                .map_err(|_| NativePermissionSetError::InvalidPolicy)?;
        }
        if let Some(search) = &self.web_search {
            if search.target.is_empty() || search.target.len() > 512 {
                return Err(NativePermissionSetError::InvalidPolicy);
            }
            search
                .policy
                .validate()
                .map_err(|_| NativePermissionSetError::InvalidPolicy)?;
        }
        if let Some(policy) = &self.web_fetch {
            policy
                .validate()
                .map_err(|_| NativePermissionSetError::InvalidPolicy)?;
        }
        if let Some(policy) = &self.tool_policy {
            policy
                .validate()
                .map_err(|_| NativePermissionSetError::InvalidPolicy)?;
        }
        for policy in &self.tool_policy_ceilings {
            policy
                .validate()
                .map_err(|_| NativePermissionSetError::InvalidPolicy)?;
        }
        Ok(())
    }

    pub fn validate_for_sandbox(&self, sandbox: Sandbox) -> Result<(), NativePermissionSetError> {
        self.validate()?;
        if sandbox == Sandbox::ReadOnly
            && (self.filesystem_write.is_some()
                || self
                    .command_execution
                    .as_ref()
                    .is_some_and(|policy| !policy.writable_roots.is_empty()))
        {
            return Err(NativePermissionSetError::WriteOutsideSandbox);
        }
        Ok(())
    }

    /// Apply one complete ceiling without granting an omitted request axis.
    ///
    /// Path exclusions and an omitted search-domain list are narrowed in
    /// place. Explicit request rules or limits outside the ceiling are
    /// rejected rather than silently dropped.
    pub fn restrict_by(
        &mut self,
        ceiling: &NativePermissionSet,
    ) -> Result<(), NativePermissionSetError> {
        self.validate()?;
        ceiling.validate()?;

        self.filesystem_read.exclude = merged_exclusions(
            &self.filesystem_read.exclude,
            &ceiling.filesystem_read.exclude,
        );
        self.filesystem_read
            .validate()
            .map_err(|_| NativePermissionSetError::InvalidPolicy)?;

        restrict_write(&mut self.filesystem_write, &ceiling.filesystem_write)?;
        restrict_command(&self.command_execution, &ceiling.command_execution)?;
        restrict_network(&self.command_network, &ceiling.command_network)?;
        restrict_search(&mut self.web_search, &ceiling.web_search)?;
        restrict_fetch(&self.web_fetch, &ceiling.web_fetch)?;
        if let Some(policy) = &ceiling.tool_policy {
            self.tool_policy_ceilings.push(policy.clone());
        }

        self.validate()
    }

    /// Apply every independent ceiling while preserving the distinction
    /// between an explicitly scoped search request and an originally
    /// unrestricted request that configuration is allowed to narrow.
    pub fn restrict_by_all(
        &mut self,
        ceilings: &[NativePermissionSet],
    ) -> Result<(), NativePermissionSetError> {
        let originally_unrestricted_search = self
            .web_search
            .as_ref()
            .is_some_and(|search| search.policy.allow_domains.is_none());
        let mut effective_search_domains: Option<Vec<String>> = None;

        for ceiling in ceilings {
            if originally_unrestricted_search {
                self.web_search
                    .as_mut()
                    .ok_or(NativePermissionSetError::InvalidPolicy)?
                    .policy
                    .allow_domains = None;
            }
            self.restrict_by(ceiling)?;
            if originally_unrestricted_search {
                let imposed = self
                    .web_search
                    .as_ref()
                    .ok_or(NativePermissionSetError::InvalidPolicy)?
                    .policy
                    .allow_domains
                    .as_deref();
                effective_search_domains =
                    intersect_domain_scopes(effective_search_domains.as_deref(), imposed)?;
            }
        }

        if originally_unrestricted_search {
            self.web_search
                .as_mut()
                .ok_or(NativePermissionSetError::InvalidPolicy)?
                .policy
                .allow_domains = effective_search_domains;
        }
        self.validate()
    }
}

impl TryFrom<&ConfigCeiling> for NativePermissionSet {
    type Error = NativePermissionSetError;

    fn try_from(value: &ConfigCeiling) -> Result<Self, Self::Error> {
        let set = Self {
            filesystem_read: NativeReadPolicy {
                exclude: value.filesystem_read.exclude.clone(),
            },
            filesystem_write: value
                .filesystem_write
                .as_ref()
                .map(|policy| NativeWritePolicy {
                    exclude: policy.exclude.clone(),
                }),
            command_execution: value
                .command_execution
                .as_ref()
                .map(|policy| NativeCommandPolicy {
                    allow: policy
                        .allow
                        .iter()
                        .map(|rule| NativeCommandRule {
                            program: rule.program.clone(),
                            args_prefix: rule.args_prefix.clone(),
                        })
                        .collect(),
                    writable_roots: policy.writable_roots.clone(),
                    max_seconds: policy.max_seconds,
                }),
            command_network: value.command_network.as_ref().map(|policy| {
                NativeCommandNetworkPolicy {
                    allow: policy
                        .allow
                        .iter()
                        .map(|rule| NativeCommandNetworkRule {
                            host: rule.host.clone(),
                            ports: rule.ports.clone(),
                        })
                        .collect(),
                    route: match policy.route {
                        NativeCommandNetworkRouteConfig::Direct => {
                            NativeCommandNetworkRoute::Direct
                        }
                        NativeCommandNetworkRouteConfig::EnvironmentProxy => {
                            NativeCommandNetworkRoute::EnvironmentProxy
                        }
                    },
                    max_connections: policy.max_connections,
                    max_bytes: policy.max_bytes,
                    connect_timeout_seconds: policy.connect_timeout_seconds,
                }
            }),
            web_search: value
                .web_search
                .as_ref()
                .map(|policy| NativeWebSearchGrant {
                    target: policy.target.clone(),
                    policy: NativeWebSearchPolicy {
                        allow_domains: policy.allow_domains.clone(),
                        max_searches: policy.max_searches,
                        search_context_size: policy.search_context_size,
                    },
                }),
            web_fetch: value.web_fetch.as_ref().map(|policy| NativeWebFetchPolicy {
                allow_domains: policy.allow_domains.clone(),
                max_fetches: policy.max_fetches,
                max_response_bytes: policy.max_response_bytes,
                max_redirects: policy.max_redirects,
            }),
            tool_policy: value
                .tool_policy
                .as_ref()
                .map(|policy| NativeToolPermissionPolicy {
                    default: permission_effect(policy.default),
                    rules: policy
                        .rules
                        .iter()
                        .map(|rule| NativeToolPermissionRule {
                            tool: rule.tool.clone(),
                            effect: permission_effect(rule.effect),
                            arguments: rule.arguments.clone(),
                        })
                        .collect(),
                }),
            tool_policy_ceilings: Vec::new(),
        };
        set.validate()?;
        Ok(set)
    }
}

const fn permission_effect(value: NativeToolPermissionEffectConfig) -> PermissionEffect {
    match value {
        NativeToolPermissionEffectConfig::Allow => PermissionEffect::Allow,
        NativeToolPermissionEffectConfig::Ask => PermissionEffect::Ask,
        NativeToolPermissionEffectConfig::Deny => PermissionEffect::Deny,
    }
}

fn merged_exclusions(left: &[String], right: &[String]) -> Vec<String> {
    left.iter()
        .chain(right)
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn restrict_write(
    request: &mut Option<NativeWritePolicy>,
    ceiling: &Option<NativeWritePolicy>,
) -> Result<(), NativePermissionSetError> {
    match (request.as_mut(), ceiling.as_ref()) {
        (None, _) => Ok(()),
        (Some(_), None) => Err(NativePermissionSetError::ExceedsCeiling),
        (Some(request), Some(ceiling)) => {
            request.exclude = merged_exclusions(&request.exclude, &ceiling.exclude);
            request
                .validate()
                .map_err(|_| NativePermissionSetError::InvalidPolicy)
        }
    }
}

fn restrict_command(
    request: &Option<NativeCommandPolicy>,
    ceiling: &Option<NativeCommandPolicy>,
) -> Result<(), NativePermissionSetError> {
    let Some(request) = request else {
        return Ok(());
    };
    let Some(ceiling) = ceiling else {
        return Err(NativePermissionSetError::ExceedsCeiling);
    };
    if request.max_seconds > ceiling.max_seconds
        || request.writable_roots.iter().any(|requested| {
            !ceiling
                .writable_roots
                .iter()
                .any(|allowed| writable_root_within(requested, allowed))
        })
        || request.allow.iter().any(|requested| {
            !ceiling.allow.iter().any(|allowed| {
                requested.program == allowed.program
                    && requested.args_prefix.starts_with(&allowed.args_prefix)
            })
        })
    {
        return Err(NativePermissionSetError::ExceedsCeiling);
    }
    Ok(())
}

fn writable_root_within(requested: &str, allowed: &str) -> bool {
    requested == allowed
        || requested
            .strip_prefix(allowed)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn restrict_network(
    request: &Option<NativeCommandNetworkPolicy>,
    ceiling: &Option<NativeCommandNetworkPolicy>,
) -> Result<(), NativePermissionSetError> {
    let Some(request) = request else {
        return Ok(());
    };
    let Some(ceiling) = ceiling else {
        return Err(NativePermissionSetError::ExceedsCeiling);
    };
    if request.route != ceiling.route
        || request.max_connections > ceiling.max_connections
        || request.max_bytes > ceiling.max_bytes
        || request.connect_timeout_seconds > ceiling.connect_timeout_seconds
        || request.allow.iter().any(|requested| {
            requested.ports.iter().any(|port| {
                !ceiling.allow.iter().any(|allowed| {
                    host_scope_within(&requested.host, &allowed.host)
                        && allowed.ports.contains(port)
                })
            })
        })
    {
        return Err(NativePermissionSetError::ExceedsCeiling);
    }
    Ok(())
}

fn restrict_search(
    request: &mut Option<NativeWebSearchGrant>,
    ceiling: &Option<NativeWebSearchGrant>,
) -> Result<(), NativePermissionSetError> {
    let Some(request) = request else {
        return Ok(());
    };
    let Some(ceiling) = ceiling else {
        return Err(NativePermissionSetError::ExceedsCeiling);
    };
    if request.target != ceiling.target
        || request.policy.max_searches > ceiling.policy.max_searches
        || context_rank(request.policy.search_context_size)
            > context_rank(ceiling.policy.search_context_size)
    {
        return Err(NativePermissionSetError::ExceedsCeiling);
    }
    restrict_domains(
        &mut request.policy.allow_domains,
        &ceiling.policy.allow_domains,
    )
}

fn restrict_fetch(
    request: &Option<NativeWebFetchPolicy>,
    ceiling: &Option<NativeWebFetchPolicy>,
) -> Result<(), NativePermissionSetError> {
    let Some(request) = request else {
        return Ok(());
    };
    let Some(ceiling) = ceiling else {
        return Err(NativePermissionSetError::ExceedsCeiling);
    };
    if request.max_fetches > ceiling.max_fetches
        || request.max_response_bytes > ceiling.max_response_bytes
        || request.max_redirects > ceiling.max_redirects
        || request.allow_domains.iter().any(|domain| {
            !ceiling
                .allow_domains
                .iter()
                .any(|base| domain_within(domain, base))
        })
    {
        return Err(NativePermissionSetError::ExceedsCeiling);
    }
    Ok(())
}

fn restrict_domains(
    request: &mut Option<Vec<String>>,
    ceiling: &Option<Vec<String>>,
) -> Result<(), NativePermissionSetError> {
    match (request.as_ref(), ceiling.as_ref()) {
        (_, None) => Ok(()),
        (None, Some(ceiling)) => {
            *request = Some(ceiling.clone());
            Ok(())
        }
        (Some(requested), Some(ceiling))
            if requested
                .iter()
                .all(|domain| ceiling.iter().any(|base| domain_within(domain, base))) =>
        {
            Ok(())
        }
        (Some(_), Some(_)) => Err(NativePermissionSetError::ExceedsCeiling),
    }
}

fn intersect_domain_scopes(
    left: Option<&[String]>,
    right: Option<&[String]>,
) -> Result<Option<Vec<String>>, NativePermissionSetError> {
    let (Some(left), Some(right)) = (left, right) else {
        return Ok(left.or(right).map(<[String]>::to_vec));
    };
    let intersection = left
        .iter()
        .flat_map(|left_domain| {
            right.iter().filter_map(move |right_domain| {
                if domain_within(left_domain, right_domain) {
                    Some(left_domain.clone())
                } else if domain_within(right_domain, left_domain) {
                    Some(right_domain.clone())
                } else {
                    None
                }
            })
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if intersection.is_empty() {
        return Err(NativePermissionSetError::ExceedsCeiling);
    }
    Ok(Some(intersection))
}

fn domain_within(requested: &str, ceiling: &str) -> bool {
    requested == ceiling || requested.ends_with(&format!(".{ceiling}"))
}

fn host_scope_within(requested: &str, ceiling: &str) -> bool {
    match (requested.strip_prefix("*."), ceiling.strip_prefix("*.")) {
        (None, None) => requested == ceiling,
        (None, Some(ceiling)) => domain_within(requested, ceiling) && requested != ceiling,
        (Some(_), None) => false,
        (Some(requested), Some(ceiling)) => domain_within(requested, ceiling),
    }
}

const fn context_rank(size: WebSearchContextSize) -> u8 {
    match size {
        WebSearchContextSize::Low => 0,
        WebSearchContextSize::Medium => 1,
        WebSearchContextSize::High => 2,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use vyane_config::{NativeToolPermissionPolicyConfig, NativeToolPermissionRuleConfig};

    fn command(program: &str, prefix: &[&str], seconds: u64) -> NativeCommandPolicy {
        NativeCommandPolicy {
            allow: vec![NativeCommandRule {
                program: program.into(),
                args_prefix: prefix.iter().map(|value| (*value).into()).collect(),
            }],
            writable_roots: Vec::new(),
            max_seconds: seconds,
        }
    }

    #[test]
    fn config_ceiling_never_grants_an_omitted_axis() {
        let mut request = NativePermissionSet::default();
        let ceiling = NativePermissionSet {
            command_execution: Some(command("git", &["status"], 30)),
            ..NativePermissionSet::default()
        };
        request.restrict_by(&ceiling).unwrap();
        assert!(request.command_execution.is_none());
    }

    #[test]
    fn tool_decision_config_is_retained_as_an_independent_ceiling() {
        let config = ConfigCeiling {
            tool_policy: Some(NativeToolPermissionPolicyConfig {
                default: NativeToolPermissionEffectConfig::Allow,
                rules: vec![NativeToolPermissionRuleConfig {
                    tool: "run_command".into(),
                    effect: NativeToolPermissionEffectConfig::Deny,
                    arguments: BTreeMap::from([("program".into(), "^sh$".into())]),
                }],
            }),
            ..ConfigCeiling::default()
        };
        let ceiling = NativePermissionSet::try_from(&config).unwrap();
        let mut request = NativePermissionSet {
            tool_policy: Some(NativeToolPermissionPolicy {
                default: PermissionEffect::Allow,
                rules: vec![NativeToolPermissionRule {
                    tool: "run_command".into(),
                    effect: PermissionEffect::Ask,
                    arguments: BTreeMap::new(),
                }],
            }),
            ..NativePermissionSet::default()
        };
        request.restrict_by(&ceiling).unwrap();

        assert_eq!(
            request.tool_policy.as_ref().unwrap().rules[0].effect,
            PermissionEffect::Ask
        );
        assert_eq!(request.tool_policy_ceilings.len(), 1);
        assert_eq!(
            request.tool_policy_ceilings[0].rules[0].effect,
            PermissionEffect::Deny
        );
    }

    #[test]
    fn command_rule_and_limit_must_be_inside_every_ceiling() {
        let ceiling = NativePermissionSet {
            command_execution: Some(command("git", &["status"], 30)),
            ..NativePermissionSet::default()
        };
        let mut narrow = NativePermissionSet {
            command_execution: Some(command("git", &["status", "--short"], 10)),
            ..NativePermissionSet::default()
        };
        narrow.restrict_by(&ceiling).unwrap();

        let mut broad = NativePermissionSet {
            command_execution: Some(command("git", &[], 30)),
            ..NativePermissionSet::default()
        };
        assert_eq!(
            broad.restrict_by(&ceiling),
            Err(NativePermissionSetError::ExceedsCeiling)
        );
    }

    #[test]
    fn writable_command_roots_must_be_inside_every_ceiling_and_outer_sandbox() {
        let mut ceiling_command = command("cargo", &["fmt"], 30);
        ceiling_command.writable_roots = vec!["crates".into(), "Cargo.toml".into()];
        let ceiling = NativePermissionSet {
            command_execution: Some(ceiling_command),
            ..NativePermissionSet::default()
        };
        let mut narrow_command = command("cargo", &["fmt"], 20);
        narrow_command.writable_roots = vec!["crates/vyane-core".into()];
        let mut narrow = NativePermissionSet {
            command_execution: Some(narrow_command),
            ..NativePermissionSet::default()
        };
        narrow.restrict_by(&ceiling).unwrap();
        assert_eq!(
            narrow.validate_for_sandbox(Sandbox::ReadOnly),
            Err(NativePermissionSetError::WriteOutsideSandbox)
        );
        narrow.validate_for_sandbox(Sandbox::Write).unwrap();

        let mut outside_command = command("cargo", &["fmt"], 20);
        outside_command.writable_roots = vec!["docs".into()];
        let mut outside = NativePermissionSet {
            command_execution: Some(outside_command),
            ..NativePermissionSet::default()
        };
        assert_eq!(
            outside.restrict_by(&ceiling),
            Err(NativePermissionSetError::ExceedsCeiling)
        );
    }

    #[test]
    fn command_network_host_ports_and_limits_must_be_inside_every_ceiling() {
        let command_policy = Some(command("cargo", &["fetch"], 30));
        let network =
            |host: &str, ports: Vec<u16>, max_connections: u32| NativeCommandNetworkPolicy {
                allow: vec![NativeCommandNetworkRule {
                    host: host.into(),
                    ports,
                }],
                route: NativeCommandNetworkRoute::Direct,
                max_connections,
                max_bytes: 1024,
                connect_timeout_seconds: 2,
            };
        let ceiling = NativePermissionSet {
            command_execution: command_policy.clone(),
            command_network: Some(network("*.example.com", vec![443, 8443], 4)),
            ..NativePermissionSet::default()
        };
        let mut narrow = NativePermissionSet {
            command_execution: command_policy.clone(),
            command_network: Some(network("api.example.com", vec![443], 2)),
            ..NativePermissionSet::default()
        };
        narrow.restrict_by(&ceiling).unwrap();

        let mut wrong_port = NativePermissionSet {
            command_execution: command_policy,
            command_network: Some(network("api.example.com", vec![80], 2)),
            ..NativePermissionSet::default()
        };
        assert_eq!(
            wrong_port.restrict_by(&ceiling),
            Err(NativePermissionSetError::ExceedsCeiling)
        );

        let split_ports_ceiling = NativePermissionSet {
            command_execution: Some(command("cargo", &["fetch"], 30)),
            command_network: Some(NativeCommandNetworkPolicy {
                allow: vec![
                    NativeCommandNetworkRule {
                        host: "api.example.com".into(),
                        ports: vec![443],
                    },
                    NativeCommandNetworkRule {
                        host: "api.example.com".into(),
                        ports: vec![8443],
                    },
                ],
                route: NativeCommandNetworkRoute::Direct,
                max_connections: 4,
                max_bytes: 1024,
                connect_timeout_seconds: 2,
            }),
            ..NativePermissionSet::default()
        };
        let mut combined_request = NativePermissionSet {
            command_execution: Some(command("cargo", &["fetch"], 30)),
            command_network: Some(network("api.example.com", vec![443, 8443], 2)),
            ..NativePermissionSet::default()
        };
        combined_request.restrict_by(&split_ports_ceiling).unwrap();
    }

    #[test]
    fn search_any_domain_is_narrowed_and_cannot_widen_again() {
        let ceiling = NativePermissionSet {
            web_search: Some(NativeWebSearchGrant {
                target: "search".into(),
                policy: NativeWebSearchPolicy {
                    allow_domains: Some(vec!["example.com".into()]),
                    max_searches: 4,
                    search_context_size: WebSearchContextSize::Medium,
                },
            }),
            ..NativePermissionSet::default()
        };
        let mut request = NativePermissionSet {
            web_search: Some(NativeWebSearchGrant {
                target: "search".into(),
                policy: NativeWebSearchPolicy {
                    allow_domains: None,
                    max_searches: 2,
                    search_context_size: WebSearchContextSize::Low,
                },
            }),
            ..NativePermissionSet::default()
        };
        request.restrict_by(&ceiling).unwrap();
        assert_eq!(
            request.web_search.unwrap().policy.allow_domains,
            Some(vec!["example.com".into()])
        );
    }

    #[test]
    fn unrestricted_search_intersects_successive_config_domain_scopes() {
        let search = |domains: Option<Vec<&str>>| NativeWebSearchGrant {
            target: "search".into(),
            policy: NativeWebSearchPolicy {
                allow_domains: domains.map(|domains| {
                    domains
                        .into_iter()
                        .map(std::string::ToString::to_string)
                        .collect()
                }),
                max_searches: 2,
                search_context_size: WebSearchContextSize::Low,
            },
        };
        let mut request = NativePermissionSet {
            web_search: Some(search(None)),
            ..NativePermissionSet::default()
        };
        let broad = NativePermissionSet {
            web_search: Some(search(Some(vec!["example.com"]))),
            ..NativePermissionSet::default()
        };
        let narrow = NativePermissionSet {
            web_search: Some(search(Some(vec!["docs.example.com"]))),
            ..NativePermissionSet::default()
        };
        request.restrict_by_all(&[broad, narrow]).unwrap();
        assert_eq!(
            request.web_search.unwrap().policy.allow_domains,
            Some(vec!["docs.example.com".into()])
        );

        let mut explicit_broad = NativePermissionSet {
            web_search: Some(search(Some(vec!["example.com"]))),
            ..NativePermissionSet::default()
        };
        let narrow = NativePermissionSet {
            web_search: Some(search(Some(vec!["docs.example.com"]))),
            ..NativePermissionSet::default()
        };
        assert_eq!(
            explicit_broad.restrict_by_all(&[narrow]),
            Err(NativePermissionSetError::ExceedsCeiling)
        );
    }

    #[test]
    fn filesystem_exclusions_accumulate_deterministically() {
        let mut request = NativePermissionSet {
            filesystem_read: NativeReadPolicy::excluding(vec!["private/**".into()]),
            filesystem_write: Some(NativeWritePolicy::workspace()),
            ..NativePermissionSet::default()
        };
        let ceiling = NativePermissionSet {
            filesystem_read: NativeReadPolicy::excluding(vec![".env*".into()]),
            filesystem_write: Some(NativeWritePolicy::excluding(vec![".git/**".into()])),
            ..NativePermissionSet::default()
        };
        request.restrict_by(&ceiling).unwrap();
        assert_eq!(request.filesystem_read.exclude, [".env*", "private/**"]);
        assert_eq!(request.filesystem_write.unwrap().exclude, [".git/**"]);
    }
}
