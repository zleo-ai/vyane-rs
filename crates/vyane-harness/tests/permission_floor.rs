use std::collections::BTreeMap;

use vyane_harness::native::{
    NativeToolPermissionPolicy, NativeToolPermissionRule, PermissionEffect, PermissionPolicy,
    PermissionRule, PermissionRuleError, ToolCall, ToolContext,
};

fn context(directory: &std::path::Path) -> ToolContext {
    ToolContext::new(directory).expect("tool context")
}

fn call(name: &str) -> ToolCall {
    ToolCall {
        id: "call-floor".into(),
        name: name.into(),
        arguments: BTreeMap::new(),
    }
}

fn rule(effect: PermissionEffect) -> PermissionRule {
    PermissionRule::new("secret_tool_name", effect).expect("permission rule")
}

#[test]
fn non_deny_floor_rules_are_rejected_and_do_not_weaken_a_later_deny() {
    let directory = tempfile::tempdir().expect("temp dir");
    let context = context(directory.path());
    let mut policy = PermissionPolicy::allow_by_default();
    for effect in [PermissionEffect::Allow, PermissionEffect::Ask] {
        let error = policy
            .push_floor_rule(rule(effect))
            .expect_err("a floor rule must deny");
        assert!(matches!(error, PermissionRuleError::FloorRuleMustDeny));
        assert_eq!(error.as_str(), "floor_rule_must_deny");
        assert_eq!(
            error.to_string(),
            "a non-overridable permission floor rule must deny"
        );
        assert!(!error.as_str().contains("secret"));
        assert!(!error.to_string().contains("secret_tool_name"));
    }

    let open = policy.decide(&call("secret_tool_name"), &context);
    assert_eq!(open.effect, PermissionEffect::Allow);
    assert!(!open.matched_floor);
    assert!(open.matched_rule_index.is_none());
    assert!(open.approval.is_none());

    policy
        .push_floor_rule(rule(PermissionEffect::Deny))
        .expect("deny floor");
    policy.push_rule(rule(PermissionEffect::Allow));
    policy
        .push_restriction(&NativeToolPermissionPolicy {
            default: PermissionEffect::Allow,
            rules: vec![NativeToolPermissionRule {
                tool: "secret_tool_name".into(),
                effect: PermissionEffect::Allow,
                arguments: BTreeMap::new(),
            }],
        })
        .expect("allow restriction");

    let denied = policy.decide(&call("secret_tool_name"), &context);
    assert_eq!(denied.effect, PermissionEffect::Deny);
    assert!(denied.matched_floor);
    assert_eq!(denied.matched_rule_index, Some(0));
    assert!(denied.approval.is_none());

    let other = policy.decide(&call("write_file"), &context);
    assert_eq!(other.effect, PermissionEffect::Allow);
    assert!(!other.matched_floor);
}
