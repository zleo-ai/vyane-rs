//! Real duplex handshake coverage for the dual-era MCP 2026 surface.
//!
//! `initialize` stays on the 2025-11-25 legacy lifecycle even when a client
//! offers 2026-07-28. `server/discover` can still negotiate 2026-07-28. Both
//! paths keep the six-tool library contract.

#![allow(clippy::unwrap_used)]

use rmcp::{
    ClientLifecycleMode, ClientServiceExt, ServerHandler, ServiceExt as _,
    model::{CallToolRequestParams, ClientInfo, ProtocolVersion},
};
use vyane_mcp::VyaneMcpServer;
use vyane_service::{StoragePaths, VyaneService};

const BASE_TOOLS: [&str; 6] = [
    "vyane_broadcast",
    "vyane_check",
    "vyane_dispatch",
    "vyane_history",
    "vyane_route",
    "vyane_sessions",
];

#[test]
fn server_info_prefers_legacy_initialize_and_advertises_2026_on_discover() {
    let (server, _root) = test_server().unwrap();
    assert_eq!(
        server.get_info().protocol_version,
        ProtocolVersion::V_2025_11_25
    );
    assert_eq!(
        server.supported_protocol_versions().as_ref(),
        ProtocolVersion::known_up_to(&ProtocolVersion::V_2026_07_28)
    );
    assert!(
        server
            .supported_protocol_versions()
            .contains(&ProtocolVersion::V_2026_07_28)
    );
}

#[tokio::test]
async fn initialize_handshake_stays_on_legacy_even_when_client_offers_2026() -> anyhow::Result<()> {
    let (server, _root) = test_server()?;
    let (server_transport, client_transport) = tokio::io::duplex(64 * 1024);
    let handle = tokio::spawn(async move {
        server.serve(server_transport).await?.waiting().await?;
        anyhow::Ok(())
    });
    let client = ClientInfo::default()
        .with_protocol_version(ProtocolVersion::V_2026_07_28)
        .serve(client_transport)
        .await?;

    let negotiated = client
        .peer_info()
        .expect("initialize must record server info")
        .protocol_version
        .clone();
    assert_eq!(negotiated, ProtocolVersion::V_2025_11_25);
    assert_tool_surface(&client).await?;
    call_check(&client).await?;

    client.cancel().await?;
    handle.await??;
    Ok(())
}

#[tokio::test]
async fn discover_handshake_negotiates_2026_and_keeps_the_tool_surface() -> anyhow::Result<()> {
    let (server, _root) = test_server()?;
    let (server_transport, client_transport) = tokio::io::duplex(64 * 1024);
    let handle = tokio::spawn(async move {
        server.serve(server_transport).await?.waiting().await?;
        anyhow::Ok(())
    });
    let client = ClientInfo::default()
        .serve_with_lifecycle(
            client_transport,
            ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await?;

    let negotiated = client
        .peer_info()
        .expect("discover must record server info")
        .protocol_version
        .clone();
    assert_eq!(negotiated, ProtocolVersion::V_2026_07_28);
    assert_tool_surface(&client).await?;
    call_check(&client).await?;

    client.cancel().await?;
    handle.await??;
    Ok(())
}

async fn assert_tool_surface(
    client: &rmcp::service::RunningService<rmcp::RoleClient, ClientInfo>,
) -> anyhow::Result<()> {
    let mut names = client
        .list_all_tools()
        .await?
        .into_iter()
        .map(|tool| tool.name.to_string())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, BASE_TOOLS);
    Ok(())
}

async fn call_check(
    client: &rmcp::service::RunningService<rmcp::RoleClient, ClientInfo>,
) -> anyhow::Result<()> {
    let result = client
        .call_tool(CallToolRequestParams::new("vyane_check"))
        .await?;
    assert_ne!(result.is_error, Some(true));
    Ok(())
}

fn test_server() -> anyhow::Result<(VyaneMcpServer, tempfile::TempDir)> {
    let root = tempfile::Builder::new()
        .prefix("vyane-mcp-handshake-")
        .tempdir()?;
    let config = root.path().join("config.toml");
    std::fs::write(
        &config,
        r#"
        [providers.safe]
        base_url = "https://example.invalid/v1"
        api_key_env = "VYANE_TEST_UNUSED_KEY"
        auth_style = "bearer"
        protocol = "openai_chat"
        default_model = "safe"

        [profiles.default]
        provider = "safe"
        model = "safe"
        "#,
    )?;
    let loaded = vyane_service::load_config(Some(&config))?;
    let service = VyaneService::from_loaded_with_paths(
        loaded,
        StoragePaths::from_data_dir(root.path().join("data")),
    )?;
    Ok((VyaneMcpServer::new(service), root))
}
