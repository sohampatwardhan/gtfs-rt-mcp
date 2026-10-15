use rmcp::model::CallToolRequestParams;
use rmcp::transport::TokioChildProcess;
use rmcp::ServiceExt;
use serde_json::{json, Value};

fn tool_json(result: &rmcp::model::CallToolResult) -> Value {
    let text = result
        .content
        .first()
        .and_then(|block| block.as_text())
        .expect("tool text")
        .text
        .clone();
    serde_json::from_str(&text).unwrap_or_else(|_| panic!("json: {text}"))
}

fn child(args: &[&str]) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_gtfs-rt-mcp"));
    command
        .args(args)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default());
    command.env("GTFS_RT_MCP_LOG", "error");
    command
}

#[tokio::test]
async fn env_registers_a_feed_without_register_feed() {
    let dir = std::env::temp_dir().join(format!(
        "gtfs-rt-mcp-env-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let paths = gtfs_rt_mcp::fixtures::materialize(&dir).unwrap();
    let secret = "env-only-header-value";
    let mut command = child(&["stdio"]);
    command
        .env("GTFS_RT_MCP_FEED_ID", "harbor")
        .env("GTFS_RT_MCP_FEED_SET", paths.feed_set.display().to_string())
        .env("GTFS_RT_MCP_RT_TTL", "30")
        .env("GTFS_RT_MCP_STATIC_TTL", "0")
        .env(
            "GTFS_RT_MCP_HTTP_HEADERS",
            format!(r#"{{"x-demo":"{secret}"}}"#),
        );
    let client =
        ().serve(TokioChildProcess::new(command).expect("spawn"))
            .await
            .expect("initialize");

    let listed = client
        .call_tool(CallToolRequestParams::new("list_feeds"))
        .await
        .expect("list_feeds");
    assert_ne!(listed.is_error, Some(true), "{listed:?}");
    let listed = tool_json(&listed);
    let rendered = listed.to_string();
    assert_eq!(listed["feeds"][0]["feed_id"], "harbor");
    assert_eq!(listed["feeds"][0]["ready"], true);
    assert_eq!(listed["feeds"][0]["headers"][0], "x-demo");
    assert_eq!(listed["feeds"][0]["realtime"]["ttl_sec"], 30);
    assert!(!rendered.contains(secret), "{rendered}");

    let status = client
        .call_tool(
            CallToolRequestParams::new("trip_status").with_arguments(
                json!({
                    "trip": "177",
                    "stop": "NHV",
                    "at_unix": paths.at_unix
                })
                .as_object()
                .unwrap()
                .clone(),
            ),
        )
        .await
        .expect("trip_status");
    assert_ne!(status.is_error, Some(true), "{status:?}");
    let status = tool_json(&status);
    assert_eq!(status["trips"][0]["at_stop"]["track"], "12");
    assert!(!status.to_string().contains(secret));

    let _ = client.cancel().await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn bad_env_stays_up_reports_errors_and_register_feed_still_works() {
    let dir = std::env::temp_dir().join(format!(
        "gtfs-rt-mcp-bad-env-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let paths = gtfs_rt_mcp::fixtures::materialize(&dir).unwrap();
    let secret = "header-secret-value";
    let mut command = child(&["stdio"]);
    command
        .env("GTFS_RT_MCP_FEED_ID", "harbor")
        .env("GTFS_RT_MCP_FEED_SET", paths.feed_set.display().to_string())
        .env("GTFS_RT_MCP_RT_TTL", "soon")
        .env(
            "GTFS_RT_MCP_HTTP_HEADERS",
            format!(r#"{{"Authorization":"Bearer {secret}"}}"#),
        )
        .env("GTFS_RT_MCP_FEEDS", "{not json");
    let client =
        ().serve(TokioChildProcess::new(command).expect("spawn"))
            .await
            .expect("initialize");

    let listed = client
        .call_tool(CallToolRequestParams::new("list_feeds"))
        .await
        .expect("list_feeds");
    assert_ne!(listed.is_error, Some(true), "{listed:?}");
    let listed = tool_json(&listed);
    let rendered = listed.to_string();
    assert!(rendered.contains("config_errors"), "{rendered}");
    assert!(rendered.contains("GTFS_RT_MCP_FEEDS"), "{rendered}");
    assert_eq!(listed["feeds"][0]["feed_id"], "harbor");
    assert_eq!(listed["feeds"][0]["ready"], false);
    assert!(
        listed["feeds"][0]["error"]
            .as_str()
            .unwrap_or("")
            .contains("soon"),
        "{rendered}"
    );
    assert_eq!(listed["feeds"][0]["headers"][0], "Authorization");
    assert!(!rendered.contains(secret), "{rendered}");

    let health = client
        .call_tool(CallToolRequestParams::new("feed_health"))
        .await
        .expect("feed_health");
    assert_ne!(health.is_error, Some(true), "{health:?}");
    let health = tool_json(&health);
    let health_text = health.to_string();
    assert_eq!(health["ready"], false);
    assert!(health_text.contains("config_errors"), "{health_text}");
    assert!(!health_text.contains(secret), "{health_text}");

    let registered = client
        .call_tool(
            CallToolRequestParams::new("register_feed").with_arguments(
                json!({
                    "feed_id": "harbor",
                    "feed_set": paths.feed_set.display().to_string(),
                    "rt_ttl_secs": 15
                })
                .as_object()
                .unwrap()
                .clone(),
            ),
        )
        .await
        .expect("register_feed");
    assert_ne!(registered.is_error, Some(true), "{registered:?}");
    let registered = tool_json(&registered);
    assert_eq!(registered["ready"], true);
    assert!(!registered.to_string().contains(secret));

    let status = client
        .call_tool(
            CallToolRequestParams::new("trip_status").with_arguments(
                json!({"trip": "177", "at_unix": paths.at_unix})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("trip_status");
    assert_ne!(status.is_error, Some(true), "{status:?}");
    assert_eq!(tool_json(&status)["trips"][0]["trip_short_name"], "177");

    let _ = client.cancel().await;
    let _ = std::fs::remove_dir_all(dir);
}
