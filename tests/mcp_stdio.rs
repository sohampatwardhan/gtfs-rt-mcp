use rmcp::model::CallToolRequestParams;
use rmcp::transport::TokioChildProcess;
use rmcp::ServiceExt;
use serde_json::json;

#[tokio::test]
async fn stdio_lists_tools_and_reads_harbor() {
    let dir = std::env::temp_dir().join(format!(
        "gtfs-rt-mcp-stdio-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let paths = gtfs_rt_mcp::fixtures::materialize(&dir).unwrap();
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_gtfs-rt-mcp"));
    command
        .arg("--feed-set")
        .arg(&paths.feed_set)
        .arg("stdio")
        .env("GTFS_RT_MCP_LOG", "error");
    let client = ()
        .serve(TokioChildProcess::new(command).expect("spawn"))
        .await
        .expect("initialize");
    let tools = client.peer().list_all_tools().await.expect("tools");
    let mut names: Vec<_> = tools.iter().map(|tool| tool.name.to_string()).collect();
    names.sort();
    assert_eq!(
        names,
        vec![
            "alerts",
            "decode_protobuf",
            "departure_board",
            "feed_health",
            "list_feeds",
            "refresh_feed",
            "register_feed",
            "search_routes",
            "search_stops",
            "trip_schedule",
            "trip_status",
            "trip_updates",
            "unregister_feed",
            "vehicle_positions",
        ]
    );

    let result = client
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
    assert_ne!(result.is_error, Some(true), "{result:?}");
    let text = result.content.first().and_then(|block| block.as_text()).unwrap();
    let value: serde_json::Value = serde_json::from_str(&text.text).unwrap();
    assert_eq!(value["trips"][0]["at_stop"]["track"], "12");
    assert_eq!(value["trips"][0]["trip_short_name"], "177");

    let _ = client.cancel().await;
    let _ = std::fs::remove_dir_all(dir);
}
