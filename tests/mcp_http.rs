use std::sync::Arc;

use rmcp::model::CallToolRequestParams;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::ServiceExt;
use serde_json::json;

#[tokio::test]
async fn http_lists_tools_and_answers_departure_board() {
    let dir = std::env::temp_dir().join(format!(
        "gtfs-rt-mcp-http-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let paths = gtfs_rt_mcp::fixtures::materialize(&dir).unwrap();
    let catalog = Arc::new(gtfs_rt_mcp::Catalog::new().unwrap());
    catalog
        .register(gtfs_rt_mcp::RegisterRequest {
            feed_set: Some(paths.feed_set.display().to_string()),
            ..Default::default()
        })
        .await
        .unwrap();
    let (addr, handle) = gtfs_rt_mcp::spawn_http(
        catalog,
        "127.0.0.1:0".parse().unwrap(),
        Vec::new(),
    )
    .await
    .unwrap();

    let health = reqwest::Client::new()
        .get(format!("http://{addr}/healthz"))
        .send()
        .await
        .unwrap();
    assert!(health.status().is_success());

    let client = ()
        .serve(StreamableHttpClientTransport::from_uri(format!(
            "http://{addr}/mcp"
        )))
        .await
        .expect("initialize");
    let tools = client.peer().list_all_tools().await.expect("tools");
    assert_eq!(tools.len(), 14);

    let result = client
        .call_tool(
            CallToolRequestParams::new("departure_board").with_arguments(
                json!({
                    "stop": "New Haven Union Station",
                    "at_unix": paths.at_unix,
                    "window_minutes": 120,
                    "past_minutes": 5
                })
                .as_object()
                .unwrap()
                .clone(),
            ),
        )
        .await
        .expect("board");
    assert_ne!(result.is_error, Some(true), "{result:?}");
    let text = result.content.first().and_then(|block| block.as_text()).unwrap();
    let value: serde_json::Value = serde_json::from_str(&text.text).unwrap();
    let names: Vec<_> = value["departures"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["trip_short_name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, vec!["177".to_string(), "B1".to_string()]);

    let _ = client.cancel().await;
    handle.abort();
    let _ = std::fs::remove_dir_all(dir);
}
