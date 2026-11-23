#![cfg(feature = "cache_me")]

use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    sync::mpsc,
    thread,
    time::Duration,
};

mod support;

use serde_json::json;
use support::read_http_request;
use teloxide_core::{adaptors::CacheMe, prelude::*, Bot};

fn read_request(client: &mut TcpStream) -> String {
    String::from_utf8(read_http_request(client)).unwrap().to_ascii_lowercase()
}

fn write_response(client: &mut TcpStream, result: serde_json::Value) {
    let body = json!({"ok": true, "result": result}).to_string();
    write!(
        client,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: \
         close\r\n\r\n{}",
        body.len(),
        body
    )
    .unwrap();
}

fn self_info(first_name: &str) -> serde_json::Value {
    json!({
        "id": 123,
        "is_bot": true,
        "first_name": first_name,
        "can_join_groups": false,
        "can_read_all_group_messages": false,
        "supports_inline_queries": false,
        "needs_inline_query_location": false,
        "can_connect_to_business": false,
        "supports_guest_queries": false,
        "supports_join_request_queries": false,
        "has_topics_enabled": false,
        "allows_users_to_create_topics": false,
        "has_topic_sidebar": false,
        "can_manage_bots": false,
        "has_main_web_app": false
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn old_in_flight_get_me_cannot_repopulate_invalidated_cache() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url: reqwest::Url = format!("http://{}", listener.local_addr().unwrap()).parse().unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let server = thread::spawn(move || {
        let (mut old_get_me, _) = listener.accept().unwrap();
        assert!(read_request(&mut old_get_me).contains("/getme "));
        started_tx.send(()).unwrap();

        // A settings request completes while the initial getMe is still in
        // flight and must replace the shared cache generation.
        let (mut settings, _) = listener.accept().unwrap();
        assert!(read_request(&mut settings).contains("/setmysettings "));
        write_response(&mut settings, json!(true));

        release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        write_response(&mut old_get_me, self_info("before-change"));

        // The old response is allowed to finish for its original caller,
        // but a subsequent getMe must fetch the post-change generation.
        let (mut fresh_get_me, _) = listener.accept().unwrap();
        assert!(read_request(&mut fresh_get_me).contains("/getme "));
        write_response(&mut fresh_get_me, self_info("after-change"));
    });

    let cached = CacheMe::new(Bot::new("123:offline").set_api_url(url));
    let cloned = cached.clone();
    let old_request = tokio::spawn(async move { cloned.get_me().await.unwrap() });

    // This blocks a test worker, not the mocked HTTP server or the request
    // executor. The test runtime has two workers explicitly.
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    cached.set_my_settings().await.unwrap();
    release_tx.send(()).unwrap();

    assert_eq!(old_request.await.unwrap().user.first_name, "before-change");
    assert_eq!(cached.get_me().await.unwrap().user.first_name, "after-change");
    server.join().unwrap();
}
