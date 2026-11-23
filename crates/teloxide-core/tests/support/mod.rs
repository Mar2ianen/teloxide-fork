//! Shared blocking HTTP request reader for offline Bot API tests.
//!
//! A TCP read returns an arbitrary fragment, not a complete HTTP request.
//! Wait for the header terminator and all Content-Length body bytes before
//! responding, so tests cannot succeed against an incomplete request.

use std::{io::Read, net::TcpStream, time::Duration};

const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;

pub fn read_http_request(stream: &mut TcpStream) -> Vec<u8> {
    stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();

    let mut request = Vec::new();
    let mut chunk = [0_u8; 4096];
    let header_end = loop {
        if let Some(pos) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            break pos + 4;
        }
        assert!(request.len() < MAX_HEADER_BYTES, "HTTP test request headers are too large");
        let size = stream.read(&mut chunk).expect("reading HTTP test request headers");
        assert!(size > 0, "HTTP test peer closed before request headers");
        request.extend_from_slice(&chunk[..size]);
    };

    let headers =
        std::str::from_utf8(&request[..header_end]).expect("HTTP request headers are UTF-8");
    let mut content_length = None;
    for line in headers.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            assert!(content_length.is_none(), "duplicate Content-Length in test request");
            content_length = Some(value.trim().parse::<usize>().expect("numeric Content-Length"));
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            // The test requests are JSON with a known length. Reject other
            // framing explicitly rather than accidentally reading a prefix.
            assert!(
                value.trim().eq_ignore_ascii_case("identity"),
                "unsupported HTTP transfer encoding in mock test: {value}"
            );
        }
    }

    let total =
        header_end.checked_add(content_length.unwrap_or(0)).expect("request length overflow");
    assert!(total <= MAX_REQUEST_BYTES, "HTTP mock request exceeded size limit");
    while request.len() < total {
        let size = stream.read(&mut chunk).expect("reading HTTP test request body");
        assert!(size > 0, "HTTP test peer closed before Content-Length was satisfied");
        request.extend_from_slice(&chunk[..size]);
    }
    request.truncate(total);
    request
}

#[cfg(test)]
mod tests {
    use std::{io::Write, net::TcpListener, sync::mpsc, thread};

    use super::*;

    #[test]
    fn waits_for_fragmented_headers_and_body() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        let (tx, rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            tx.send(read_http_request(&mut server)).unwrap();
        });

        client.write_all(b"POST /test HTTP/1.1\r\nContent-Len").unwrap();
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());

        client.write_all(b"gth: 8\r\nHost: localhost\r\n\r\nabc").unwrap();
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());

        client.write_all(b"defgh").unwrap();
        let request = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(&request[request.len() - 8..], b"abcdefgh");
        worker.join().unwrap();
    }
}
