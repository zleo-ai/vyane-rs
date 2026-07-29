//! Policy broker for explicitly authorized command networking.
//!
//! The untrusted command remains in a Bubblewrap network namespace. A trusted
//! loopback proxy talks to this broker over inherited descriptor 5; only the
//! broker resolves names and opens external TCP connections.

#[cfg(any(target_os = "linux", test))]
use std::collections::HashSet;
use std::net::IpAddr;
#[cfg(any(target_os = "linux", test))]
use std::net::{Ipv4Addr, Ipv6Addr};
#[cfg(any(target_os = "linux", test))]
use std::time::Duration;

use serde::{Deserialize, Serialize};
#[cfg(target_os = "linux")]
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
#[cfg(target_os = "linux")]
use tokio::net::{TcpStream, UnixStream, lookup_host};
#[cfg(target_os = "linux")]
use vyane_core::{NativeExecutionAuthority, NativeSideEffect, Result as VyaneResult};

const DEFAULT_MAX_CONNECTIONS: u32 = 8;
const DEFAULT_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_CONNECT_TIMEOUT_SECONDS: u64 = 10;
const MAX_NETWORK_RULES: usize = 128;
const MAX_PORTS_PER_RULE: usize = 32;
const MAX_CONNECTIONS: u32 = 64;
const MAX_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_CONNECT_TIMEOUT_SECONDS: u64 = 60;
const MAX_HOST_BYTES: usize = 253;
#[cfg(target_os = "linux")]
const MAX_FRAME_BYTES: usize = 64 * 1024;
#[cfg(any(target_os = "linux", test))]
const MAX_TLS_CLIENT_HELLO_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCommandNetworkRule {
    pub host: String,
    pub ports: Vec<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCommandNetworkPolicy {
    pub allow: Vec<NativeCommandNetworkRule>,
    #[serde(default)]
    pub route: NativeCommandNetworkRoute,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_max_bytes")]
    pub max_bytes: u64,
    #[serde(default = "default_connect_timeout_seconds")]
    pub connect_timeout_seconds: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeCommandNetworkRoute {
    #[default]
    Direct,
    EnvironmentProxy,
}

impl NativeCommandNetworkPolicy {
    pub fn validate(&self) -> Result<(), NativeCommandNetworkPolicyError> {
        if self.allow.is_empty() {
            return Err(NativeCommandNetworkPolicyError::EmptyAllowlist);
        }
        if self.allow.len() > MAX_NETWORK_RULES {
            return Err(NativeCommandNetworkPolicyError::TooManyRules);
        }
        if self.max_connections == 0 || self.max_connections > MAX_CONNECTIONS {
            return Err(NativeCommandNetworkPolicyError::InvalidConnectionLimit);
        }
        if self.max_bytes == 0 || self.max_bytes > MAX_BYTES {
            return Err(NativeCommandNetworkPolicyError::InvalidByteLimit);
        }
        if self.connect_timeout_seconds == 0
            || self.connect_timeout_seconds > MAX_CONNECT_TIMEOUT_SECONDS
        {
            return Err(NativeCommandNetworkPolicyError::InvalidConnectTimeout);
        }
        for rule in &self.allow {
            validate_host_pattern(&rule.host)?;
            if rule.ports.is_empty()
                || rule.ports.len() > MAX_PORTS_PER_RULE
                || rule.ports.contains(&0)
            {
                return Err(NativeCommandNetworkPolicyError::InvalidPorts);
            }
        }
        Ok(())
    }

    #[cfg(any(target_os = "linux", test))]
    fn permits(&self, host: &str, port: u16) -> bool {
        self.allow.iter().any(|rule| {
            rule.ports.contains(&port)
                && match rule.host.strip_prefix("*.") {
                    Some(suffix) => {
                        host.len() > suffix.len()
                            && host.ends_with(suffix)
                            && host.as_bytes()[host.len() - suffix.len() - 1] == b'.'
                    }
                    None => rule.host == host,
                }
        })
    }
}

const fn default_max_connections() -> u32 {
    DEFAULT_MAX_CONNECTIONS
}

const fn default_max_bytes() -> u64 {
    DEFAULT_MAX_BYTES
}

const fn default_connect_timeout_seconds() -> u64 {
    DEFAULT_CONNECT_TIMEOUT_SECONDS
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NativeCommandNetworkPolicyError {
    #[error("native command network allowlist must not be empty")]
    EmptyAllowlist,
    #[error("native command network policy contains too many rules")]
    TooManyRules,
    #[error("native command network policy contains an invalid host pattern")]
    InvalidHost,
    #[error("native command network policy contains invalid ports")]
    InvalidPorts,
    #[error("native command network connection limit is invalid")]
    InvalidConnectionLimit,
    #[error("native command network byte limit is invalid")]
    InvalidByteLimit,
    #[error("native command network connect timeout is invalid")]
    InvalidConnectTimeout,
}

#[cfg(target_os = "linux")]
pub(crate) fn validate_network_route_host(policy: &NativeCommandNetworkPolicy) -> Result<(), ()> {
    match policy.route {
        NativeCommandNetworkRoute::Direct => Ok(()),
        NativeCommandNetworkRoute::EnvironmentProxy => {
            environment_proxy().map(|_| ()).map_err(|_| ())
        }
    }
}

#[cfg(target_os = "linux")]
pub(crate) async fn run_network_broker(
    channel: std::os::fd::OwnedFd,
    policy: &NativeCommandNetworkPolicy,
    authority: &dyn NativeExecutionAuthority,
    effect: NativeSideEffect,
) -> VyaneResult<()> {
    let std_channel = std::os::unix::net::UnixStream::from(channel);
    std_channel
        .set_nonblocking(true)
        .map_err(vyane_core::VyaneError::from)?;
    let mut channel = UnixStream::from_std(std_channel).map_err(vyane_core::VyaneError::from)?;
    let mut connections = 0u32;
    let mut transferred = 0u64;

    'requests: loop {
        let kind = match channel.read_u8().await {
            Ok(kind) => kind,
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        if kind != 1 {
            return Err(vyane_core::VyaneError::new(
                vyane_core::ErrorKind::Config,
                "native network proxy sent an invalid request",
            ));
        }
        let host_len = channel.read_u16().await? as usize;
        let port = channel.read_u16().await?;
        if host_len == 0 || host_len > MAX_HOST_BYTES {
            channel.write_u8(0).await?;
            continue;
        }
        let mut host = vec![0u8; host_len];
        channel.read_exact(&mut host).await?;
        let Ok(host) = String::from_utf8(host) else {
            channel.write_u8(0).await?;
            continue;
        };
        let host = host.to_ascii_lowercase();
        connections = connections.saturating_add(1);
        if connections > policy.max_connections
            || host.starts_with("*.")
            || validate_host_pattern(&host).is_err()
            || !policy.permits(&host, port)
        {
            channel.write_u8(0).await?;
            continue;
        }

        authority.revalidate(effect).await?;
        let stream = connect_destination(
            &host,
            port,
            policy.connect_timeout_seconds,
            policy.route,
            authority,
            effect,
        )
        .await?;
        let Ok(mut stream) = stream else {
            channel.write_u8(0).await?;
            continue;
        };
        channel.write_u8(1).await?;
        let Some(prelude) =
            read_https_prelude(&mut channel, &host, &mut transferred, policy.max_bytes).await?
        else {
            write_frame(&mut channel, &[]).await?;
            drain_rejected_tunnel(&mut channel, &mut transferred, policy.max_bytes).await?;
            continue 'requests;
        };
        stream.write_all(&prelude).await?;
        let mut remote_eof = false;
        let mut client_eof = false;
        let mut remote_buf = vec![0u8; MAX_FRAME_BYTES];
        while !remote_eof || !client_eof {
            tokio::select! {
                frame = read_frame(&mut channel), if !client_eof => {
                    let frame = match frame {
                        Ok(frame) => frame,
                        Err(error) if proxy_closed(&error) => return Ok(()),
                        Err(error) => return Err(error.into()),
                    };
                    match frame {
                        Some(bytes) => {
                            transferred = transferred.saturating_add(bytes.len() as u64);
                            if transferred > policy.max_bytes {
                                return Ok(());
                            }
                            stream.write_all(&bytes).await?;
                        }
                        None => {
                            client_eof = true;
                            stream.shutdown().await?;
                        }
                    }
                }
                read = stream.read(&mut remote_buf), if !remote_eof => {
                    let count = read?;
                    if count == 0 {
                        remote_eof = true;
                        if let Err(error) = write_frame(&mut channel, &[]).await {
                            if proxy_closed(&error) {
                                return Ok(());
                            }
                            return Err(error.into());
                        }
                    } else {
                        transferred = transferred.saturating_add(count as u64);
                        if transferred > policy.max_bytes {
                            return Ok(());
                        }
                        if let Err(error) = write_frame(&mut channel, &remote_buf[..count]).await {
                            if proxy_closed(&error) {
                                return Ok(());
                            }
                            return Err(error.into());
                        }
                    }
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
async fn connect_destination(
    host: &str,
    port: u16,
    timeout_seconds: u64,
    route: NativeCommandNetworkRoute,
    authority: &dyn NativeExecutionAuthority,
    effect: NativeSideEffect,
) -> VyaneResult<std::io::Result<TcpStream>> {
    match route {
        NativeCommandNetworkRoute::Direct => {
            connect_public(host, port, timeout_seconds, authority, effect).await
        }
        NativeCommandNetworkRoute::EnvironmentProxy => {
            connect_via_environment_proxy(host, port, timeout_seconds, authority, effect).await
        }
    }
}

#[cfg(target_os = "linux")]
async fn connect_public(
    host: &str,
    port: u16,
    timeout_seconds: u64,
    authority: &dyn NativeExecutionAuthority,
    effect: NativeSideEffect,
) -> VyaneResult<std::io::Result<TcpStream>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_seconds);
    authority.revalidate(effect).await?;
    let mut addresses = match tokio::time::timeout_at(deadline, lookup_host((host, port))).await {
        Ok(Ok(addresses)) => addresses.collect::<Vec<_>>(),
        Ok(Err(error)) => return Ok(Err(error)),
        Err(_) => return Ok(Err(connect_timeout_error())),
    };
    deduplicate_addresses(&mut addresses);
    if addresses.is_empty()
        || addresses.len() > 32
        || addresses.iter().any(|address| !public_ip(address.ip()))
    {
        return Ok(Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "native network destination resolved outside the public Internet",
        )));
    }
    let mut last_error = None;
    let address_count = addresses.len();
    for (index, address) in addresses.into_iter().enumerate() {
        authority.revalidate(effect).await?;
        let attempt_deadline = shared_attempt_deadline(deadline, address_count - index);
        match tokio::time::timeout_at(attempt_deadline, TcpStream::connect(address)).await {
            Ok(Ok(stream)) => return Ok(Ok(stream)),
            Ok(Err(error)) => last_error = Some(error),
            Err(_) => last_error = Some(connect_timeout_error()),
        }
    }
    Ok(Err(last_error.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "no native network destination",
        )
    })))
}

#[cfg(target_os = "linux")]
async fn connect_via_environment_proxy(
    host: &str,
    port: u16,
    timeout_seconds: u64,
    authority: &dyn NativeExecutionAuthority,
    effect: NativeSideEffect,
) -> VyaneResult<std::io::Result<TcpStream>> {
    let proxy = match environment_proxy() {
        Ok(proxy) => proxy,
        Err(error) => return Ok(Err(error)),
    };
    let Some(proxy_host) = proxy.host_str() else {
        return Ok(Err(invalid_environment_proxy()));
    };
    let Some(proxy_port) = proxy.port_or_known_default() else {
        return Ok(Err(invalid_environment_proxy()));
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_seconds);
    authority.revalidate(effect).await?;
    let mut addresses =
        match tokio::time::timeout_at(deadline, lookup_host((proxy_host, proxy_port))).await {
            Ok(Ok(addresses)) => addresses.collect::<Vec<_>>(),
            Ok(Err(error)) => return Ok(Err(error)),
            Err(_) => return Ok(Err(connect_timeout_error())),
        };
    deduplicate_addresses(&mut addresses);
    if addresses.is_empty() || addresses.len() > 32 {
        return Ok(Err(invalid_environment_proxy()));
    }
    let mut stream = None;
    let address_count = addresses.len();
    for (index, address) in addresses.into_iter().enumerate() {
        authority.revalidate(effect).await?;
        let attempt_deadline = shared_attempt_deadline(deadline, address_count - index);
        match tokio::time::timeout_at(attempt_deadline, TcpStream::connect(address)).await {
            Ok(Ok(connected)) => {
                stream = Some(connected);
                break;
            }
            Ok(Err(_)) => {}
            Err(_) => break,
        }
    }
    let Some(mut stream) = stream else {
        return Ok(Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "native network upstream proxy is unavailable",
        )));
    };
    authority.revalidate(effect).await?;
    if let Err(error) = stream
        .write_all(
            format!(
                "CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\nProxy-Connection: Keep-Alive\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
    {
        return Ok(Err(error));
    }
    let mut response = Vec::new();
    let mut buffer = [0u8; 512];
    while response.windows(4).all(|window| window != b"\r\n\r\n") {
        let count = match tokio::time::timeout_at(deadline, stream.read(&mut buffer)).await {
            Ok(Ok(count)) => count,
            Ok(Err(error)) => return Ok(Err(error)),
            Err(_) => {
                return Ok(Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "native network upstream proxy timed out",
                )));
            }
        };
        if count == 0 || response.len().saturating_add(count) > 8192 {
            return Ok(Err(invalid_environment_proxy()));
        }
        response.extend_from_slice(&buffer[..count]);
    }
    let Some(header_end) = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
    else {
        return Ok(Err(invalid_environment_proxy()));
    };
    if header_end != response.len() || !proxy_connect_succeeded(&response) {
        return Ok(Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "native network upstream proxy rejected the destination",
        )));
    }
    Ok(Ok(stream))
}

#[cfg(target_os = "linux")]
fn connect_timeout_error() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "native network connection timed out",
    )
}

#[cfg(any(target_os = "linux", test))]
fn deduplicate_addresses(addresses: &mut Vec<std::net::SocketAddr>) {
    let mut seen = HashSet::new();
    addresses.retain(|address| seen.insert(*address));
}

#[cfg(any(target_os = "linux", test))]
fn shared_attempt_deadline(
    overall: tokio::time::Instant,
    attempts_remaining: usize,
) -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    let remaining = overall.saturating_duration_since(now);
    now + remaining / u32::try_from(attempts_remaining).unwrap_or(u32::MAX).max(1)
}

#[cfg(any(target_os = "linux", test))]
fn proxy_connect_succeeded(response: &[u8]) -> bool {
    let Some(status) = response.split(|byte| *byte == b'\n').next() else {
        return false;
    };
    let mut parts = status.split(|byte| *byte == b' ');
    let version = parts.next();
    let code = parts.next();
    matches!(version, Some(b"HTTP/1.1" | b"HTTP/1.0"))
        && code.is_some_and(|code| {
            code.len() == 3
                && code[0] == b'2'
                && code[1].is_ascii_digit()
                && code[2].is_ascii_digit()
        })
}

#[cfg(target_os = "linux")]
fn environment_proxy() -> std::io::Result<url::Url> {
    let value = std::env::var("HTTPS_PROXY")
        .or_else(|_| std::env::var("https_proxy"))
        .map_err(|_| invalid_environment_proxy())?;
    let proxy = url::Url::parse(&value).map_err(|_| invalid_environment_proxy())?;
    if proxy.scheme() != "http"
        || proxy.host_str().is_none()
        || proxy.username() != ""
        || proxy.password().is_some()
        || !matches!(proxy.path(), "" | "/")
        || proxy.query().is_some()
        || proxy.fragment().is_some()
    {
        return Err(invalid_environment_proxy());
    }
    Ok(proxy)
}

#[cfg(target_os = "linux")]
fn invalid_environment_proxy() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "native network environment proxy is unsupported",
    )
}

#[cfg(target_os = "linux")]
async fn read_frame(channel: &mut UnixStream) -> std::io::Result<Option<Vec<u8>>> {
    let length = channel.read_u32().await? as usize;
    if length == 0 {
        return Ok(None);
    }
    if length > MAX_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "native network proxy frame is too large",
        ));
    }
    let mut bytes = vec![0u8; length];
    channel.read_exact(&mut bytes).await?;
    Ok(Some(bytes))
}

#[cfg(target_os = "linux")]
async fn write_frame(channel: &mut UnixStream, bytes: &[u8]) -> std::io::Result<()> {
    channel.write_u32(bytes.len() as u32).await?;
    channel.write_all(bytes).await
}

#[cfg(target_os = "linux")]
async fn drain_rejected_tunnel(
    channel: &mut UnixStream,
    transferred: &mut u64,
    max_bytes: u64,
) -> std::io::Result<()> {
    while let Some(frame) = read_frame(channel).await? {
        *transferred = transferred.saturating_add(frame.len() as u64);
        if *transferred > max_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "native network byte limit exceeded",
            ));
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
async fn read_https_prelude(
    channel: &mut UnixStream,
    host: &str,
    transferred: &mut u64,
    max_bytes: u64,
) -> std::io::Result<Option<Vec<u8>>> {
    let mut bytes = Vec::new();
    loop {
        let Some(frame) = read_frame(channel).await? else {
            return Ok(None);
        };
        *transferred = transferred.saturating_add(frame.len() as u64);
        if *transferred > max_bytes
            || bytes.len().saturating_add(frame.len()) > MAX_TLS_CLIENT_HELLO_BYTES
        {
            return Ok(None);
        }
        bytes.extend_from_slice(&frame);
        match tls_records_match_sni(&bytes, host) {
            TlsPreludeState::Incomplete => {}
            TlsPreludeState::Invalid => return Ok(None),
            TlsPreludeState::Valid => return Ok(Some(bytes)),
        }
    }
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TlsPreludeState {
    Incomplete,
    Invalid,
    Valid,
}

#[cfg(any(target_os = "linux", test))]
fn tls_records_match_sni(bytes: &[u8], host: &str) -> TlsPreludeState {
    let mut record_offset = 0usize;
    let mut handshake = Vec::new();
    loop {
        let Some(header_end) = record_offset.checked_add(5) else {
            return TlsPreludeState::Invalid;
        };
        if bytes.len() < header_end {
            return TlsPreludeState::Incomplete;
        }
        if bytes[record_offset] != 22 {
            return TlsPreludeState::Invalid;
        }
        let record_len =
            u16::from_be_bytes([bytes[record_offset + 3], bytes[record_offset + 4]]) as usize;
        let Some(record_end) = header_end.checked_add(record_len) else {
            return TlsPreludeState::Invalid;
        };
        if record_end > MAX_TLS_CLIENT_HELLO_BYTES {
            return TlsPreludeState::Invalid;
        }
        if bytes.len() < record_end {
            return TlsPreludeState::Incomplete;
        }
        handshake.extend_from_slice(&bytes[header_end..record_end]);
        if handshake.len() >= 4 {
            if handshake[0] != 1 {
                return TlsPreludeState::Invalid;
            }
            let handshake_len = ((handshake[1] as usize) << 16)
                | ((handshake[2] as usize) << 8)
                | handshake[3] as usize;
            let Some(handshake_end) = 4usize.checked_add(handshake_len) else {
                return TlsPreludeState::Invalid;
            };
            if handshake_end > MAX_TLS_CLIENT_HELLO_BYTES {
                return TlsPreludeState::Invalid;
            }
            if handshake.len() >= handshake_end {
                return if tls_client_hello_sni(&handshake[..handshake_end])
                    .is_some_and(|sni| sni.eq_ignore_ascii_case(host))
                {
                    TlsPreludeState::Valid
                } else {
                    TlsPreludeState::Invalid
                };
            }
        }
        record_offset = record_end;
        if record_offset == bytes.len() {
            return TlsPreludeState::Incomplete;
        }
    }
}

#[cfg(any(target_os = "linux", test))]
fn tls_client_hello_sni(bytes: &[u8]) -> Option<&str> {
    if bytes.len() < 4 || bytes[0] != 1 {
        return None;
    }
    let handshake_len =
        ((bytes[1] as usize) << 16) | ((bytes[2] as usize) << 8) | bytes[3] as usize;
    if 4usize.checked_add(handshake_len)? > bytes.len() {
        return None;
    }
    let mut offset = 4usize.checked_add(2 + 32)?;
    let session_len = *bytes.get(offset)? as usize;
    offset = offset.checked_add(1 + session_len)?;
    let cipher_len = u16::from_be_bytes([*bytes.get(offset)?, *bytes.get(offset + 1)?]) as usize;
    offset = offset.checked_add(2 + cipher_len)?;
    let compression_len = *bytes.get(offset)? as usize;
    offset = offset.checked_add(1 + compression_len)?;
    let extensions_len =
        u16::from_be_bytes([*bytes.get(offset)?, *bytes.get(offset + 1)?]) as usize;
    offset = offset.checked_add(2)?;
    let extensions_end = offset.checked_add(extensions_len)?;
    if extensions_end > 4 + handshake_len {
        return None;
    }
    while offset < extensions_end {
        let extension_type = u16::from_be_bytes([*bytes.get(offset)?, *bytes.get(offset + 1)?]);
        let extension_len =
            u16::from_be_bytes([*bytes.get(offset + 2)?, *bytes.get(offset + 3)?]) as usize;
        offset = offset.checked_add(4)?;
        let extension_end = offset.checked_add(extension_len)?;
        if extension_end > extensions_end {
            return None;
        }
        if extension_type == 0 {
            let list_len =
                u16::from_be_bytes([*bytes.get(offset)?, *bytes.get(offset + 1)?]) as usize;
            let mut name = offset.checked_add(2)?;
            let list_end = name.checked_add(list_len)?;
            if list_end != extension_end {
                return None;
            }
            while name < list_end {
                let name_type = *bytes.get(name)?;
                let name_len =
                    u16::from_be_bytes([*bytes.get(name + 1)?, *bytes.get(name + 2)?]) as usize;
                name = name.checked_add(3)?;
                let name_end = name.checked_add(name_len)?;
                if name_end > list_end {
                    return None;
                }
                if name_type == 0 {
                    return std::str::from_utf8(bytes.get(name..name_end)?).ok();
                }
                name = name_end;
            }
            return None;
        }
        offset = extension_end;
    }
    None
}

#[cfg(target_os = "linux")]
fn proxy_closed(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionReset
    )
}

fn validate_host_pattern(host: &str) -> Result<(), NativeCommandNetworkPolicyError> {
    let plain = host.strip_prefix("*.").unwrap_or(host);
    if plain.is_empty()
        || plain.len() > MAX_HOST_BYTES
        || plain.parse::<IpAddr>().is_ok()
        || plain.ends_with('.')
        || plain.split('.').count() < 2
        || plain.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
    {
        return Err(NativeCommandNetworkPolicyError::InvalidHost);
    }
    Ok(())
}

#[cfg(any(target_os = "linux", test))]
fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => public_v4(ip),
        IpAddr::V6(ip) => public_v6(ip),
    }
}

#[cfg(any(target_os = "linux", test))]
fn public_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, d] = ip.octets();
    !(a == 0
        || a == 10
        || a == 127
        || (a == 100 && (64..=127).contains(&b))
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && b == 0 && c == 0)
        || (a == 192 && b == 0 && c == 2)
        || (a == 192 && b == 88 && c == 99)
        || (a == 192 && b == 168)
        || (a == 198 && (b == 18 || b == 19))
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113)
        || a >= 224
        || [a, b, c, d] == [255, 255, 255, 255])
}

#[cfg(any(target_os = "linux", test))]
fn public_v6(ip: Ipv6Addr) -> bool {
    if let Some(mapped) = ip.to_ipv4_mapped() {
        return public_v4(mapped);
    }
    let segments = ip.segments();
    if segments[..6].iter().all(|segment| *segment == 0)
        || (segments[0] == 0x0064
            && segments[1] == 0xff9b
            && segments[2..6].iter().all(|segment| *segment == 0))
    {
        let octets = ip.octets();
        return public_v4(Ipv4Addr::new(
            octets[12], octets[13], octets[14], octets[15],
        ));
    }
    let local_translation_prefix =
        segments[0] == 0x0064 && segments[1] == 0xff9b && segments[2] == 1;
    let allocated_global_unicast = segments[0] & 0xe000 == 0x2000;
    let special_2001 = segments[0] == 0x2001
        && (segments[1] == 0
            || segments[1] == 2
            || (0x0010..=0x001f).contains(&segments[1])
            || (0x0020..=0x002f).contains(&segments[1]));
    let six_to_four = segments[0] == 0x2002;
    let documentation_3fff = segments[0] == 0x3fff && segments[1] & 0xf000 == 0;
    allocated_global_unicast
        && !(ip.is_unspecified()
            || ip.is_loopback()
            || ip.is_multicast()
            || segments[0] & 0xfe00 == 0xfc00
            || segments[0] & 0xffc0 == 0xfe80
            || segments[0] & 0xffc0 == 0xfec0
            || (segments[0] == 0x2001 && segments[1] == 0x0db8)
            || local_translation_prefix
            || special_2001
            || six_to_four
            || documentation_3fff)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(host: &str) -> NativeCommandNetworkPolicy {
        NativeCommandNetworkPolicy {
            allow: vec![NativeCommandNetworkRule {
                host: host.into(),
                ports: vec![443],
            }],
            route: NativeCommandNetworkRoute::Direct,
            max_connections: 2,
            max_bytes: 1024,
            connect_timeout_seconds: 1,
        }
    }

    fn client_hello(host: &str) -> Vec<u8> {
        let host = host.as_bytes();
        let list_len = 3 + host.len();
        let extension_len = 2 + list_len;
        let extensions_len = 4 + extension_len;
        let mut body = vec![3, 3];
        body.extend([0; 32]);
        body.push(0);
        body.extend_from_slice(&2u16.to_be_bytes());
        body.extend_from_slice(&[0x13, 0x01]);
        body.extend_from_slice(&[1, 0]);
        body.extend_from_slice(&(extensions_len as u16).to_be_bytes());
        body.extend_from_slice(&0u16.to_be_bytes());
        body.extend_from_slice(&(extension_len as u16).to_be_bytes());
        body.extend_from_slice(&(list_len as u16).to_be_bytes());
        body.push(0);
        body.extend_from_slice(&(host.len() as u16).to_be_bytes());
        body.extend_from_slice(host);
        let mut handshake = vec![
            1,
            ((body.len() >> 16) & 0xff) as u8,
            ((body.len() >> 8) & 0xff) as u8,
            (body.len() & 0xff) as u8,
        ];
        handshake.extend(body);
        let mut record = vec![22, 3, 1];
        record.extend_from_slice(&(handshake.len() as u16).to_be_bytes());
        record.extend(handshake);
        record
    }

    #[test]
    fn exact_and_wildcard_hosts_are_distinct() {
        assert!(policy("api.example.com").permits("api.example.com", 443));
        assert!(!policy("api.example.com").permits("other.example.com", 443));
        assert!(policy("*.example.com").permits("api.example.com", 443));
        assert!(!policy("*.example.com").permits("example.com", 443));
        assert!(!policy("*.example.com").permits("badexample.com", 443));
    }

    #[test]
    fn policy_rejects_ambiguous_or_overbroad_destinations() {
        for host in [
            "EXAMPLE.com",
            "127.0.0.1",
            "*.com",
            "example.com.",
            "-x.example",
        ] {
            assert_eq!(
                policy(host).validate(),
                Err(NativeCommandNetworkPolicyError::InvalidHost)
            );
        }
    }

    #[test]
    fn private_reserved_and_documentation_addresses_are_not_public() {
        for address in [
            "127.0.0.1",
            "10.0.0.1",
            "100.64.0.1",
            "169.254.1.1",
            "172.16.0.1",
            "192.168.0.1",
            "198.18.0.1",
            "203.0.113.1",
            "::1",
            "fc00::1",
            "fe80::1",
            "2001:db8::1",
            "::ffff:127.0.0.1",
            "::10.0.0.1",
            "64:ff9b::10.0.0.1",
            "64:ff9b:1::1",
            "100::1",
            "100:0:0:1::1",
            "2002::1",
            "3fff::1",
            "5f00::1",
        ] {
            assert!(
                !public_ip(address.parse().expect("test IP address")),
                "{address}"
            );
        }
        assert!(public_ip("1.1.1.1".parse().expect("public IPv4")));
        assert!(public_ip(
            "2606:4700:4700::1111".parse().expect("public IPv6")
        ));
    }

    #[test]
    fn tls_client_hello_exposes_the_exact_sni() {
        let hello = client_hello("crates.io");
        assert_eq!(tls_client_hello_sni(&hello[5..]), Some("crates.io"));
        assert_eq!(
            tls_records_match_sni(&hello, "crates.io"),
            TlsPreludeState::Valid
        );
        let mut malformed = hello;
        malformed[0] = 23;
        assert_eq!(
            tls_records_match_sni(&malformed, "crates.io"),
            TlsPreludeState::Invalid
        );
    }

    #[test]
    fn fragmented_tls_client_hello_is_assembled_before_sni_validation() {
        let hello = client_hello("crates.io");
        let handshake = &hello[5..];
        let split = handshake.len() / 2;
        let mut fragmented = vec![22, 3, 1];
        fragmented.extend_from_slice(&(split as u16).to_be_bytes());
        fragmented.extend_from_slice(&handshake[..split]);
        fragmented.extend_from_slice(&[22, 3, 1]);
        fragmented.extend_from_slice(&((handshake.len() - split) as u16).to_be_bytes());
        fragmented.extend_from_slice(&handshake[split..]);
        assert_eq!(
            tls_records_match_sni(&fragmented, "crates.io"),
            TlsPreludeState::Valid
        );
    }

    #[test]
    fn upstream_proxy_requires_a_real_two_hundred_status() {
        assert!(proxy_connect_succeeded(
            b"HTTP/1.1 200 Connection Established\r\n\r\n"
        ));
        assert!(!proxy_connect_succeeded(b"HTTP/1.1 403 Forbidden\r\n\r\n"));
        assert!(!proxy_connect_succeeded(b"HTTP/1.1 2000 Invalid\r\n\r\n"));
    }

    #[test]
    fn address_deduplication_preserves_resolver_order_and_attempts_share_time() {
        let first = "1.1.1.1:443".parse().expect("first address");
        let second = "8.8.8.8:443".parse().expect("second address");
        let mut addresses = vec![first, second, first];
        deduplicate_addresses(&mut addresses);
        assert_eq!(addresses, [first, second]);

        let overall = tokio::time::Instant::now() + Duration::from_secs(10);
        let first_attempt = shared_attempt_deadline(overall, 2);
        let final_attempt = shared_attempt_deadline(overall, 1);
        assert!(first_attempt < final_attempt);
        assert!(final_attempt <= overall);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn rejected_tunnel_drain_consumes_the_client_eof_frame() {
        let (broker, mut proxy) = UnixStream::pair().expect("proxy pair");
        let mut transferred = 0;
        write_frame(&mut proxy, b"discarded")
            .await
            .expect("client data");
        write_frame(&mut proxy, &[]).await.expect("client EOF");

        let mut broker = broker;
        drain_rejected_tunnel(&mut broker, &mut transferred, 1024)
            .await
            .expect("drain");
        assert_eq!(transferred, 9);
    }
}
