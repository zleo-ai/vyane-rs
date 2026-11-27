//! Same-machine IPC helpers for the pieces of Vyane that assume POSIX.
//!
//! Two things live here. Both Windows-only entry points stay callable on
//! POSIX and fail before they touch the filesystem.
//!
//! [`wait_readable`] is `select` on POSIX: one descriptor, read set only.
//! A regular file is readable. A pipe is readable once bytes are queued or
//! the write end is closed. A socket is readable the same way. The wait
//! never reads. On Windows, `select` accepts sockets only, so a socket still
//! uses Winsock `select`, a disk file returns immediately, and an anonymous
//! pipe polls `PeekNamedPipe`. That poll reports readable when bytes are
//! queued or the writer has closed, and it does not consume a byte. A
//! console or other character handle is rejected.
//!
//! Windows has no `AF_UNIX`. [`LoopbackDatagramListener`] and
//! [`send_loopback_datagram`] are the stand-in for a pathname
//! `AF_UNIX` `SOCK_DGRAM` socket: they bind UDP `127.0.0.1` on an ephemeral
//! port and rendezvous through a port file at the original path. The file
//! is `{magic} 127.0.0.1 {port} {token}\n` with a 128-bit token. It is
//! created with a DACL for the current user only. Windows may spell a
//! well-known SID as an SDDL alias when that descriptor is read back; the
//! sender still compares the full owner SID before it sends. Datagrams whose
//! token does not match are
//! dropped. On POSIX both entry points return [`LoopbackError::NotImplemented`].
//! Callers there keep using a pathname `AF_UNIX` socket. This module does
//! not invent one.
//!
//! Nothing here is called by the kernel, the ledger, the CLI, MCP, the
//! daemon, or conversation wake.
//!
//! # What CI runs
//!
//! Linux and macOS CI run POSIX `select` (pipes, regular files, sockets),
//! [`duplicate_socket`], the `NotImplemented` entry points, the port-file
//! grammar, and the pure decisions for "readable pipe", "always-readable
//! file", and "rejected character handle". They do not bind the loopback
//! socket, write a DACL, or call `PeekNamedPipe`.
//!
//! Those Win32 calls are `cfg(windows)`. Windows CI compiles and runs them.
//! A green Linux run does not certify a live ACL or a Winsock `select`.
//! Constants used by the decision tables are checked against `windows-sys`
//! when that target is built.

use std::fmt;
#[cfg(any(windows, test))]
use std::fs::File;
use std::io;
#[cfg(any(windows, test))]
use std::io::Read;
use std::path::Path;
use std::time::Duration;
#[cfg(unix)]
use std::time::Instant;

#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
#[cfg(windows)]
use std::os::windows::io::{
    AsHandle, AsSocket, BorrowedHandle, BorrowedSocket, OwnedHandle, OwnedSocket,
};

/// First field of a Windows loopback port file.
///
/// The file is one ASCII line: `{magic} 127.0.0.1 {port} {token}\n`.
pub const PORT_FILE_MAGIC: &str = "vyane-localipc-dgram/1";

#[cfg(not(windows))]
const LISTENER_UNSUPPORTED: &str = "LoopbackDatagramListener is the Windows stand-in for AF_UNIX; use a pathname AF_UNIX socket on POSIX";

#[cfg(not(windows))]
const SEND_UNSUPPORTED: &str = "send_loopback_datagram is the Windows stand-in for AF_UNIX; use a pathname AF_UNIX socket on POSIX";

/// `GetFileType` value for a disk file. Windows `wait_readable` returns
/// immediately for this type.
#[cfg(any(windows, test))]
const FILE_TYPE_DISK: u32 = 1;

/// `GetFileType` value for a character device (console, `NUL`).
#[cfg(any(windows, test))]
const FILE_TYPE_CHAR: u32 = 2;

/// `GetFileType` value for a pipe. Compared with `==`, so the remote bit
/// (`0x8000`) is not masked off.
#[cfg(any(windows, test))]
const FILE_TYPE_PIPE: u32 = 3;

/// Largest datagram the listener reads. A longer message is an operating-system
/// error from `recv` and that listener thread stops, matching `recvfrom` of this size.
#[cfg(windows)]
const MAX_DATAGRAM: usize = 2048;

/// Python's default `send_loopback_datagram(..., timeout=0.05)`.
#[cfg(windows)]
const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_millis(50);

/// Win32 `ERROR_BROKEN_PIPE`. `PeekNamedPipe` returns this when the write
/// end is closed; that is readable (a read returns EOF) and not an error.
#[cfg(any(windows, test))]
const ERROR_BROKEN_PIPE: i32 = 109;

/// Win32 `ERROR_ACCESS_DENIED`, stored on a foreign-owner [`LocalIpcError`].
#[cfg(any(windows, test))]
const FOREIGN_OWNER_CODE: i32 = 5;

#[cfg(any(windows, test))]
const TOKEN_BYTES: usize = 16;

#[cfg(any(windows, test))]
const TOKEN_HEX_LEN: usize = TOKEN_BYTES * 2;

#[cfg(any(windows, test))]
const PORT_FILE_MAX: usize = 512;

/// A local IPC rendezvous was refused.
///
/// This is the `LocalIpcError` / `OSError` subclass: a malformed or oversized
/// port file, a port file owned by someone else, or a temp file that could
/// not be created beside the port file. A missing port file is
/// [`io::ErrorKind::NotFound`], not this type. [`Self::raw_os_error`] is `0`
/// when the failure has no Win32 code, and `5` (`ERROR_ACCESS_DENIED`) when
/// the owner SID is not the current user.
///
/// [`From`]`<LocalIpcError> for io::Error` maps that code onto an
/// [`io::Error`]. [`LoopbackError::Refused`] keeps this value intact so
/// callers can match it without downcasting.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct LocalIpcError {
    message: String,
    raw_os_error: i32,
}

impl LocalIpcError {
    #[cfg(any(windows, test))]
    fn new(raw_os_error: i32, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            raw_os_error,
        }
    }

    /// `0` for a malformed port file, `5` when the owner is not the current user.
    #[must_use]
    pub fn raw_os_error(&self) -> i32 {
        self.raw_os_error
    }

    /// The refusal text, without an errno prefix.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl From<LocalIpcError> for io::Error {
    fn from(err: LocalIpcError) -> Self {
        // `5` is Win32 ERROR_ACCESS_DENIED. The Windows build asserts that
        // the windows-sys constant is the same value.
        let kind = if err.raw_os_error == 5 {
            io::ErrorKind::PermissionDenied
        } else {
            io::ErrorKind::Other
        };
        io::Error::new(kind, err)
    }
}

/// Failure from [`send_loopback_datagram`] or [`LoopbackDatagramListener`].
///
/// [`Self::NotImplemented`] is Python's `NotImplementedError` (POSIX).
/// [`Self::Refused`] is [`LocalIpcError`]. [`Self::Io`] is any other
/// `OSError`, including a missing port file.
#[derive(Debug, thiserror::Error)]
pub enum LoopbackError {
    /// POSIX stand-in. The text tells the caller to use a pathname `AF_UNIX` socket.
    #[error("{0}")]
    NotImplemented(&'static str),
    /// Port file refused. Nothing was sent.
    #[error(transparent)]
    Refused(LocalIpcError),
    /// Operating-system error. [`io::ErrorKind::NotFound`] is a missing port file.
    #[error(transparent)]
    Io(io::Error),
}

impl LoopbackError {
    /// `true` when this host does not implement the Windows loopback rendezvous.
    #[must_use]
    pub fn is_not_implemented(&self) -> bool {
        matches!(self, Self::NotImplemented(_))
    }

    /// `true` when the port file is missing.
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::Io(err) if err.kind() == io::ErrorKind::NotFound)
    }

    /// The [`LocalIpcError`] when the rendezvous was refused.
    #[must_use]
    pub fn refused(&self) -> Option<&LocalIpcError> {
        match self {
            Self::Refused(err) => Some(err),
            _ => None,
        }
    }
}

/// [`duplicate_socket`] could not return a duplicate.
///
/// [`Self::NotOpen`] and [`Self::NotASocket`] are Python `ValueError`s.
/// [`Self::Io`] is an `OSError` from the duplication itself. The caller's
/// descriptor is still open in every case.
#[derive(Debug, thiserror::Error)]
pub enum DuplicateSocketError {
    /// `fstat` failed.
    #[error("is not an open file descriptor")]
    NotOpen(#[source] io::Error),
    /// The descriptor is open and is not a socket.
    #[error("is not a socket")]
    NotASocket,
    /// The platform duplication call failed.
    #[error(transparent)]
    Io(io::Error),
}

/// Duplicate a socket without closing `fd`.
///
/// On POSIX this `fstat`s the descriptor, rejects anything that is not a
/// socket, and `dup`s it. On Windows the argument is a Winsock socket
/// (`AsSocket`); duplication is `WSADuplicateSocketW` plus `WSASocketW`,
/// which is what `socket.dup` uses because a Winsock handle is not a CRT
/// file descriptor. Dropping the returned socket closes only the duplicate.
///
/// # Errors
///
/// On POSIX, [`DuplicateSocketError::NotOpen`] when `fstat` fails and
/// [`DuplicateSocketError::NotASocket`] when the descriptor is not a socket.
/// Duplication failure is [`DuplicateSocketError::Io`] on both hosts.
#[cfg(unix)]
pub fn duplicate_socket(fd: impl AsFd) -> Result<OwnedFd, DuplicateSocketError> {
    let fd = fd.as_fd();
    let stat = match rustix::fs::fstat(fd) {
        Ok(stat) => stat,
        Err(err) => return Err(DuplicateSocketError::NotOpen(err.into())),
    };
    #[allow(clippy::unnecessary_cast, clippy::cast_possible_truncation)]
    let mode = stat.st_mode as rustix::fs::RawMode;
    if !rustix::fs::FileType::from_raw_mode(mode).is_socket() {
        return Err(DuplicateSocketError::NotASocket);
    }
    rustix::io::dup(fd).map_err(|err| DuplicateSocketError::Io(err.into()))
}

/// Duplicate a Winsock socket without closing `sock`.
///
/// See the POSIX [`duplicate_socket`] docs. Windows has no `fstat` check:
/// the type system already requires a socket.
#[cfg(windows)]
pub fn duplicate_socket(sock: impl AsSocket) -> Result<OwnedSocket, DuplicateSocketError> {
    win::duplicate_socket(sock.as_socket())
}

mod sealed {
    #[cfg(unix)]
    pub trait Sealed {}
    #[cfg(unix)]
    impl<T: std::os::fd::AsFd + ?Sized> Sealed for T {}

    #[cfg(windows)]
    pub trait Sealed {}
}

/// Something [`wait_readable`] can poll.
///
/// On POSIX this is implemented for every [`AsFd`] type (`File`, pipes,
/// `ChildStdout`, TCP and Unix sockets, owned and borrowed descriptors).
/// On Windows it is implemented for files, anonymous pipes, child stdio
/// pipes, handles, and for std sockets. Sockets are a separate implementation
/// so they use `select` rather than `PeekNamedPipe`.
pub trait AsWaitable: sealed::Sealed {
    /// Borrow the descriptor or socket to poll.
    #[doc(hidden)]
    fn as_waitable(&self) -> Waitable<'_>;
}

/// Borrowed source for one [`wait_readable`] call.
#[non_exhaustive]
pub enum Waitable<'a> {
    /// POSIX descriptor. Polled with `select`.
    #[cfg(unix)]
    Fd(BorrowedFd<'a>),
    /// Windows file or pipe handle.
    #[cfg(windows)]
    Handle(BorrowedHandle<'a>),
    /// Winsock socket. Polled with Winsock `select`.
    #[cfg(windows)]
    Socket(BorrowedSocket<'a>),
}

#[cfg(unix)]
impl<T: AsFd + ?Sized> AsWaitable for T {
    fn as_waitable(&self) -> Waitable<'_> {
        Waitable::Fd(self.as_fd())
    }
}

#[cfg(windows)]
impl sealed::Sealed for File {}
#[cfg(windows)]
impl AsWaitable for File {
    fn as_waitable(&self) -> Waitable<'_> {
        Waitable::Handle(self.as_handle())
    }
}

#[cfg(windows)]
impl sealed::Sealed for std::io::PipeReader {}
#[cfg(windows)]
impl AsWaitable for std::io::PipeReader {
    fn as_waitable(&self) -> Waitable<'_> {
        Waitable::Handle(self.as_handle())
    }
}

#[cfg(windows)]
impl sealed::Sealed for std::process::ChildStdout {}
#[cfg(windows)]
impl AsWaitable for std::process::ChildStdout {
    fn as_waitable(&self) -> Waitable<'_> {
        Waitable::Handle(self.as_handle())
    }
}

#[cfg(windows)]
impl sealed::Sealed for std::process::ChildStderr {}
#[cfg(windows)]
impl AsWaitable for std::process::ChildStderr {
    fn as_waitable(&self) -> Waitable<'_> {
        Waitable::Handle(self.as_handle())
    }
}

#[cfg(windows)]
impl sealed::Sealed for OwnedHandle {}
#[cfg(windows)]
impl AsWaitable for OwnedHandle {
    fn as_waitable(&self) -> Waitable<'_> {
        Waitable::Handle(self.as_handle())
    }
}

#[cfg(windows)]
impl sealed::Sealed for BorrowedHandle<'_> {}
#[cfg(windows)]
impl AsWaitable for BorrowedHandle<'_> {
    fn as_waitable(&self) -> Waitable<'_> {
        Waitable::Handle(self.as_handle())
    }
}

#[cfg(windows)]
impl sealed::Sealed for std::net::TcpStream {}
#[cfg(windows)]
impl AsWaitable for std::net::TcpStream {
    fn as_waitable(&self) -> Waitable<'_> {
        Waitable::Socket(self.as_socket())
    }
}

#[cfg(windows)]
impl sealed::Sealed for std::net::TcpListener {}
#[cfg(windows)]
impl AsWaitable for std::net::TcpListener {
    fn as_waitable(&self) -> Waitable<'_> {
        Waitable::Socket(self.as_socket())
    }
}

#[cfg(windows)]
impl sealed::Sealed for std::net::UdpSocket {}
#[cfg(windows)]
impl AsWaitable for std::net::UdpSocket {
    fn as_waitable(&self) -> Waitable<'_> {
        Waitable::Socket(self.as_socket())
    }
}

#[cfg(windows)]
impl sealed::Sealed for OwnedSocket {}
#[cfg(windows)]
impl AsWaitable for OwnedSocket {
    fn as_waitable(&self) -> Waitable<'_> {
        Waitable::Socket(self.as_socket())
    }
}

#[cfg(windows)]
impl sealed::Sealed for BorrowedSocket<'_> {}
#[cfg(windows)]
impl AsWaitable for BorrowedSocket<'_> {
    fn as_waitable(&self) -> Waitable<'_> {
        Waitable::Socket(self.as_socket())
    }
}

/// Wait until `source` is readable, the peer has closed, or `timeout` elapses.
///
/// `None` waits without a deadline. `Some(Duration::ZERO)` polls once.
/// The wait does not read: a later `read` still returns the queued bytes.
/// On EOF (the write end of a pipe is closed) this returns `Ok(true)` and a
/// following `read` returns `0`.
///
/// On POSIX this is `select` on the read set. `EINTR` retries with the time
/// left on the deadline. A descriptor at or above `FD_SETSIZE`, or a negative
/// one, is an error.
///
/// On Windows a socket uses Winsock `select`. A disk file returns `Ok(true)`
/// immediately. A pipe polls `PeekNamedPipe`, sleeping from 1ms up to 20ms
/// between peeks, same as the Windows pipe loop this replaces. A character
/// handle (console, `NUL`, any other non-pipe non-disk type) returns an
/// [`io::Error`] of kind [`io::ErrorKind::Unsupported`].
///
/// # Errors
///
/// An operating-system error from `select` or `PeekNamedPipe`, or
/// [`io::ErrorKind::Unsupported`] for a Windows handle that is neither a
/// socket, a pipe, nor a disk file.
///
/// # Examples
///
/// ```
/// use std::io::{Read, Write};
/// use std::time::Duration;
///
/// let (mut reader, mut writer) = std::io::pipe().unwrap();
/// assert!(!vyane_core::wait_readable(&reader, Some(Duration::from_millis(20))).unwrap());
/// writer.write_all(b"x").unwrap();
/// assert!(vyane_core::wait_readable(&reader, Some(Duration::from_secs(2))).unwrap());
/// let mut byte = [0u8; 1];
/// assert_eq!(reader.read(&mut byte).unwrap(), 1);
/// assert_eq!(byte, [b'x']);
/// drop(writer);
/// assert!(vyane_core::wait_readable(&reader, Some(Duration::from_secs(2))).unwrap());
/// assert_eq!(reader.read(&mut byte).unwrap(), 0);
/// ```
pub fn wait_readable(source: &impl AsWaitable, timeout: Option<Duration>) -> io::Result<bool> {
    match source.as_waitable() {
        #[cfg(unix)]
        Waitable::Fd(fd) => select_readable(fd, timeout),
        #[cfg(windows)]
        Waitable::Handle(handle) => win::wait_handle(handle, timeout),
        #[cfg(windows)]
        Waitable::Socket(socket) => win::wait_socket(socket, timeout),
    }
}

#[cfg(unix)]
#[allow(
    unsafe_code,
    reason = "POSIX wait_readable is select(2). libc exposes FD_SET and select as unsafe."
)]
fn select_readable(fd: BorrowedFd<'_>, timeout: Option<Duration>) -> io::Result<bool> {
    let raw = fd.as_raw_fd();
    if raw < 0 || (raw as usize) >= libc::FD_SETSIZE {
        let code = if raw < 0 { libc::EBADF } else { libc::EINVAL };
        return Err(io::Error::from_raw_os_error(code));
    }
    let deadline = timeout.map(|duration| Instant::now() + duration);
    loop {
        // SAFETY: `set` is a zeroed fd_set and `raw` is in range for FD_SET.
        // FD_ZERO and FD_SET only write that set.
        let mut set = unsafe { std::mem::zeroed::<libc::fd_set>() };
        unsafe {
            libc::FD_ZERO(&mut set);
            libc::FD_SET(raw, &mut set);
        }
        let mut stored = deadline
            .map(|deadline| timeval_from(deadline.saturating_duration_since(Instant::now())));
        let tv = stored.as_mut().map_or(std::ptr::null_mut(), |value| value);
        // SAFETY: `set` is initialized, `tv` is null or points at `stored`,
        // and `raw + 1` fits because `raw < FD_SETSIZE`.
        let rc = unsafe {
            libc::select(
                raw + 1,
                &mut set,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                tv,
            )
        };
        if rc < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    return Ok(false);
                }
                continue;
            }
            return Err(err);
        }
        return Ok(rc > 0);
    }
}

#[cfg(unix)]
fn timeval_from(duration: Duration) -> libc::timeval {
    let secs = duration.as_secs().min(i64::MAX as u64);
    #[allow(clippy::unnecessary_cast, clippy::cast_possible_truncation)]
    let tv_sec = secs as libc::time_t;
    #[allow(clippy::unnecessary_cast, clippy::cast_possible_truncation)]
    let tv_usec = duration.subsec_micros() as libc::suseconds_t;
    libc::timeval { tv_sec, tv_usec }
}

#[cfg(any(windows, test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandleClass {
    File,
    Pipe,
    Rejected(u32),
}

/// Classify a `GetFileType` result. Equality is exact: a pipe with the
/// remote bit set is rejected, matching `kind == FILE_TYPE_PIPE`.
#[cfg(any(windows, test))]
fn classify_file_type(kind: u32) -> HandleClass {
    if kind == FILE_TYPE_DISK {
        HandleClass::File
    } else if kind == FILE_TYPE_PIPE {
        HandleClass::Pipe
    } else {
        HandleClass::Rejected(kind)
    }
}

#[cfg(any(windows, test))]
fn unsupported_handle_message(kind: u32) -> String {
    format!(
        "wait_readable on Windows supports pipes, sockets and regular files only (file type {kind})"
    )
}

#[cfg(any(windows, test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Peek {
    Readable,
    Waiting,
    Failed(i32),
}

/// `PeekNamedPipe` outcome. A broken pipe is EOF and therefore readable.
/// `available > 0` is queued data. Zero means keep waiting. The call itself
/// does not read.
#[cfg(any(windows, test))]
fn pipe_peek_outcome(succeeded: bool, available: u32, error: i32) -> Peek {
    if !succeeded {
        if error == ERROR_BROKEN_PIPE {
            Peek::Readable
        } else {
            Peek::Failed(error)
        }
    } else if available > 0 {
        Peek::Readable
    } else {
        Peek::Waiting
    }
}

#[cfg(any(windows, test))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedPortFile {
    port: u16,
    token: [u8; TOKEN_HEX_LEN],
}

#[cfg(any(windows, test))]
enum PortFileFailure {
    TooLarge,
    Malformed,
}

#[cfg(any(windows, test))]
fn format_port_file(port: u16, token_hex: &str) -> Vec<u8> {
    format!("{PORT_FILE_MAGIC} 127.0.0.1 {port} {token_hex}\n").into_bytes()
}

#[cfg(windows)]
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

/// Parse a port-file body. `raw` is the bytes that were read, at most
/// [`PORT_FILE_MAX`] plus the one extra byte used to detect overflow.
#[cfg(any(windows, test))]
fn parse_port_bytes(raw: &[u8]) -> Result<ParsedPortFile, PortFileFailure> {
    if raw.len() > PORT_FILE_MAX {
        return Err(PortFileFailure::TooLarge);
    }
    let Ok(text) = std::str::from_utf8(raw) else {
        return Err(PortFileFailure::Malformed);
    };
    if !text.is_ascii() {
        return Err(PortFileFailure::Malformed);
    }
    let mut parts = text.split_whitespace();
    let magic = parts.next();
    let host = parts.next();
    let port_text = parts.next();
    let token = parts.next();
    if parts.next().is_some() {
        return Err(PortFileFailure::Malformed);
    }
    let (Some(magic), Some(host), Some(port_text), Some(token)) = (magic, host, port_text, token)
    else {
        return Err(PortFileFailure::Malformed);
    };
    if magic != PORT_FILE_MAGIC || host != "127.0.0.1" {
        return Err(PortFileFailure::Malformed);
    }
    let Ok(port) = port_text.parse::<i64>() else {
        return Err(PortFileFailure::Malformed);
    };
    if !(1..65536).contains(&port) {
        return Err(PortFileFailure::Malformed);
    }
    if token.len() != TOKEN_HEX_LEN
        || !token
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(PortFileFailure::Malformed);
    }
    let mut token_bytes = [0u8; TOKEN_HEX_LEN];
    token_bytes.copy_from_slice(token.as_bytes());
    let Ok(port) = u16::try_from(port) else {
        return Err(PortFileFailure::Malformed);
    };
    Ok(ParsedPortFile {
        port,
        token: token_bytes,
    })
}

#[cfg(any(windows, test))]
fn read_port_file(path: &Path) -> Result<ParsedPortFile, LoopbackError> {
    let mut file = File::open(path).map_err(LoopbackError::Io)?;
    let mut buf = [0u8; PORT_FILE_MAX + 1];
    let mut filled = 0usize;
    while filled < buf.len() {
        match file.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(count) => filled += count,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(LoopbackError::Io(err)),
        }
    }
    match parse_port_bytes(&buf[..filled]) {
        Ok(parsed) => Ok(parsed),
        Err(PortFileFailure::TooLarge) => Err(LoopbackError::Refused(LocalIpcError::new(
            0,
            format!("port file {} is too large", path.display()),
        ))),
        Err(PortFileFailure::Malformed) => Err(LoopbackError::Refused(LocalIpcError::new(
            0,
            format!("port file {} is malformed", path.display()),
        ))),
    }
}

#[cfg(any(windows, test))]
fn foreign_owner_message(path: &Path, owner: &str) -> String {
    format!(
        "port file {} is owned by {owner}, not the current user; refusing to send",
        path.display()
    )
}

/// Payload after a matching `token:` prefix, or `None` when the datagram
/// must be dropped. The token compare does not stop at the first mismatch.
#[cfg(any(windows, test))]
fn split_datagram<'a>(data: &'a [u8], token: &[u8; TOKEN_HEX_LEN]) -> Option<&'a [u8]> {
    if data.len() < TOKEN_HEX_LEN + 1 || data[TOKEN_HEX_LEN] != b':' {
        return None;
    }
    if !constant_time_eq(&data[..TOKEN_HEX_LEN], token) {
        return None;
    }
    Some(&data[TOKEN_HEX_LEN + 1..])
}

#[cfg(any(windows, test))]
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for (lhs, rhs) in left.iter().zip(right.iter()) {
        diff |= lhs ^ rhs;
    }
    diff == 0
}

/// Send `payload` to the listener whose port file is `path`.
///
/// `timeout` is the socket send timeout. `None` uses 50ms, the same default
/// as the Python helper. `Some(Duration::ZERO)` is a non-blocking send.
///
/// On POSIX this returns [`LoopbackError::NotImplemented`] and does not open
/// `path`. On Windows a missing file is [`LoopbackError::is_not_found`]. A
/// malformed file, a file larger than 512 bytes, or a file whose owner SID
/// is not the current user's SID is [`LoopbackError::Refused`]. Nothing is
/// sent in those cases. The datagram is the ASCII token, a colon, then
/// `payload`.
///
/// # Errors
///
/// [`LoopbackError::NotImplemented`] on POSIX.
/// [`LoopbackError::Io`] when the file is missing or the send fails.
/// [`LoopbackError::Refused`] when the port file is unusable or not owned
/// by the current user.
pub fn send_loopback_datagram(
    path: impl AsRef<Path>,
    payload: &[u8],
    timeout: Option<Duration>,
) -> Result<(), LoopbackError> {
    #[cfg(windows)]
    {
        win::send(path.as_ref(), payload, timeout)
    }
    #[cfg(not(windows))]
    {
        let _ = (path, payload, timeout);
        Err(LoopbackError::NotImplemented(SEND_UNSUPPORTED))
    }
}

/// Windows stand-in for a pathname `AF_UNIX` datagram listener.
///
/// On POSIX [`Self::new`] returns [`LoopbackError::NotImplemented`] and no
/// value exists. On Windows, [`Self::start`] binds `127.0.0.1` UDP, writes
/// the private port file at [`Self::path`], and delivers payloads on a
/// background thread. [`Self::close`] stops delivery and leaves the port
/// file in place; the next [`Self::start`] replaces it.
///
/// The callback runs on the listener thread. The byte slice is valid only
/// for that call. A panic inside the callback is caught and the listener
/// keeps running. Calling [`Self::close`] from the callback does not join
/// the listener thread (that would deadlock); the thread exits after the
/// callback returns.
#[must_use = "dropping the listener closes the socket; the port file is kept"]
pub struct LoopbackDatagramListener {
    #[cfg(windows)]
    inner: win::Listener,
    #[cfg(not(windows))]
    _never: std::convert::Infallible,
}

impl LoopbackDatagramListener {
    /// Store `path` and `on_datagram`.
    ///
    /// On POSIX this is the `NotImplementedError` entry point and does not
    /// create `path`. On Windows the socket is not bound until [`Self::start`].
    ///
    /// # Errors
    ///
    /// [`LoopbackError::NotImplemented`] on POSIX.
    pub fn new<F>(path: impl AsRef<Path>, on_datagram: F) -> Result<Self, LoopbackError>
    where
        F: FnMut(&[u8]) + Send + 'static,
    {
        #[cfg(windows)]
        {
            Ok(Self {
                inner: win::Listener::new(path.as_ref().to_path_buf(), on_datagram),
            })
        }
        #[cfg(not(windows))]
        {
            let _ = (path, on_datagram);
            Err(LoopbackError::NotImplemented(LISTENER_UNSUPPORTED))
        }
    }
}

#[cfg(windows)]
impl LoopbackDatagramListener {
    /// Bind, publish the port file, and start receiving.
    ///
    /// # Errors
    ///
    /// An operating-system error from the bind or the port file, or
    /// [`LocalIpcError`] when a private temp file could not be created.
    /// A second `start` without [`Self::close`] is refused.
    pub fn start(&mut self) -> Result<(), LoopbackError> {
        self.inner.start()
    }

    /// Bound UDP port, or `None` before [`Self::start`] and after [`Self::close`].
    #[must_use]
    pub fn port(&self) -> Option<u16> {
        self.inner.port()
    }

    /// Path of the port file.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    /// Stop delivery and keep the port file.
    ///
    /// Waits up to two seconds for the recv thread. A callback that is
    /// already running is allowed to finish. Later datagrams are not
    /// delivered.
    pub fn close(&mut self) {
        self.inner.close();
    }
}

#[cfg(windows)]
impl Drop for LoopbackDatagramListener {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(windows)]
impl fmt::Debug for LoopbackDatagramListener {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LoopbackDatagramListener")
            .field("path", &self.path())
            .field("port", &self.port())
            .finish_non_exhaustive()
    }
}

#[cfg(not(windows))]
impl LoopbackDatagramListener {
    fn dead(&self) -> ! {
        match self._never {}
    }

    /// Bind, publish the port file, and start receiving.
    ///
    /// # Errors
    ///
    /// Unreachable on POSIX: [`Self::new`] already returned
    /// [`LoopbackError::NotImplemented`].
    pub fn start(&mut self) -> Result<(), LoopbackError> {
        self.dead()
    }

    /// Bound UDP port. Unreachable on POSIX.
    #[must_use]
    pub fn port(&self) -> Option<u16> {
        self.dead()
    }

    /// Path of the port file. Unreachable on POSIX.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.dead()
    }

    /// Stop delivery and keep the port file. Unreachable on POSIX.
    pub fn close(&mut self) {
        self.dead()
    }
}

#[cfg(not(windows))]
impl fmt::Debug for LoopbackDatagramListener {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _ = self;
        formatter.write_str("LoopbackDatagramListener")
    }
}

#[cfg(not(any(unix, windows)))]
compile_error!("localipc supports unix and windows");

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "Win32 PeekNamedPipe, Winsock select, socket duplication, and security-descriptor calls have no safe wrapper in this workspace. Readable/EOF decisions are classify_file_type, pipe_peek_outcome, and split_datagram."
)]
mod win {
    use std::ffi::OsString;
    use std::fs::{self, File};
    use std::io::{self, Write};
    use std::mem::{self, ManuallyDrop};
    use std::net::{Ipv4Addr, UdpSocket};
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::{
        AsRawHandle, AsRawSocket, BorrowedHandle, BorrowedSocket, FromRawHandle, FromRawSocket,
        OwnedSocket, RawSocket,
    };
    use std::path::{Path, PathBuf};
    use std::ptr;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, mpsc};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_ACCESS_DENIED, ERROR_BROKEN_PIPE, ERROR_FILE_EXISTS, GENERIC_WRITE,
        HANDLE, INVALID_HANDLE_VALUE, LocalFree,
    };
    use windows_sys::Win32::Networking::WinSock::{
        FD_SET, FROM_PROTOCOL_INFO, INVALID_SOCKET, SOCKET, SOCKET_ERROR, TIMEVAL,
        WSA_FLAG_OVERLAPPED, WSADuplicateSocketW, WSAPROTOCOL_INFOW, WSASocketW, select,
    };
    #[cfg(test)]
    use windows_sys::Win32::Security::Authorization::ConvertSecurityDescriptorToStringSecurityDescriptorW;
    use windows_sys::Win32::Security::Authorization::{
        ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
        GetNamedSecurityInfoW, SDDL_REVISION_1, SE_FILE_OBJECT,
    };
    #[cfg(test)]
    use windows_sys::Win32::Security::DACL_SECURITY_INFORMATION;
    use windows_sys::Win32::Security::{
        OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SECURITY_ATTRIBUTES,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        CREATE_NEW, CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_TYPE_CHAR, FILE_TYPE_DISK,
        FILE_TYPE_PIPE, GetFileType,
    };
    use windows_sys::Win32::System::Pipes::PeekNamedPipe;
    use windows_sys::Win32::System::Threading::GetCurrentProcessId;
    use windows_sys::core::PWSTR;

    use super::{
        FOREIGN_OWNER_CODE, HandleClass, LOCAL_PIPE_POLL_MAX, LOCAL_PIPE_POLL_START, LoopbackError,
        MAX_DATAGRAM, Peek, TOKEN_BYTES, TOKEN_HEX_LEN, classify_file_type, foreign_owner_message,
        format_port_file, hex_encode, pipe_peek_outcome, read_port_file, split_datagram,
        unsupported_handle_message,
    };

    const _: () = {
        assert!(FILE_TYPE_DISK == super::FILE_TYPE_DISK);
        assert!(FILE_TYPE_CHAR == super::FILE_TYPE_CHAR);
        assert!(FILE_TYPE_PIPE == super::FILE_TYPE_PIPE);
        assert!(ERROR_BROKEN_PIPE == super::ERROR_BROKEN_PIPE as u32);
        assert!(ERROR_ACCESS_DENIED == FOREIGN_OWNER_CODE as u32);
        assert!(ERROR_FILE_EXISTS == 80);
    };

    const RECV_POLL: Duration = Duration::from_millis(250);
    const JOIN_LIMIT: Duration = Duration::from_secs(2);

    type DatagramCallback = Box<dyn FnMut(&[u8]) + Send>;

    pub(super) struct Listener {
        path: PathBuf,
        callback: Arc<Mutex<DatagramCallback>>,
        running: Option<Running>,
    }

    struct Running {
        port: u16,
        stop: Arc<AtomicBool>,
        thread: JoinHandle<()>,
        exit: mpsc::Receiver<()>,
    }

    impl Listener {
        pub(super) fn new(path: PathBuf, on_datagram: impl FnMut(&[u8]) + Send + 'static) -> Self {
            Self {
                path,
                callback: Arc::new(Mutex::new(Box::new(on_datagram))),
                running: None,
            }
        }

        pub(super) fn path(&self) -> &Path {
            &self.path
        }

        pub(super) fn port(&self) -> Option<u16> {
            self.running.as_ref().map(|running| running.port)
        }

        pub(super) fn start(&mut self) -> Result<(), LoopbackError> {
            if self.running.is_some() {
                return Err(LoopbackError::Refused(super::LocalIpcError::new(
                    0,
                    "listener is already started",
                )));
            }
            ensure_parent(&self.path)?;
            let (token, token_hex) = random_token().map_err(LoopbackError::Io)?;
            let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).map_err(LoopbackError::Io)?;
            socket
                .set_read_timeout(Some(RECV_POLL))
                .map_err(LoopbackError::Io)?;
            let port = socket.local_addr().map_err(LoopbackError::Io)?.port();
            write_private_file(&self.path, &format_port_file(port, &token_hex))?;
            let stop = Arc::new(AtomicBool::new(false));
            let (exit_tx, exit_rx) = mpsc::channel();
            let thread = {
                let stop = Arc::clone(&stop);
                let callback = Arc::clone(&self.callback);
                match thread::Builder::new()
                    .name("vyane-localipc".to_owned())
                    .spawn(move || {
                        let _exit_tx = exit_tx;
                        recv_loop(&socket, &stop, &callback, &token);
                    }) {
                    Ok(handle) => handle,
                    Err(err) => {
                        let _ = fs::remove_file(&self.path);
                        return Err(LoopbackError::Io(err));
                    }
                }
            };
            self.running = Some(Running {
                port,
                stop,
                thread,
                exit: exit_rx,
            });
            Ok(())
        }

        pub(super) fn close(&mut self) {
            let Some(running) = self.running.take() else {
                return;
            };
            running.stop.store(true, Ordering::SeqCst);
            if running.thread.thread().id() == thread::current().id() {
                return;
            }
            wake(running.port);
            match running.exit.recv_timeout(JOIN_LIMIT) {
                Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                    let _ = running.thread.join();
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if running.thread.is_finished() {
                        let _ = running.thread.join();
                    }
                }
            }
        }
    }

    fn wake(port: u16) {
        if let Ok(socket) = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)) {
            let _ = socket.send_to(&[0], (Ipv4Addr::LOCALHOST, port));
        }
    }

    fn recv_loop(
        socket: &UdpSocket,
        stop: &AtomicBool,
        callback: &Mutex<DatagramCallback>,
        token: &[u8; TOKEN_HEX_LEN],
    ) {
        let mut buf = [0u8; MAX_DATAGRAM];
        while !stop.load(Ordering::SeqCst) {
            match socket.recv_from(&mut buf) {
                Ok((size, _)) => {
                    if stop.load(Ordering::SeqCst) {
                        return;
                    }
                    if let Some(payload) = split_datagram(&buf[..size], token) {
                        if stop.load(Ordering::SeqCst) {
                            return;
                        }
                        deliver(callback, payload);
                    }
                }
                Err(err) if is_retryable_recv(&err) => continue,
                Err(_) => return,
            }
        }
    }

    fn is_retryable_recv(err: &io::Error) -> bool {
        matches!(
            err.kind(),
            io::ErrorKind::TimedOut
                | io::ErrorKind::WouldBlock
                | io::ErrorKind::Interrupted
                | io::ErrorKind::ConnectionReset
        )
    }

    fn deliver(callback: &Mutex<DatagramCallback>, payload: &[u8]) {
        let mut guard = callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| guard(payload)));
    }

    pub(in crate::localipc) fn send(
        path: &Path,
        payload: &[u8],
        timeout: Option<Duration>,
    ) -> Result<(), LoopbackError> {
        let sid = current_user_sid().map_err(LoopbackError::Io)?;
        send_with_sid(path, payload, timeout, &sid)
    }

    pub(in crate::localipc) fn send_with_sid(
        path: &Path,
        payload: &[u8],
        timeout: Option<Duration>,
        current_sid: &str,
    ) -> Result<(), LoopbackError> {
        let parsed = read_port_file(path)?;
        let owner = file_owner_sid(path).map_err(LoopbackError::Io)?;
        if owner != current_sid {
            return Err(LoopbackError::Refused(super::LocalIpcError::new(
                FOREIGN_OWNER_CODE,
                foreign_owner_message(path, &owner),
            )));
        }
        let mut packet = Vec::with_capacity(TOKEN_HEX_LEN + 1 + payload.len());
        packet.extend_from_slice(&parsed.token);
        packet.push(b':');
        packet.extend_from_slice(payload);
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).map_err(LoopbackError::Io)?;
        let timeout = timeout.unwrap_or(super::DEFAULT_SEND_TIMEOUT);
        if timeout.is_zero() {
            socket.set_nonblocking(true).map_err(LoopbackError::Io)?;
        } else {
            socket
                .set_write_timeout(Some(timeout))
                .map_err(LoopbackError::Io)?;
        }
        socket
            .send_to(&packet, (Ipv4Addr::LOCALHOST, parsed.port))
            .map_err(LoopbackError::Io)?;
        Ok(())
    }

    pub(super) fn wait_handle(
        handle: BorrowedHandle<'_>,
        timeout: Option<Duration>,
    ) -> io::Result<bool> {
        let raw = handle.as_raw_handle();
        // SAFETY: `handle` stays open for this call. GetFileType only reads
        // the handle type and does not consume bytes.
        let kind = unsafe { GetFileType(raw) };
        match classify_file_type(kind) {
            HandleClass::File => Ok(true),
            HandleClass::Pipe => poll_pipe(raw, timeout),
            HandleClass::Rejected(kind) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                unsupported_handle_message(kind),
            )),
        }
    }

    fn poll_pipe(handle: HANDLE, timeout: Option<Duration>) -> io::Result<bool> {
        let deadline = timeout.map(|duration| Instant::now() + duration);
        let mut delay = LOCAL_PIPE_POLL_START;
        loop {
            let mut available = 0u32;
            // SAFETY: `handle` is the caller's pipe. A null buffer with a
            // zero size does not read or remove bytes. `available` is writable.
            let ok = unsafe {
                PeekNamedPipe(
                    handle,
                    ptr::null_mut(),
                    0,
                    ptr::null_mut(),
                    &mut available,
                    ptr::null_mut(),
                )
            };
            let error = if ok == 0 {
                io::Error::last_os_error().raw_os_error().unwrap_or(0)
            } else {
                0
            };
            match pipe_peek_outcome(ok != 0, available, error) {
                Peek::Readable => return Ok(true),
                Peek::Failed(code) => return Err(io::Error::from_raw_os_error(code)),
                Peek::Waiting => {}
            }
            if !park_until(deadline, &mut delay) {
                return Ok(false);
            }
        }
    }

    fn park_until(deadline: Option<Instant>, delay: &mut Duration) -> bool {
        match deadline {
            Some(deadline) => {
                let now = Instant::now();
                if now >= deadline {
                    return false;
                }
                thread::sleep((*delay).min(deadline - now));
            }
            None => thread::sleep(*delay),
        }
        *delay = (*delay * 2).min(LOCAL_PIPE_POLL_MAX);
        true
    }

    pub(super) fn wait_socket(
        sock: BorrowedSocket<'_>,
        timeout: Option<Duration>,
    ) -> io::Result<bool> {
        let raw = winsock(sock.as_raw_socket());
        let deadline = timeout.map(|duration| Instant::now() + duration);
        loop {
            let mut set = FD_SET {
                fd_count: 1,
                ..FD_SET::default()
            };
            set.fd_array[0] = raw;
            let tv = deadline
                .map(|deadline| timeval_from(deadline.saturating_duration_since(Instant::now())));
            let tv_ptr = tv.as_ref().map_or(ptr::null(), |value| value);
            // SAFETY: `set` holds one live socket. `tv_ptr` is null (block)
            // or points at `tv`. std started Winsock when the socket was created.
            let rc = unsafe { select(0, &mut set, ptr::null_mut(), ptr::null_mut(), tv_ptr) };
            if rc == SOCKET_ERROR {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                        return Ok(false);
                    }
                    continue;
                }
                return Err(err);
            }
            return Ok(rc > 0);
        }
    }

    fn timeval_from(duration: Duration) -> TIMEVAL {
        let secs = duration.as_secs().min(i32::MAX as u64);
        #[allow(clippy::unnecessary_cast, clippy::cast_possible_truncation)]
        let tv_sec = secs as i32;
        #[allow(clippy::unnecessary_cast, clippy::cast_possible_truncation)]
        let tv_usec = duration.subsec_micros() as i32;
        TIMEVAL { tv_sec, tv_usec }
    }

    pub(super) fn duplicate_socket(
        sock: BorrowedSocket<'_>,
    ) -> Result<OwnedSocket, super::DuplicateSocketError> {
        let raw = winsock(sock.as_raw_socket());
        let mut info = WSAPROTOCOL_INFOW::default();
        // SAFETY: `raw` is a live socket the caller keeps. `info` is writable
        // for the call. WSADuplicateSocketW does not close `raw`.
        let rc = unsafe { WSADuplicateSocketW(raw, GetCurrentProcessId(), &mut info) };
        if rc != 0 {
            return Err(super::DuplicateSocketError::Io(io::Error::last_os_error()));
        }
        // SAFETY: `info` was written by WSADuplicateSocketW. FROM_PROTOCOL_INFO
        // builds a new overlapped socket from it. The result is not aliased.
        let duplicated = unsafe {
            WSASocketW(
                FROM_PROTOCOL_INFO,
                FROM_PROTOCOL_INFO,
                FROM_PROTOCOL_INFO,
                &info,
                0,
                WSA_FLAG_OVERLAPPED,
            )
        };
        if duplicated == INVALID_SOCKET {
            return Err(super::DuplicateSocketError::Io(io::Error::last_os_error()));
        }
        // SAFETY: `duplicated` is a newly created socket with no other owner.
        Ok(unsafe { OwnedSocket::from_raw_socket(raw_socket(duplicated)) })
    }

    fn winsock(raw: RawSocket) -> SOCKET {
        // `RawSocket` is `u64`. `SOCKET` is pointer-sized. A live handle fits.
        #[allow(clippy::cast_possible_truncation)]
        let sock = raw as SOCKET;
        sock
    }

    fn raw_socket(sock: SOCKET) -> RawSocket {
        #[allow(clippy::cast_possible_truncation)]
        let raw = sock as RawSocket;
        raw
    }

    fn current_user_sid() -> io::Result<String> {
        crate::securefile::current_user_id(false).map_err(io::Error::other)
    }

    fn ensure_parent(path: &Path) -> Result<(), LoopbackError> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(LoopbackError::Io)?;
        }
        Ok(())
    }

    fn random_token() -> io::Result<([u8; TOKEN_HEX_LEN], String)> {
        let mut raw = [0u8; TOKEN_BYTES];
        getrandom::fill(&mut raw).map_err(io::Error::other)?;
        let hex = hex_encode(&raw);
        let mut token = [0u8; TOKEN_HEX_LEN];
        token.copy_from_slice(hex.as_bytes());
        Ok((token, hex))
    }

    fn write_private_file(path: &Path, data: &[u8]) -> Result<(), LoopbackError> {
        let sid = current_user_sid().map_err(LoopbackError::Io)?;
        let sddl = format!("O:{sid}D:P(A;;FA;;;{sid})");
        let wide_sddl = wide_str(&sddl);
        let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
        // SAFETY: `wide_sddl` is NUL-terminated. On success `descriptor` is
        // a LocalAlloc block owned by `LocalFreeGuard`.
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                wide_sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(LoopbackError::Io(io::Error::last_os_error()));
        }
        let _free = LocalFreeGuard(descriptor);
        let (temp, handle) = create_temp(path, descriptor)?;
        if let Err(err) = write_all_handle(handle, data) {
            let _ = fs::remove_file(&temp);
            return Err(LoopbackError::Io(err));
        }
        if let Err(err) = replace_with_retry(&temp, path) {
            let _ = fs::remove_file(&temp);
            return Err(err);
        }
        Ok(())
    }

    fn create_temp(
        path: &Path,
        descriptor: PSECURITY_DESCRIPTOR,
    ) -> Result<(PathBuf, WinHandle), LoopbackError> {
        let attributes = SECURITY_ATTRIBUTES {
            nLength: u32::try_from(mem::size_of::<SECURITY_ATTRIBUTES>()).unwrap_or(u32::MAX),
            lpSecurityDescriptor: descriptor,
            bInheritHandle: 0,
        };
        for _ in 0..8 {
            let temp = temp_path(path).map_err(LoopbackError::Io)?;
            let wide = wide_path(&temp).map_err(LoopbackError::Io)?;
            // SAFETY: `wide` is NUL-terminated. `attributes` points at
            // `descriptor`, which the caller frees after this returns.
            // CREATE_NEW fails if the name exists. The handle is owned here.
            let handle = unsafe {
                CreateFileW(
                    wide.as_ptr(),
                    GENERIC_WRITE,
                    0,
                    &attributes,
                    CREATE_NEW,
                    FILE_ATTRIBUTE_NORMAL,
                    ptr::null_mut(),
                )
            };
            if handle.is_null() || handle == INVALID_HANDLE_VALUE {
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(ERROR_FILE_EXISTS as i32) {
                    continue;
                }
                return Err(LoopbackError::Io(err));
            }
            return Ok((temp, WinHandle(handle)));
        }
        Err(LoopbackError::Refused(super::LocalIpcError::new(
            0,
            format!("could not create a temp file next to {}", path.display()),
        )))
    }

    fn temp_path(path: &Path) -> io::Result<PathBuf> {
        let name = path.file_name().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "port file path has no file name",
            )
        })?;
        let mut random = [0u8; 6];
        getrandom::fill(&mut random).map_err(io::Error::other)?;
        let mut file_name = OsString::from(".");
        file_name.push(name);
        file_name.push(".");
        file_name.push(hex_encode(&random));
        file_name.push(".tmp");
        Ok(path.with_file_name(file_name))
    }

    fn write_all_handle(handle: WinHandle, data: &[u8]) -> io::Result<()> {
        let mut file = handle.into_file();
        file.write_all(data)?;
        file.flush()?;
        Ok(())
    }

    fn replace_with_retry(source: &Path, target: &Path) -> Result<(), LoopbackError> {
        for attempt in 0..20 {
            match fs::rename(source, target) {
                Ok(()) => return Ok(()),
                Err(err) if is_sharing_or_access(&err) && attempt < 19 => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(err) => return Err(LoopbackError::Io(err)),
            }
        }
        Err(LoopbackError::Io(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "could not replace the port file",
        )))
    }

    fn is_sharing_or_access(err: &io::Error) -> bool {
        err.kind() == io::ErrorKind::PermissionDenied || matches!(err.raw_os_error(), Some(5 | 32))
    }

    pub(in crate::localipc) fn file_owner_sid(path: &Path) -> io::Result<String> {
        let wide = wide_path(path)?;
        let mut owner: PSID = ptr::null_mut();
        let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
        // SAFETY: `wide` is NUL-terminated. On success `descriptor` is
        // LocalAlloc'd and `owner` points inside it.
        let code = unsafe {
            GetNamedSecurityInfoW(
                wide.as_ptr(),
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION,
                &mut owner,
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                &mut descriptor,
            )
        };
        if code != 0 {
            return Err(io::Error::from_raw_os_error(code as i32));
        }
        let _descriptor = LocalFreeGuard(descriptor);
        sid_to_string(owner)
    }

    #[cfg(test)]
    pub(in crate::localipc) fn file_security_sddl(path: &Path) -> io::Result<String> {
        let wide = wide_path(path)?;
        let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
        let info = OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION;
        // SAFETY: `wide` is NUL-terminated. On success `descriptor` is
        // LocalAlloc'd. The string conversion reads that block.
        let code = unsafe {
            GetNamedSecurityInfoW(
                wide.as_ptr(),
                SE_FILE_OBJECT,
                info,
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                &mut descriptor,
            )
        };
        if code != 0 {
            return Err(io::Error::from_raw_os_error(code as i32));
        }
        let _descriptor = LocalFreeGuard(descriptor);
        let mut out: PWSTR = ptr::null_mut();
        // SAFETY: `descriptor` is the live security descriptor from above.
        // On success `out` is a LocalAlloc wide string owned by `LocalWide`.
        let ok = unsafe {
            ConvertSecurityDescriptorToStringSecurityDescriptorW(
                descriptor,
                SDDL_REVISION_1,
                info,
                &mut out,
                ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        let _free = LocalWide(out);
        wide_to_string(out)
    }

    /// Windows' own spelling of `sddl`, including aliases such as `LA`.
    #[cfg(test)]
    pub(in crate::localipc) fn canonical_sddl(sddl: &str) -> io::Result<String> {
        let wide = wide_str(sddl);
        let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
        // SAFETY: `wide` is NUL-terminated. On success `descriptor` is a
        // LocalAlloc block owned by `LocalFreeGuard`.
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                wide.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        let _descriptor = LocalFreeGuard(descriptor);
        let info = OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION;
        let mut out: PWSTR = ptr::null_mut();
        // SAFETY: `descriptor` is the live descriptor from above. On success
        // `out` is a LocalAlloc wide string owned by `LocalWide`.
        let ok = unsafe {
            ConvertSecurityDescriptorToStringSecurityDescriptorW(
                descriptor,
                SDDL_REVISION_1,
                info,
                &mut out,
                ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        let _free = LocalWide(out);
        wide_to_string(out)
    }

    fn sid_to_string(psid: PSID) -> io::Result<String> {
        if psid.is_null() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "SID is null"));
        }
        let mut raw: PWSTR = ptr::null_mut();
        // SAFETY: `psid` points at a SID inside a descriptor this function's
        // caller still owns. The wide string is freed by `LocalWide`.
        let ok = unsafe { ConvertSidToStringSidW(psid, &mut raw) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        let _free = LocalWide(raw);
        wide_to_string(raw)
    }

    fn wide_to_string(ptr: *const u16) -> io::Result<String> {
        if ptr.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "wide string is null",
            ));
        }
        let mut len = 0usize;
        // SAFETY: ConvertSid / ConvertSecurityDescriptor returned a
        // NUL-terminated wide string. The scan stops at 2048 units.
        let units = unsafe {
            while len < 2048 && *ptr.add(len) != 0 {
                len += 1;
            }
            if len == 2048 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "wide string is unexpectedly long",
                ));
            }
            std::slice::from_raw_parts(ptr, len)
        };
        String::from_utf16(units)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "wide string is not Unicode"))
    }

    fn wide_str(text: &str) -> Vec<u16> {
        text.encode_utf16().chain(std::iter::once(0)).collect()
    }

    fn wide_path(path: &Path) -> io::Result<Vec<u16>> {
        let mut wide: Vec<u16> = path.as_os_str().encode_wide().collect();
        if wide.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "path contains an interior NUL",
            ));
        }
        wide.push(0);
        Ok(wide)
    }

    struct WinHandle(HANDLE);

    impl WinHandle {
        fn into_file(self) -> File {
            let handle = ManuallyDrop::new(self);
            // SAFETY: CreateFileW returned this handle. ManuallyDrop skips
            // CloseHandle, so the File is the only owner.
            unsafe { File::from_raw_handle(handle.0) }
        }
    }

    impl Drop for WinHandle {
        fn drop(&mut self) {
            if !self.0.is_null() && self.0 != INVALID_HANDLE_VALUE {
                // SAFETY: this module owns the handle and has not moved it into a File.
                unsafe {
                    CloseHandle(self.0);
                }
            }
        }
    }

    struct LocalFreeGuard(PSECURITY_DESCRIPTOR);

    impl Drop for LocalFreeGuard {
        fn drop(&mut self) {
            if !self.0.is_null() {
                // SAFETY: the security APIs allocated this descriptor with LocalAlloc.
                unsafe {
                    LocalFree(self.0);
                }
            }
        }
    }

    struct LocalWide(PWSTR);

    impl Drop for LocalWide {
        fn drop(&mut self) {
            if !self.0.is_null() {
                // SAFETY: ConvertSidToStringSidW or the SDDL converter allocated this string.
                unsafe {
                    LocalFree(self.0.cast());
                }
            }
        }
    }
}

#[cfg(windows)]
const LOCAL_PIPE_POLL_START: Duration = Duration::from_millis(1);
#[cfg(windows)]
const LOCAL_PIPE_POLL_MAX: Duration = Duration::from_millis(20);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::io::{Read, Seek, Write};
    use std::net::{TcpListener, TcpStream};
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn anonymous_pipe_is_readable_on_data_and_eof_without_consuming() {
        let (mut reader, mut writer) = std::io::pipe().unwrap();
        assert!(!wait_readable(&reader, Some(Duration::from_millis(50))).unwrap());
        writer.write_all(b"x").unwrap();
        assert!(wait_readable(&reader, Some(Duration::from_secs(2))).unwrap());
        let mut buf = [0u8; 1];
        assert_eq!(reader.read(&mut buf).unwrap(), 1);
        assert_eq!(&buf, b"x");
        assert!(!wait_readable(&reader, Some(Duration::from_millis(50))).unwrap());
        drop(writer);
        assert!(wait_readable(&reader, Some(Duration::from_secs(2))).unwrap());
        assert_eq!(reader.read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn subprocess_pipe_becomes_readable_when_the_child_writes() {
        let mut command = sleeping_writer();
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = command.spawn().unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let started = Instant::now();
        let mut collected = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if !wait_readable(&stdout, Some(Duration::from_millis(50))).unwrap() {
                if String::from_utf8_lossy(&collected).contains("ready") {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "child produced no ready line: {collected:?}"
                );
                continue;
            }
            let mut buf = [0u8; 64];
            let count = stdout.read(&mut buf).unwrap();
            assert_ne!(count, 0, "child stdout closed before ready");
            collected.extend_from_slice(&buf[..count]);
            if String::from_utf8_lossy(&collected).contains("ready") {
                while wait_readable(&stdout, Some(Duration::from_millis(20))).unwrap() {
                    let count = stdout.read(&mut buf).unwrap();
                    if count == 0 {
                        break;
                    }
                    collected.extend_from_slice(&buf[..count]);
                }
                break;
            }
        }
        assert!(started.elapsed() >= Duration::from_millis(200));
        assert!(String::from_utf8_lossy(&collected).contains("ready"));
        assert!(!wait_readable(&stdout, Some(Duration::from_millis(50))).unwrap());
        drop(stdout);
        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn socket_becomes_readable_through_select_without_consuming() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        client.set_nodelay(true).unwrap();
        assert!(!wait_readable(&server, Some(Duration::from_millis(50))).unwrap());
        client.write_all(b"y").unwrap();
        assert!(wait_readable(&server, Some(Duration::from_secs(2))).unwrap());
        let mut buf = [0u8; 1];
        assert_eq!(server.read(&mut buf).unwrap(), 1);
        assert_eq!(&buf, b"y");
    }

    #[test]
    fn regular_file_in_a_temp_dir_is_always_readable_and_is_not_consumed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plain.txt");
        std::fs::write(&path, b"abc").unwrap();
        let mut file = File::open(&path).unwrap();
        assert_eq!(file.stream_position().unwrap(), 0);
        assert!(wait_readable(&file, Some(Duration::ZERO)).unwrap());
        assert_eq!(file.stream_position().unwrap(), 0);
        let mut buf = [0u8; 1];
        assert_eq!(file.read(&mut buf).unwrap(), 1);
        assert_eq!(&buf, b"a");
        let position = file.stream_position().unwrap();
        assert!(wait_readable(&file, Some(Duration::ZERO)).unwrap());
        assert_eq!(file.stream_position().unwrap(), position);
    }

    #[cfg(unix)]
    #[test]
    fn character_device_stays_readable_on_select() {
        let file = File::open("/dev/null").unwrap();
        assert!(wait_readable(&file, Some(Duration::ZERO)).unwrap());
    }

    #[cfg(windows)]
    #[test]
    fn character_handle_is_rejected() {
        let file = File::open("NUL").unwrap();
        let err = wait_readable(&file, Some(Duration::from_millis(20))).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
        assert!(
            err.to_string().contains("file type 2"),
            "expected FILE_TYPE_CHAR, got {err}"
        );
    }

    #[test]
    fn duplicate_socket_does_not_close_the_original() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let duplicated = duplicate_socket(&listener).unwrap();
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            assert_ne!(duplicated.as_raw_fd(), listener.as_raw_fd());
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawSocket;
            assert_ne!(duplicated.as_raw_socket(), listener.as_raw_socket());
        }
        drop(duplicated);
        let _stream = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn duplicate_socket_rejects_pipes_and_regular_files() {
        let (reader, _writer) = std::io::pipe().unwrap();
        let err = duplicate_socket(&reader).unwrap_err();
        assert_eq!(err.to_string(), "is not a socket");

        let dir = tempfile::tempdir().unwrap();
        let file = File::create(dir.path().join("plain")).unwrap();
        let err = duplicate_socket(&file).unwrap_err();
        assert_eq!(err.to_string(), "is not a socket");
    }

    #[cfg(not(windows))]
    #[test]
    fn loopback_entry_points_are_not_implemented_and_do_not_read_the_path() {
        let missing = Path::new("no-such-localipc-port-file");
        let err = send_loopback_datagram(missing, b"room-a", None).unwrap_err();
        assert_eq!(err.to_string(), SEND_UNSUPPORTED);
        assert!(err.is_not_implemented());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wake.sock");
        std::fs::write(&path, b"garbage\n").unwrap();
        let err = send_loopback_datagram(&path, b"room-a", None).unwrap_err();
        assert!(err.is_not_implemented());

        let err = LoopbackDatagramListener::new(&path, |_| {}).unwrap_err();
        assert_eq!(err.to_string(), LISTENER_UNSUPPORTED);
        assert!(err.is_not_implemented());
    }

    #[test]
    fn port_file_grammar_accepts_the_canonical_line_and_rejects_the_rest() {
        let token = "ab".repeat(16);
        let bytes = format_port_file(34567, &token);
        assert_eq!(
            bytes,
            format!("{PORT_FILE_MAGIC} 127.0.0.1 34567 {token}\n").into_bytes()
        );
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wake.sock");
        std::fs::write(&path, &bytes).unwrap();
        let parsed = read_port_file(&path).unwrap();
        assert_eq!(parsed.port, 34567);
        assert_eq!(parsed.token.as_slice(), token.as_bytes());

        std::fs::write(&path, format_port_file(65535, &token)).unwrap();
        assert_eq!(read_port_file(&path).unwrap().port, 65535);

        let mut no_newline = format_port_file(7, &token);
        no_newline.pop();
        std::fs::write(&path, &no_newline).unwrap();
        assert_eq!(read_port_file(&path).unwrap().port, 7);

        std::fs::write(
            &path,
            format!("{PORT_FILE_MAGIC} 127.0.0.1 04000 {token}\n"),
        )
        .unwrap();
        assert_eq!(read_port_file(&path).unwrap().port, 4000);

        let missing = dir.path().join("absent.sock");
        let err = read_port_file(&missing).unwrap_err();
        assert!(err.is_not_found());

        for content in malformed_port_files() {
            std::fs::write(&path, &content).unwrap();
            let err = read_port_file(&path).unwrap_err();
            let refused = err.refused().expect("malformed port file is LocalIpcError");
            assert_eq!(refused.raw_os_error(), 0);
            let message = refused.message();
            assert!(
                message.contains("malformed") || message.contains("too large"),
                "{message}"
            );
        }

        let mut huge = format_port_file(9, &token);
        huge.extend(std::iter::repeat_n(b' ', 600));
        std::fs::write(&path, &huge).unwrap();
        let err = read_port_file(&path).unwrap_err();
        assert!(err.refused().unwrap().message().contains("too large"));
    }

    #[test]
    fn pipe_and_file_type_decisions_match_the_windows_wait() {
        assert_eq!(classify_file_type(FILE_TYPE_DISK), HandleClass::File);
        assert_eq!(classify_file_type(FILE_TYPE_PIPE), HandleClass::Pipe);
        assert_eq!(
            classify_file_type(FILE_TYPE_CHAR),
            HandleClass::Rejected(FILE_TYPE_CHAR)
        );
        assert_eq!(classify_file_type(0), HandleClass::Rejected(0));
        assert_eq!(
            classify_file_type(FILE_TYPE_PIPE | 0x8000),
            HandleClass::Rejected(FILE_TYPE_PIPE | 0x8000)
        );
        assert!(unsupported_handle_message(FILE_TYPE_CHAR).contains("file type 2"));

        assert_eq!(pipe_peek_outcome(true, 4, 0), Peek::Readable);
        assert_eq!(pipe_peek_outcome(true, 0, 0), Peek::Waiting);
        assert_eq!(
            pipe_peek_outcome(false, 0, ERROR_BROKEN_PIPE),
            Peek::Readable
        );
        assert_eq!(pipe_peek_outcome(false, 0, 5), Peek::Failed(5));
    }

    #[test]
    fn datagram_without_the_matching_token_is_dropped() {
        let token = *b"0123456789abcdef0123456789abcdef";
        assert_eq!(split_datagram(b"room-forged", &token), None);
        let mut wrong = b"0".repeat(32);
        wrong.extend_from_slice(b":room-forged");
        assert_eq!(split_datagram(&wrong, &token), None);
        assert_eq!(split_datagram(&token, &token), None);
        let mut good = token.to_vec();
        good.extend_from_slice(b":room-real");
        assert_eq!(split_datagram(&good, &token), Some(&b"room-real"[..]));
        let mut empty = token.to_vec();
        empty.push(b':');
        assert_eq!(split_datagram(&empty, &token), Some(&b""[..]));
        let mut shifted = token.to_vec();
        shifted.insert(0, b':');
        assert_eq!(split_datagram(&shifted, &token), None);
    }

    #[test]
    fn foreign_owner_refusal_names_the_owner_and_uses_access_denied() {
        let path = Path::new("wake.sock");
        let message = foreign_owner_message(path, "S-1-5-18");
        assert!(message.contains("not the current user"));
        assert!(message.contains("S-1-5-18"));
        let err = LocalIpcError::new(FOREIGN_OWNER_CODE, message);
        assert_eq!(err.raw_os_error(), 5);
    }

    #[cfg(windows)]
    #[test]
    fn port_file_is_private_to_the_current_user() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wake.sock");
        let mut listener = LoopbackDatagramListener::new(&path, |_| {}).unwrap();
        listener.start().unwrap();
        let sid = crate::securefile::current_user_id(false).unwrap();
        assert_eq!(win::file_owner_sid(&path).unwrap(), sid);
        // Windows prints a well-known SID as an alias. The built-in
        // Administrator comes back as `LA`. Compare the file with that
        // canonical spelling of the descriptor this process wrote.
        let requested = format!("O:{sid}D:P(A;;FA;;;{sid})");
        let sddl = win::file_security_sddl(&path).unwrap();
        assert_eq!(sddl, win::canonical_sddl(&requested).unwrap());
        assert!(sddl.contains("D:P(A;;FA;;;"));
        assert!(!sddl.contains("WD"));
        let text = std::fs::read_to_string(&path).unwrap();
        let fields: Vec<_> = text.split_whitespace().collect();
        assert_eq!(fields[0], PORT_FILE_MAGIC);
        assert_eq!(fields[1], "127.0.0.1");
        assert_eq!(fields[2].parse::<u16>().unwrap(), listener.port().unwrap());
        assert_eq!(fields[3].len(), 32);
        assert!(
            fields[3]
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        );
        listener.close();
        assert!(path.is_file());
        assert!(listener.port().is_none());
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    #[cfg(windows)]
    #[test]
    fn forged_datagram_without_token_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wake.sock");
        let (tx, rx) = std::sync::mpsc::channel();
        let mut listener = LoopbackDatagramListener::new(&path, move |payload| {
            tx.send(payload.to_vec()).unwrap();
        })
        .unwrap();
        listener.start().unwrap();
        let port = listener.port().unwrap();
        let forger = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        forger.send_to(b"room-forged", ("127.0.0.1", port)).unwrap();
        let mut wrong = b"0".repeat(32);
        wrong.extend_from_slice(b":room-forged");
        forger.send_to(&wrong, ("127.0.0.1", port)).unwrap();
        std::thread::sleep(Duration::from_millis(400));
        assert!(rx.try_recv().is_err());
        send_loopback_datagram(&path, b"room-real", None).unwrap();
        let got = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(got, b"room-real");
        listener.close();
    }

    #[cfg(windows)]
    #[test]
    fn sender_refuses_port_file_owned_by_someone_else() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wake.sock");
        let (tx, rx) = std::sync::mpsc::channel();
        let mut listener = LoopbackDatagramListener::new(&path, move |payload| {
            tx.send(payload.to_vec()).unwrap();
        })
        .unwrap();
        listener.start().unwrap();
        let err = win::send_with_sid(&path, b"room-a", None, "S-1-5-18").unwrap_err();
        let refused = err.refused().expect("foreign owner is LocalIpcError");
        assert!(refused.message().contains("not the current user"));
        assert_eq!(refused.raw_os_error(), 5);
        std::thread::sleep(Duration::from_millis(200));
        assert!(rx.try_recv().is_err());
        listener.close();
    }

    #[cfg(windows)]
    #[test]
    fn sender_refuses_malformed_port_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wake.sock");
        for content in malformed_port_files() {
            std::fs::write(&path, &content).unwrap();
            let err = send_loopback_datagram(&path, b"room-a", None).unwrap_err();
            assert!(err.refused().is_some(), "{err}");
        }
    }

    #[cfg(windows)]
    #[test]
    fn sender_missing_port_file_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let err =
            send_loopback_datagram(dir.path().join("absent.sock"), b"room-a", None).unwrap_err();
        assert!(err.is_not_found());
    }

    #[cfg(windows)]
    #[test]
    fn close_stops_delivery_and_keeps_the_port_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wake.sock");
        let (tx, rx) = std::sync::mpsc::channel();
        let mut listener = LoopbackDatagramListener::new(&path, move |payload| {
            tx.send(payload.to_vec()).unwrap();
        })
        .unwrap();
        listener.start().unwrap();
        listener.close();
        assert!(path.is_file());
        assert!(listener.port().is_none());
        let _ = send_loopback_datagram(&path, b"room-late", None);
        std::thread::sleep(Duration::from_millis(200));
        assert!(rx.try_recv().is_err());
    }

    #[cfg(any(windows, test))]
    fn malformed_port_files() -> Vec<Vec<u8>> {
        let token = b"a".repeat(32);
        vec![
            Vec::new(),
            b"garbage\n".to_vec(),
            {
                let mut bytes = b"vyane-localipc-dgram/1 10.0.0.5 4000 ".to_vec();
                bytes.extend_from_slice(&token);
                bytes.push(b'\n');
                bytes
            },
            {
                let mut bytes = b"vyane-localipc-dgram/1 127.0.0.1 0 ".to_vec();
                bytes.extend_from_slice(&token);
                bytes.push(b'\n');
                bytes
            },
            b"vyane-localipc-dgram/1 127.0.0.1 4000 short\n".to_vec(),
            vec![b'x'; 4096],
            {
                let mut bytes = b"vyane-localipc-dgram/1 127.0.0.1 4000 ".to_vec();
                bytes.extend(std::iter::repeat_n(b'A', 32));
                bytes.push(b'\n');
                bytes
            },
            {
                let mut bytes = b"vyane-localipc-dgram/1 127.0.0.1 65536 ".to_vec();
                bytes.extend_from_slice(&token);
                bytes.push(b'\n');
                bytes
            },
        ]
    }

    #[cfg(unix)]
    fn sleeping_writer() -> Command {
        let mut command = Command::new("sh");
        command.args(["-c", "sleep 0.3; printf '%s\\n' ready; read line"]);
        command
    }

    #[cfg(windows)]
    fn sleeping_writer() -> Command {
        let mut command = Command::new("cmd");
        command.args(["/c", "ping -n 2 127.0.0.1 >nul & echo ready& set /p dummy="]);
        command
    }
}
