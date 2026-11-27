//! Private-file and no-follow primitives for POSIX and Windows.
//!
//! On POSIX each function is a straight passthrough of the original call. On
//! Windows each function either provides an equivalent guarantee or fails loudly.
//!
//! POSIX passthroughs are `open` with `O_NOFOLLOW` (and the non-inheritable
//! flag `os.open` sets), `chmod` / `fchmod`, `open(O_DIRECTORY)` plus `fsync`,
//! `rename`, and `getuid` / `geteuid`. A path check uses `stat`, which follows
//! a symlink. `pin_directory` fails on POSIX: callers keep using a directory
//! file descriptor there.
//!
//! Windows equivalents:
//!
//! * `open_nofollow` opens the leaf with `FILE_FLAG_OPEN_REPARSE_POINT` and
//!   rejects it when the handle's reparse tag is `IO_REPARSE_TAG_SYMLINK` or
//!   `IO_REPARSE_TAG_MOUNT_POINT`. Other reparse points (OneDrive, dedup, WSL,
//!   and the rest) stay ordinary files, matching POSIX `O_NOFOLLOW`.
//! * `private_problem` / `owned_by_current_user` read the security descriptor.
//!   The owner must be the current user or the token's default owner. Every
//!   allow ACE that applies must be the current user, `SYSTEM`,
//!   `Administrators`, or `OWNER RIGHTS`. A NULL DACL, an Everyone or Users
//!   allow, or an allow ACE this module does not parse is not private. `st`
//!   and `effective` are ignored: the live descriptor is the answer. A symlink
//!   or junction is not owned and not private.
//! * `make_private` / `make_private_fd` install a protected DACL for the
//!   current user (inheritable on a directory). An owner that is neither the
//!   current user nor the token's default owner is refused, even if
//!   `WRITE_DAC` would allow the write. A mode with group or other bits is not
//!   a privacy request on Windows; it only toggles the read-only attribute.
//! * `fsync_directory` opens the directory for write and calls
//!   `FlushFileBuffers`.
//! * `replace` is `MoveFileExW(MOVEFILE_REPLACE_EXISTING)`. A read-only target
//!   has that attribute cleared for the rename and restored if the rename
//!   still fails. Access-denied and sharing-violation errors are retried for
//!   about one second.
//! * `pin_directory` holds every component from the volume root open without
//!   `FILE_SHARE_DELETE` and rejects a symlink or junction component.
//!
//! `O_APPEND` without `O_TRUNC` uses `FILE_APPEND_DATA` and not
//! `FILE_WRITE_DATA`, so writes land at end of file. `O_APPEND` together with
//! `O_TRUNC` truncates and then leaves a writable handle: a Rust [`File`] has
//! no CRT `O_APPEND` bit. POSIX passes both flags through to `open`.
//!
//! # What CI runs
//!
//! Linux and macOS CI execute the POSIX passthrough, including symlink
//! rejection, `chmod` / `fchmod`, directory `fsync`, and `rename`. They also
//! execute the pure decision tables below (which reparse tags count as links,
//! and which DACL entries count as private). They do not open a Windows
//! handle, read a security descriptor, or pin a directory.
//!
//! The Win32 calls are `cfg(windows)`. Windows CI is what compiles and runs
//! them. A green Linux run does not certify a live ACL. Constants used by the
//! decision tables are checked against `windows-sys` when that target is
//! built, so a drifted tag or ACE number fails that build instead of being
//! reinterpreted here.

use std::fmt;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

/// `true` when this build's private-file primitives use the Windows guarantees.
pub const IS_WINDOWS: bool = cfg!(windows);

/// `0o600`, the private file mode.
///
/// ```
/// assert_eq!(vyane_core::PRIVATE_FILE_MODE, 0o600);
/// ```
pub const PRIVATE_FILE_MODE: u32 = 0o600;

/// `0o700`, the private directory mode.
///
/// ```
/// assert_eq!(vyane_core::PRIVATE_DIR_MODE, 0o700);
/// ```
pub const PRIVATE_DIR_MODE: u32 = 0o700;

/// Why a file or directory is not private.
///
/// The text is `"owner"` or `"mode"`, the same two results the checks report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivateProblem {
    /// The owner is not the current user (and, on Windows, not the token's
    /// default owner). A symlink or junction on Windows is reported here.
    Owner,
    /// Group/other permission bits are set, or the Windows DACL is not private.
    Mode,
}

impl PrivateProblem {
    /// `"owner"` or `"mode"`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Mode => "mode",
        }
    }
}

impl fmt::Display for PrivateProblem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Open flags for [`open_nofollow`].
///
/// On POSIX the bits are the platform `O_*` values and are passed through to
/// `open` with `O_NOFOLLOW` and `O_CLOEXEC` added. On Windows the named flags
/// are the MSVC CRT values CPython exposes as `os.O_*`; only `RDONLY`,
/// `WRONLY`, `RDWR`, `CREAT`, `EXCL`, `TRUNC`, and `APPEND` change the open.
///
/// `RDONLY` is zero on both platforms. It is the access mode when neither
/// `WRONLY` nor `RDWR` is set. [`OpenFlags::contains`] is true for `RDONLY`
/// on every value, because masking with zero never removes a bit.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct OpenFlags(u32);

impl OpenFlags {
    /// `O_RDONLY` (`0`).
    pub const RDONLY: Self = Self(0);

    /// `O_WRONLY`.
    #[cfg(unix)]
    pub const WRONLY: Self = Self(rustix::fs::OFlags::WRONLY.bits());
    /// `O_WRONLY` (`1` on Windows).
    #[cfg(windows)]
    pub const WRONLY: Self = Self(1);

    /// `O_RDWR`.
    #[cfg(unix)]
    pub const RDWR: Self = Self(rustix::fs::OFlags::RDWR.bits());
    /// `O_RDWR` (`2` on Windows).
    #[cfg(windows)]
    pub const RDWR: Self = Self(2);

    /// `O_CREAT`.
    #[cfg(unix)]
    pub const CREAT: Self = Self(rustix::fs::OFlags::CREATE.bits());
    /// `O_CREAT` (`0x100` on Windows).
    #[cfg(windows)]
    pub const CREAT: Self = Self(0x0100);

    /// `O_EXCL`.
    #[cfg(unix)]
    pub const EXCL: Self = Self(rustix::fs::OFlags::EXCL.bits());
    /// `O_EXCL` (`0x400` on Windows).
    #[cfg(windows)]
    pub const EXCL: Self = Self(0x0400);

    /// `O_TRUNC`.
    #[cfg(unix)]
    pub const TRUNC: Self = Self(rustix::fs::OFlags::TRUNC.bits());
    /// `O_TRUNC` (`0x200` on Windows).
    #[cfg(windows)]
    pub const TRUNC: Self = Self(0x0200);

    /// `O_APPEND`.
    #[cfg(unix)]
    pub const APPEND: Self = Self(rustix::fs::OFlags::APPEND.bits());
    /// `O_APPEND` (`8` on Windows).
    #[cfg(windows)]
    pub const APPEND: Self = Self(8);

    /// Bits exactly as stored.
    #[must_use]
    pub const fn from_raw(bits: u32) -> Self {
        Self(bits)
    }

    /// Stored bits.
    #[must_use]
    pub const fn bits(self) -> u32 {
        self.0
    }

    /// Every bit of `other` is set. A zero `other` ([`Self::RDONLY`]) matches
    /// every value.
    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

impl fmt::Debug for OpenFlags {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "OpenFlags({:#x})", self.0)
    }
}

impl std::ops::BitOr for OpenFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for OpenFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

#[cfg(unix)]
const _: () = {
    assert!(OpenFlags::RDONLY.bits() == rustix::fs::OFlags::RDONLY.bits());
    assert!(OpenFlags::WRONLY.bits() == rustix::fs::OFlags::WRONLY.bits());
    assert!(OpenFlags::RDWR.bits() == rustix::fs::OFlags::RDWR.bits());
    assert!(OpenFlags::CREAT.bits() == rustix::fs::OFlags::CREATE.bits());
    assert!(OpenFlags::EXCL.bits() == rustix::fs::OFlags::EXCL.bits());
    assert!(OpenFlags::TRUNC.bits() == rustix::fs::OFlags::TRUNC.bits());
    assert!(OpenFlags::APPEND.bits() == rustix::fs::OFlags::APPEND.bits());
};

/// `lstat` fields [`is_link_stat`] reads.
///
/// On POSIX only [`Self::mode`] (`st_mode`) is consulted. On Windows,
/// [`Self::file_attributes`] is `st_file_attributes` and [`Self::reparse_tag`]
/// is `st_reparse_tag` (zero when the reparse-point attribute is clear).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkStat {
    /// `st_mode`.
    pub mode: u32,
    /// Windows `st_file_attributes`. Ignored on POSIX.
    pub file_attributes: u32,
    /// Windows `st_reparse_tag`. Ignored on POSIX.
    pub reparse_tag: u32,
}

/// POSIX owner and mode, for the optional `st` argument.
///
/// On POSIX a provided snapshot is used instead of calling `stat` / `fstat`.
/// On Windows the snapshot is ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatSnapshot {
    /// `st_uid`. Compared with [`current_user_id`] on POSIX.
    pub uid: u32,
    /// `st_mode`. Group and other bits are `mode & 0o077`.
    pub mode: u32,
}

/// A path or an already open file.
///
/// A path is `stat` on POSIX (the call follows a symlink) and a no-follow open
/// on Windows. A file is `fstat` on POSIX and the handle's descriptor on Windows.
#[derive(Clone, Copy)]
pub enum SecureTarget<'a> {
    /// Look up this path.
    Path(&'a Path),
    /// Use this open file. The caller keeps ownership.
    File(&'a File),
}

/// Failure from a private-file primitive.
#[derive(Debug, thiserror::Error)]
pub enum SecureFileError {
    /// An operating-system error. [`Self::raw_os_error`] is the errno on POSIX
    /// and the Win32 code on Windows, when the source carries one. A Windows
    /// `ELOOP`, `EISDIR`, `ENOTDIR`, or owner `EPERM` uses the matching
    /// [`io::ErrorKind`] and does not invent a Win32 code: those values are
    /// CRT errnos, and a raw code here is a Win32 error.
    #[error("{message}")]
    Io {
        /// Path and operation, when this module has them.
        message: String,
        /// The underlying error. Its kind and raw code are unchanged.
        #[source]
        source: io::Error,
    },
    /// Windows `make_private_fd` was asked for a mode with group or other bits.
    #[error("make_private_fd only narrows access; mode {mode:#o} is not private")]
    ModeNotPrivate {
        /// The rejected mode.
        mode: u32,
    },
    /// [`PinnedDirectory::child`] was given something other than one component.
    #[error("not a single path component: {name:?}")]
    NotASingleComponent {
        /// The rejected component.
        name: String,
    },
    /// `pin_directory` was called on POSIX.
    #[error(
        "pin_directory is the Windows stand-in for O_DIRECTORY/dir_fd walks; POSIX callers use dir_fd directly"
    )]
    PinDirectoryUnsupported,
    /// [`PinnedDirectory::fd`] was used after [`PinnedDirectory::close`].
    #[error("pinned directory is closed")]
    PinnedDirectoryClosed,
}

impl SecureFileError {
    /// [`io::ErrorKind`] when this is an operating-system error.
    #[must_use]
    pub fn io_kind(&self) -> Option<io::ErrorKind> {
        match self {
            Self::Io { source, .. } => Some(source.kind()),
            _ => None,
        }
    }

    /// errno on POSIX, Win32 code on Windows, when the source has one.
    #[must_use]
    pub fn raw_os_error(&self) -> Option<i32> {
        match self {
            Self::Io { source, .. } => source.raw_os_error(),
            _ => None,
        }
    }
}

impl From<io::Error> for SecureFileError {
    fn from(source: io::Error) -> Self {
        let message = source.to_string();
        Self::Io { message, source }
    }
}

/// A directory chain held open so no component can be swapped.
///
/// On Windows every component from the volume root through `path` is open
/// without `FILE_SHARE_DELETE`, and none of them is a symlink or junction.
/// Dropping the value closes those handles. On POSIX the value cannot be
/// constructed: [`pin_directory`] returns [`SecureFileError::PinDirectoryUnsupported`].
#[must_use = "the directory stays pinned only while this value is held"]
pub struct PinnedDirectory {
    #[cfg(windows)]
    path: PathBuf,
    #[cfg(windows)]
    files: Vec<File>,
    #[cfg(unix)]
    _never: std::convert::Infallible,
}

impl fmt::Debug for PinnedDirectory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[cfg(windows)]
        {
            formatter
                .debug_struct("PinnedDirectory")
                .field("path", &self.path)
                .field("open_directories", &self.files.len())
                .finish()
        }
        #[cfg(unix)]
        {
            let _ = self;
            formatter.write_str("PinnedDirectory")
        }
    }
}

#[cfg(unix)]
impl PinnedDirectory {
    fn dead(&self) -> ! {
        match self._never {}
    }

    /// Absolute path that was pinned.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.dead()
    }

    /// Open file for the pinned leaf directory.
    pub fn fd(&self) -> Result<&File, SecureFileError> {
        self.dead()
    }

    /// `fstat` of the pinned leaf.
    pub fn stat(&self) -> Result<std::fs::Metadata, SecureFileError> {
        self.dead()
    }

    /// Path of one direct child. `name` must be a single component.
    pub fn child(&self, name: &str) -> Result<PathBuf, SecureFileError> {
        let _ = name;
        self.dead()
    }

    /// `open_nofollow` of one direct child.
    pub fn open(&self, name: &str, flags: OpenFlags, mode: u32) -> Result<File, SecureFileError> {
        let _ = (name, flags, mode);
        self.dead()
    }

    /// Close the pinned handles. Later [`Self::fd`] fails.
    pub fn close(&mut self) {
        self.dead()
    }
}

#[cfg(windows)]
impl PinnedDirectory {
    /// Absolute path that was pinned.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Open file for the pinned leaf directory.
    pub fn fd(&self) -> Result<&File, SecureFileError> {
        self.files
            .last()
            .ok_or(SecureFileError::PinnedDirectoryClosed)
    }

    /// Metadata of the pinned leaf (`fstat`).
    pub fn stat(&self) -> Result<std::fs::Metadata, SecureFileError> {
        self.fd()?.metadata().map_err(SecureFileError::from)
    }

    /// Path of one direct child. `name` must be a single component: not empty,
    /// not `.` or `..`, and not containing `/`, `\`, or `:`.
    pub fn child(&self, name: &str) -> Result<PathBuf, SecureFileError> {
        if name.is_empty()
            || name == "."
            || name == ".."
            || name.contains('/')
            || name.contains('\\')
            || name.contains(':')
        {
            return Err(SecureFileError::NotASingleComponent {
                name: name.to_owned(),
            });
        }
        Ok(self.path.join(name))
    }

    /// [`open_nofollow`] on one direct child.
    pub fn open(&self, name: &str, flags: OpenFlags, mode: u32) -> Result<File, SecureFileError> {
        let path = self.child(name)?;
        open_nofollow(&path, flags, mode)
    }

    /// Close the pinned handles. Later [`Self::fd`] fails.
    pub fn close(&mut self) {
        while self.files.pop().is_some() {}
    }
}

#[cfg(windows)]
impl Drop for PinnedDirectory {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(not(any(unix, windows)))]
compile_error!("securefile supports unix and windows");

/// Real user id, or the effective id when `effective` is true.
///
/// POSIX returns `getuid` / `geteuid`. Windows returns the process token's
/// user SID; `effective` does not change it, and reading the token can fail.
#[cfg(unix)]
#[must_use]
pub fn current_user_id(effective: bool) -> u32 {
    posix::current_user_id(effective)
}

/// Process token user SID. `effective` is ignored; reading the token can fail.
#[cfg(windows)]
pub fn current_user_id(effective: bool) -> Result<String, SecureFileError> {
    win::current_user_id(effective)
}

/// Whether `st` is a symlink or, on Windows, a junction.
///
/// POSIX is `S_ISLNK` and ignores the Windows fields. Windows also returns
/// true when the reparse-point attribute is set and the tag is
/// `IO_REPARSE_TAG_SYMLINK` (`0xA000000C`) or `IO_REPARSE_TAG_MOUNT_POINT`
/// (`0xA0000003`). Any other tag is an ordinary file or directory.
#[must_use]
pub fn is_link_stat(st: LinkStat) -> bool {
    #[cfg(unix)]
    {
        // `RawMode` is `u32` on Linux and `u16` on macOS.
        #[allow(clippy::unnecessary_cast, clippy::cast_possible_truncation)]
        let raw = st.mode as rustix::fs::RawMode;
        if rustix::fs::FileType::from_raw_mode(raw) == rustix::fs::FileType::Symlink {
            return true;
        }
        let _ = (st.file_attributes, st.reparse_tag);
        false
    }
    #[cfg(windows)]
    {
        classify_windows_link(st.mode, st.file_attributes, st.reparse_tag)
    }
}

/// Open `path` without following a symlink at the leaf.
///
/// `mode` is used when `CREAT` creates the file. Pass `0o777` for the usual
/// open default; the process umask still applies on POSIX. The returned file
/// is binary and non-inheritable.
///
/// A symlink or junction at the leaf fails with the same [`io::ErrorKind`] as
/// `ELOOP`. On Windows a directory fails with [`io::ErrorKind::IsADirectory`]. POSIX
/// `open` of a real directory succeeds. `CREAT|EXCL` on an existing file fails
/// with [`io::ErrorKind::AlreadyExists`].
pub fn open_nofollow(
    path: impl AsRef<Path>,
    flags: OpenFlags,
    mode: u32,
) -> Result<File, SecureFileError> {
    #[cfg(unix)]
    {
        posix::open_nofollow(path.as_ref(), flags, mode)
    }
    #[cfg(windows)]
    {
        win::open_nofollow(path.as_ref(), flags, mode)
    }
}

/// `fsync` a directory so a rename in it is durable.
///
/// POSIX is `open(O_RDONLY|O_DIRECTORY)` plus `fsync`, and it follows a
/// symlink to a directory. Windows opens the directory for write and calls
/// `FlushFileBuffers`.
pub fn fsync_directory(path: impl AsRef<Path>) -> Result<(), SecureFileError> {
    #[cfg(unix)]
    {
        posix::fsync_directory(path.as_ref())
    }
    #[cfg(windows)]
    {
        win::fsync_directory(path.as_ref())
    }
}

/// `None` when `target` is private, otherwise `"owner"` or `"mode"`.
///
/// POSIX compares the owner with [`current_user_id`]`(effective)` and then
/// checks `mode & 0o077`. A provided `st` skips the `stat` / `fstat` call.
/// Windows reads the live security descriptor and ignores `st` and `effective`.
pub fn private_problem(
    target: SecureTarget<'_>,
    st: Option<StatSnapshot>,
    effective: bool,
) -> Result<Option<PrivateProblem>, SecureFileError> {
    #[cfg(unix)]
    {
        posix::private_problem(target, st, effective)
    }
    #[cfg(windows)]
    {
        let _ = (st, effective);
        win::private_problem(target)
    }
}

/// Whether `target` is owned by the current user.
///
/// POSIX compares `st_uid` with [`current_user_id`]`(effective)`. Windows
/// compares the owner SID with the current user and the token's default owner,
/// ignores `st` and `effective`, and reports a symlink or junction as not owned.
pub fn owned_by_current_user(
    target: SecureTarget<'_>,
    st: Option<StatSnapshot>,
    effective: bool,
) -> Result<bool, SecureFileError> {
    #[cfg(unix)]
    {
        posix::owned_by_current_user(target, st, effective)
    }
    #[cfg(windows)]
    {
        let _ = (st, effective);
        win::owned_by_current_user(target)
    }
}

/// Restrict `path` to the current user.
///
/// `None` uses [`PRIVATE_DIR_MODE`] for a directory and [`PRIVATE_FILE_MODE`]
/// for anything else (`isdir` / the directory attribute). POSIX `chmod`s that
/// mode and follows a symlink. Windows installs the protected DACL, refuses a
/// symlink or junction with the same [`io::ErrorKind`] as `ELOOP`, and refuses a
/// foreign owner with [`io::ErrorKind::PermissionDenied`]. A Windows mode whose
/// group or other bits are set only toggles the read-only attribute.
pub fn make_private(path: impl AsRef<Path>, mode: Option<u32>) -> Result<(), SecureFileError> {
    #[cfg(unix)]
    {
        posix::make_private(path.as_ref(), mode)
    }
    #[cfg(windows)]
    {
        win::make_private(path.as_ref(), mode)
    }
}

/// `fchmod(file, mode)` on POSIX.
///
/// Windows reopens the same file object (no path lookup) and installs a
/// non-inheritable owner DACL. A mode with group or other bits is
/// [`SecureFileError::ModeNotPrivate`] and the DACL is left unchanged.
/// [`PRIVATE_FILE_MODE`] is the mode callers pass when they want `0o600`.
pub fn make_private_fd(file: &File, mode: u32) -> Result<(), SecureFileError> {
    #[cfg(unix)]
    {
        posix::make_private_fd(file, mode)
    }
    #[cfg(windows)]
    {
        win::make_private_fd(file, mode)
    }
}

/// Rename `src` over `dst`.
///
/// POSIX is `rename`. Windows is `MoveFileExW` with `MOVEFILE_REPLACE_EXISTING`.
/// A read-only target has that attribute cleared for the rename and restored
/// if the rename still fails. `ERROR_ACCESS_DENIED` and `ERROR_SHARING_VIOLATION`
/// are retried for about one second when `src` still exists; every other error
/// is returned as-is.
pub fn replace(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<(), SecureFileError> {
    #[cfg(unix)]
    {
        posix::replace(src.as_ref(), dst.as_ref())
    }
    #[cfg(windows)]
    {
        win::replace(src.as_ref(), dst.as_ref())
    }
}

/// Pin `path` on Windows. On POSIX this fails and does not open anything.
pub fn pin_directory(path: impl AsRef<Path>) -> Result<PinnedDirectory, SecureFileError> {
    #[cfg(unix)]
    {
        let _ = path;
        Err(SecureFileError::PinDirectoryUnsupported)
    }
    #[cfg(windows)]
    {
        win::pin_directory(path.as_ref())
    }
}

fn path_io(path: &Path, source: io::Error) -> SecureFileError {
    let detail = source.to_string();
    SecureFileError::Io {
        message: format!("{detail}: {}", path.display()),
        source,
    }
}

#[cfg(windows)]
fn with_path(path: &Path, source: io::Error, what: &str) -> SecureFileError {
    SecureFileError::Io {
        message: format!("{what}: {}", path.display()),
        source,
    }
}

/// Kind of `ELOOP`. `ErrorKind::FilesystemLoop` is still unstable, so the value
/// comes from the OS error std already maps, and the error we return does not
/// keep that raw code.
#[cfg(any(windows, test))]
fn filesystem_loop_kind() -> io::ErrorKind {
    #[cfg(unix)]
    let code = rustix::io::Errno::LOOP.raw_os_error();
    #[cfg(windows)]
    let code = windows_sys::Win32::Foundation::ERROR_CANT_RESOLVE_FILENAME as i32;
    io::Error::from_raw_os_error(code).kind()
}

#[cfg(windows)]
fn eloop(path: &Path) -> SecureFileError {
    let source = io::Error::new(filesystem_loop_kind(), "Too many levels of symbolic links");
    with_path(path, source, "Too many levels of symbolic links")
}

#[cfg(windows)]
fn eisdir(path: &Path) -> SecureFileError {
    let source = io::Error::new(io::ErrorKind::IsADirectory, "Is a directory");
    with_path(path, source, "Is a directory")
}

#[cfg(windows)]
fn enotdir(path: &Path) -> SecureFileError {
    let source = io::Error::new(io::ErrorKind::NotADirectory, "Not a directory");
    with_path(path, source, "Not a directory")
}

#[cfg(windows)]
fn eperm(path: Option<&Path>) -> SecureFileError {
    let source = io::Error::new(
        io::ErrorKind::PermissionDenied,
        "Operation not permitted: not the owner of the file",
    );
    let message = match path {
        Some(path) => format!(
            "Operation not permitted: not the owner of the file: {}",
            path.display()
        ),
        None => "Operation not permitted: not the owner of the file".to_owned(),
    };
    SecureFileError::Io { message, source }
}

#[cfg(unix)]
mod posix {
    use std::fs::{self, File};
    use std::io;
    use std::path::Path;

    use super::{
        OpenFlags, PRIVATE_DIR_MODE, PRIVATE_FILE_MODE, SecureFileError, SecureTarget,
        StatSnapshot, path_io,
    };

    pub(super) fn current_user_id(effective: bool) -> u32 {
        if effective {
            rustix::process::geteuid().as_raw()
        } else {
            rustix::process::getuid().as_raw()
        }
    }

    /// `uid_t` and `mode_t` are `u32` on Linux and `mode_t` is `u16` on macOS.
    fn as_u32(value: impl Into<u64>) -> u32 {
        u32::try_from(value.into()).unwrap_or(u32::MAX)
    }

    fn mode_bits(mode: u32) -> rustix::fs::Mode {
        // `RawMode` is `u32` on Linux and `u16` on macOS.
        #[allow(clippy::unnecessary_cast, clippy::cast_possible_truncation)]
        let raw = mode as rustix::fs::RawMode;
        rustix::fs::Mode::from_raw_mode(raw)
    }

    pub(super) fn open_nofollow(
        path: &Path,
        flags: OpenFlags,
        mode: u32,
    ) -> Result<File, SecureFileError> {
        let oflags = rustix::fs::OFlags::from_bits_truncate(flags.bits())
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC;
        let owned = rustix::fs::open(path, oflags, mode_bits(mode))
            .map_err(|err| path_io(path, io::Error::from(err)))?;
        Ok(File::from(owned))
    }

    pub(super) fn fsync_directory(path: &Path) -> Result<(), SecureFileError> {
        let owned = rustix::fs::open(
            path,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(|err| path_io(path, io::Error::from(err)))?;
        rustix::fs::fsync(&owned).map_err(|err| path_io(path, io::Error::from(err)))?;
        Ok(())
    }

    fn snapshot(
        target: SecureTarget<'_>,
        st: Option<StatSnapshot>,
    ) -> Result<StatSnapshot, SecureFileError> {
        if let Some(st) = st {
            return Ok(st);
        }
        let info = match target {
            SecureTarget::File(file) => rustix::fs::fstat(file)
                .map_err(|err| SecureFileError::from(io::Error::from(err)))?,
            SecureTarget::Path(path) => {
                rustix::fs::stat(path).map_err(|err| path_io(path, io::Error::from(err)))?
            }
        };
        Ok(StatSnapshot {
            uid: as_u32(info.st_uid),
            mode: as_u32(info.st_mode),
        })
    }

    pub(super) fn private_problem(
        target: SecureTarget<'_>,
        st: Option<StatSnapshot>,
        effective: bool,
    ) -> Result<Option<super::PrivateProblem>, SecureFileError> {
        let info = snapshot(target, st)?;
        if info.uid != current_user_id(effective) {
            return Ok(Some(super::PrivateProblem::Owner));
        }
        // `S_IMODE(mode) & 0o077`. The file-type and set-id bits are outside `0o077`.
        if info.mode & 0o077 != 0 {
            return Ok(Some(super::PrivateProblem::Mode));
        }
        Ok(None)
    }

    pub(super) fn owned_by_current_user(
        target: SecureTarget<'_>,
        st: Option<StatSnapshot>,
        effective: bool,
    ) -> Result<bool, SecureFileError> {
        let info = snapshot(target, st)?;
        Ok(info.uid == current_user_id(effective))
    }

    pub(super) fn make_private(path: &Path, mode: Option<u32>) -> Result<(), SecureFileError> {
        let mode = match mode {
            Some(mode) => mode,
            None => {
                if fs::metadata(path).is_ok_and(|meta| meta.is_dir()) {
                    PRIVATE_DIR_MODE
                } else {
                    PRIVATE_FILE_MODE
                }
            }
        };
        rustix::fs::chmod(path, mode_bits(mode))
            .map_err(|err| path_io(path, io::Error::from(err)))?;
        Ok(())
    }

    pub(super) fn make_private_fd(file: &File, mode: u32) -> Result<(), SecureFileError> {
        rustix::fs::fchmod(file, mode_bits(mode))
            .map_err(|err| SecureFileError::from(io::Error::from(err)))?;
        Ok(())
    }

    pub(super) fn replace(src: &Path, dst: &Path) -> Result<(), SecureFileError> {
        fs::rename(src, dst).map_err(|err| SecureFileError::Io {
            message: format!("replace {} -> {}: {err}", src.display(), dst.display()),
            source: err,
        })
    }
}

/// Windows link and DACL decisions. Linux tests run these directly. The Win32
/// calls that feed them are `cfg(windows)` and are not executed here.
#[cfg(any(windows, test))]
const IO_REPARSE_TAG_SYMLINK: u32 = 0xA000_000C;
#[cfg(any(windows, test))]
const IO_REPARSE_TAG_MOUNT_POINT: u32 = 0xA000_0003;
#[cfg(any(windows, test))]
const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
#[cfg(windows)]
const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x10;
#[cfg(any(windows, test))]
const S_IFMT: u32 = 0o170000;
#[cfg(any(windows, test))]
const S_IFLNK: u32 = 0o120000;
#[cfg(any(windows, test))]
const ACCESS_ALLOWED_ACE_TYPE: u8 = 0x0;
#[cfg(any(windows, test))]
const ACCESS_DENIED_ACE_TYPE: u8 = 0x1;
#[cfg(any(windows, test))]
const ACCESS_ALLOWED_CALLBACK_ACE_TYPE: u8 = 0x9;
#[cfg(any(windows, test))]
const INHERIT_ONLY_ACE: u8 = 0x8;
#[cfg(any(windows, test))]
const SID_SYSTEM: &str = "S-1-5-18";
#[cfg(any(windows, test))]
const SID_ADMINISTRATORS: &str = "S-1-5-32-544";
#[cfg(any(windows, test))]
const SID_OWNER_RIGHTS: &str = "S-1-3-4";

#[cfg(any(windows, test))]
#[derive(Clone)]
struct AceRecord {
    ace_type: u8,
    flags: u8,
    mask: u32,
    sid: String,
}

#[cfg(any(windows, test))]
fn is_link_reparse_tag(tag: u32) -> bool {
    tag == IO_REPARSE_TAG_SYMLINK || tag == IO_REPARSE_TAG_MOUNT_POINT
}

#[cfg(any(windows, test))]
fn classify_windows_link(mode: u32, file_attributes: u32, reparse_tag: u32) -> bool {
    if mode & S_IFMT == S_IFLNK {
        return true;
    }
    if file_attributes & FILE_ATTRIBUTE_REPARSE_POINT == 0 {
        return false;
    }
    is_link_reparse_tag(reparse_tag)
}

#[cfg(any(windows, test))]
fn is_deny_ace(ace_type: u8) -> bool {
    matches!(ace_type, 0x1 | 0x6 | 0xA | 0xC)
}

#[cfg(any(windows, test))]
fn is_sacl_ace(ace_type: u8) -> bool {
    matches!(
        ace_type,
        0x2 | 0x3 | 0x7 | 0x8 | 0xD | 0xE | 0xF | 0x10 | 0x11 | 0x12 | 0x13 | 0x14
    )
}

#[cfg(any(windows, test))]
fn sid_in(sid: &str, set: &[String]) -> bool {
    set.iter().any(|item| item == sid)
}

/// `None` when the descriptor is private.
///
/// `aces == None` is a NULL DACL (everyone). An empty slice is an empty DACL
/// (nobody granted). Inherit-only ACEs are ignored for a file and counted for
/// a directory. Deny and audit ACEs are skipped. Any other ACE type fails closed.
#[cfg(any(windows, test))]
fn acl_problem(
    owner_sid: &str,
    aces: Option<&[AceRecord]>,
    directory: bool,
    owner_sids: &[String],
    trusted_sids: &[String],
) -> Option<PrivateProblem> {
    if !sid_in(owner_sid, owner_sids) {
        return Some(PrivateProblem::Owner);
    }
    let Some(aces) = aces else {
        return Some(PrivateProblem::Mode);
    };
    for ace in aces {
        if ace.flags & INHERIT_ONLY_ACE != 0 && !directory {
            continue;
        }
        if is_deny_ace(ace.ace_type) || is_sacl_ace(ace.ace_type) {
            continue;
        }
        if ace.ace_type == ACCESS_ALLOWED_ACE_TYPE
            || ace.ace_type == ACCESS_ALLOWED_CALLBACK_ACE_TYPE
        {
            if ace.mask != 0 && !sid_in(&ace.sid, trusted_sids) {
                return Some(PrivateProblem::Mode);
            }
            continue;
        }
        return Some(PrivateProblem::Mode);
    }
    None
}

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "Win32 CreateFile, security-descriptor, and token calls have no safe wrapper in this workspace. The private/link decision is the safe acl_problem and classify_windows_link functions."
)]
mod win {
    use std::fs::File;
    use std::io;
    use std::mem::{self, ManuallyDrop, offset_of};
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::fs::MetadataExt;
    use std::os::windows::io::{AsRawHandle, FromRawHandle};
    use std::path::{Component, Path, PathBuf};
    use std::ptr;
    use std::sync::OnceLock;
    use std::time::Duration;

    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_ACCESS_DENIED, ERROR_SHARING_VIOLATION, GENERIC_READ, GENERIC_WRITE,
        HANDLE, INVALID_HANDLE_VALUE, LocalFree,
    };
    use windows_sys::Win32::Security::Authorization::{
        ConvertSidToStringSidW, GetSecurityInfo, SE_FILE_OBJECT, SetSecurityInfo,
    };
    use windows_sys::Win32::Security::{
        ACCESS_ALLOWED_ACE, ACCESS_ALLOWED_CALLBACK_ACE, ACCESS_DENIED_ACE, ACE_HEADER, ACL,
        ACL_REVISION, AddAccessAllowedAceEx, CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION,
        GetAce, GetLengthSid, GetTokenInformation, INHERIT_ONLY_ACE, InitializeAcl,
        OBJECT_INHERIT_ACE, OWNER_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION,
        PSECURITY_DESCRIPTOR, PSID, TOKEN_QUERY, TokenOwner, TokenUser,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        CREATE_NEW, CreateFileW, FILE_ALL_ACCESS, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_READONLY,
        FILE_ATTRIBUTE_TAG_INFO, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_GENERIC_WRITE, FILE_READ_ATTRIBUTES, FILE_READ_DATA, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_WRITE_DATA, FileAttributeTagInfo, FindClose,
        FindFirstFileW, FlushFileBuffers, GetFileInformationByHandleEx, MOVEFILE_REPLACE_EXISTING,
        MoveFileExW, OPEN_ALWAYS, OPEN_EXISTING, READ_CONTROL, ReOpenFile, SYNCHRONIZE,
        WIN32_FIND_DATAW, WRITE_DAC,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};
    use windows_sys::core::PWSTR;

    use super::{
        AceRecord, OpenFlags, PinnedDirectory, PrivateProblem, SID_ADMINISTRATORS,
        SID_OWNER_RIGHTS, SID_SYSTEM, SecureFileError, SecureTarget, acl_problem, eisdir, eloop,
        enotdir, eperm, is_link_reparse_tag, path_io,
    };

    const S_IWRITE: u32 = 0o200;
    const SHARE_ALL: u32 = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
    /// `SECURITY_MAX_SID_SIZE`.
    const MAX_SID_BYTES: usize = 68;
    const MAX_TOKEN_BYTES: u32 = 65_536;

    const _: () = {
        use windows_sys::Win32::System::SystemServices::{
            ACCESS_ALLOWED_ACE_TYPE, ACCESS_ALLOWED_CALLBACK_ACE_TYPE, ACCESS_DENIED_ACE_TYPE,
            IO_REPARSE_TAG_MOUNT_POINT, IO_REPARSE_TAG_SYMLINK,
        };
        assert!(super::IO_REPARSE_TAG_SYMLINK == IO_REPARSE_TAG_SYMLINK);
        assert!(super::IO_REPARSE_TAG_MOUNT_POINT == IO_REPARSE_TAG_MOUNT_POINT);
        assert!(super::ACCESS_ALLOWED_ACE_TYPE as u32 == ACCESS_ALLOWED_ACE_TYPE);
        assert!(super::ACCESS_DENIED_ACE_TYPE as u32 == ACCESS_DENIED_ACE_TYPE);
        assert!(super::ACCESS_ALLOWED_CALLBACK_ACE_TYPE as u32 == ACCESS_ALLOWED_CALLBACK_ACE_TYPE);
        assert!(
            super::FILE_ATTRIBUTE_REPARSE_POINT
                == windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT
        );
        assert!(
            super::FILE_ATTRIBUTE_DIRECTORY
                == windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_DIRECTORY
        );
        assert!(FILE_ATTRIBUTE_READONLY == 1);
        assert!(FILE_ATTRIBUTE_NORMAL == 0x80);
        assert!(offset_of!(ACCESS_ALLOWED_ACE, SidStart) == 8);
        assert!(
            offset_of!(ACCESS_ALLOWED_ACE, SidStart) == offset_of!(ACCESS_DENIED_ACE, SidStart)
        );
        assert!(
            offset_of!(ACCESS_ALLOWED_ACE, SidStart)
                == offset_of!(ACCESS_ALLOWED_CALLBACK_ACE, SidStart)
        );
        assert!(FILE_GENERIC_WRITE == 0x0012_0116);
        assert!(FILE_WRITE_DATA == 2);
        assert!(GENERIC_READ == 0x8000_0000);
        assert!(GENERIC_WRITE == 0x4000_0000);
        assert!(FILE_ALL_ACCESS == 0x001F_01FF);
        assert!(READ_CONTROL == 0x0002_0000);
        assert!(WRITE_DAC == 0x0004_0000);
        assert!(FILE_FLAG_OPEN_REPARSE_POINT == 0x0020_0000);
        assert!(FILE_FLAG_BACKUP_SEMANTICS == 0x0200_0000);
        assert!(ERROR_ACCESS_DENIED == 5);
        assert!(ERROR_SHARING_VIOLATION == 32);
        assert!(MOVEFILE_REPLACE_EXISTING == 1);
        assert!(CREATE_NEW == 1 && OPEN_EXISTING == 3 && OPEN_ALWAYS == 4);
        assert!(OBJECT_INHERIT_ACE == 1 && CONTAINER_INHERIT_ACE == 2);
        assert!(INHERIT_ONLY_ACE == 8);
        assert!(super::INHERIT_ONLY_ACE as u32 == INHERIT_ONLY_ACE);
        assert!(ACL_REVISION == 2);
        assert!(TOKEN_QUERY == 8 && TokenUser == 1 && TokenOwner == 4);
        assert!(SE_FILE_OBJECT == 1);
        assert!(OWNER_SECURITY_INFORMATION == 1 && DACL_SECURITY_INFORMATION == 4);
        assert!(PROTECTED_DACL_SECURITY_INFORMATION == 0x8000_0000);
        assert!(
            offset_of!(ACCESS_ALLOWED_ACE, SidStart) == offset_of!(ACE_HEADER, AceType) + 8
                || offset_of!(ACCESS_ALLOWED_ACE, SidStart) == 8
        );
    };

    struct WinHandle(HANDLE);

    impl WinHandle {
        fn into_file(self) -> File {
            let handle = ManuallyDrop::new(self);
            // SAFETY: CreateFileW/ReOpenFile returned this handle. ManuallyDrop
            // skips CloseHandle so the File is the sole owner.
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

    struct FindHandle(HANDLE);

    impl Drop for FindHandle {
        fn drop(&mut self) {
            if !self.0.is_null() && self.0 != INVALID_HANDLE_VALUE {
                // SAFETY: FindFirstFileW returned this search handle.
                unsafe {
                    FindClose(self.0);
                }
            }
        }
    }

    struct LocalFreeGuard(PSECURITY_DESCRIPTOR);

    impl Drop for LocalFreeGuard {
        fn drop(&mut self) {
            if !self.0.is_null() {
                // SAFETY: GetSecurityInfo allocated this descriptor with LocalAlloc.
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
                // SAFETY: ConvertSidToStringSidW allocated this wide string.
                unsafe {
                    LocalFree(self.0.cast());
                }
            }
        }
    }

    struct OwnedSid {
        data: [u8; MAX_SID_BYTES],
        len: usize,
    }

    impl OwnedSid {
        fn as_psid(&self) -> PSID {
            self.data.as_ptr().cast_mut().cast()
        }
    }

    struct UserSid {
        text: String,
        bytes: OwnedSid,
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

    fn wide_to_string(ptr: *const u16) -> io::Result<String> {
        if ptr.is_null() {
            return Ok(String::new());
        }
        let mut len = 0usize;
        // SAFETY: ConvertSidToStringSidW returns a NUL-terminated wide string,
        // freed by the caller after this read. The scan stops at 256 units.
        let units = unsafe {
            while len < 256 && *ptr.add(len) != 0 {
                len += 1;
            }
            if len == 256 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "SID string is unexpectedly long",
                ));
            }
            std::slice::from_raw_parts(ptr, len)
        };
        String::from_utf16(units)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "SID string is not Unicode"))
    }

    fn sid_to_string(psid: PSID) -> io::Result<String> {
        if psid.is_null() {
            return Ok(String::new());
        }
        let mut raw: PWSTR = ptr::null_mut();
        // SAFETY: psid is a SID returned by GetTokenInformation or GetSecurityInfo
        // and still allocated. The wide string is freed by LocalWide.
        let ok = unsafe { ConvertSidToStringSidW(psid, &mut raw) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        let _free = LocalWide(raw);
        wide_to_string(raw)
    }

    fn copy_sid(psid: PSID) -> io::Result<OwnedSid> {
        if psid.is_null() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "SID is null"));
        }
        // SAFETY: psid is a live SID. GetLengthSid only reads it.
        let len = unsafe { GetLengthSid(psid) } as usize;
        if len == 0 || len > MAX_SID_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "SID length is not usable",
            ));
        }
        let mut owned = OwnedSid {
            data: [0; MAX_SID_BYTES],
            len,
        };
        // SAFETY: GetLengthSid reported `len` bytes, which fit in `owned.data`.
        // The source and destination do not overlap.
        unsafe {
            ptr::copy_nonoverlapping(psid.cast::<u8>(), owned.data.as_mut_ptr(), len);
        }
        Ok(owned)
    }

    fn read_token_sid(class: i32) -> io::Result<UserSid> {
        let mut token: HANDLE = ptr::null_mut();
        // SAFETY: GetCurrentProcess is the pseudo-handle. OpenProcessToken
        // writes a real handle we close via WinHandle.
        let opened = unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) };
        if opened == 0 {
            return Err(io::Error::last_os_error());
        }
        let _token = WinHandle(token);
        let mut needed = 0u32;
        // SAFETY: a null buffer with a zero length is the size query. The
        // BOOL failure is expected; `needed` is the result.
        unsafe {
            GetTokenInformation(token, class, ptr::null_mut(), 0, &mut needed);
        }
        if needed == 0 || needed > MAX_TOKEN_BYTES {
            return Err(io::Error::last_os_error());
        }
        let mut buffer = vec![0u64; (needed as usize).div_ceil(8)];
        // SAFETY: `buffer` is large enough for `needed` and aligned for a pointer.
        let ok = unsafe {
            GetTokenInformation(
                token,
                class,
                buffer.as_mut_ptr().cast(),
                (buffer.len() * mem::size_of::<u64>()) as u32,
                &mut needed,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        // TOKEN_USER and TOKEN_OWNER both start with the SID pointer.
        // SAFETY: GetTokenInformation wrote that structure at the start of
        // `buffer`. The SID remains valid until `buffer` drops, which is after
        // the copy below.
        let psid = unsafe { *buffer.as_ptr().cast::<PSID>() };
        let text = sid_to_string(psid)?;
        let bytes = copy_sid(psid)?;
        Ok(UserSid { text, bytes })
    }

    #[cfg(test)]
    pub(super) fn handle_is_inheritable(file: &File) -> io::Result<bool> {
        use windows_sys::Win32::Foundation::{GetHandleInformation, HANDLE_FLAG_INHERIT};
        let mut flags = 0u32;
        // SAFETY: `file` is open. GetHandleInformation only reads the inherit bit.
        let ok = unsafe { GetHandleInformation(file.as_raw_handle(), &mut flags) };
        if ok == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(flags & HANDLE_FLAG_INHERIT != 0)
        }
    }

    fn user_sid() -> io::Result<&'static UserSid> {
        static CACHE: OnceLock<UserSid> = OnceLock::new();
        if let Some(sid) = CACHE.get() {
            return Ok(sid);
        }
        let sid = read_token_sid(TokenUser)?;
        Ok(CACHE.get_or_init(|| sid))
    }

    fn owner_sids() -> io::Result<&'static Vec<String>> {
        static CACHE: OnceLock<Vec<String>> = OnceLock::new();
        if let Some(sids) = CACHE.get() {
            return Ok(sids);
        }
        let mut sids = vec![user_sid()?.text.clone()];
        if let Ok(owner) = read_token_sid(TokenOwner)
            && !sids.iter().any(|sid| sid == &owner.text)
        {
            sids.push(owner.text);
        }
        Ok(CACHE.get_or_init(|| sids))
    }

    fn trusted_sids() -> io::Result<&'static Vec<String>> {
        static CACHE: OnceLock<Vec<String>> = OnceLock::new();
        if let Some(sids) = CACHE.get() {
            return Ok(sids);
        }
        let mut sids = owner_sids()?.clone();
        for extra in [SID_SYSTEM, SID_ADMINISTRATORS, SID_OWNER_RIGHTS] {
            if !sids.iter().any(|sid| sid == extra) {
                sids.push(extra.to_owned());
            }
        }
        Ok(CACHE.get_or_init(|| sids))
    }

    pub(super) fn current_user_id(_effective: bool) -> Result<String, SecureFileError> {
        user_sid()
            .map(|sid| sid.text.clone())
            .map_err(SecureFileError::from)
    }

    fn create_file(
        path: &Path,
        access: u32,
        share: u32,
        disposition: u32,
        flags: u32,
    ) -> io::Result<WinHandle> {
        let wide = wide_path(path)?;
        // SAFETY: `wide` is NUL-terminated. A null security descriptor makes
        // the handle non-inheritable. Failure returns INVALID_HANDLE_VALUE.
        let handle = unsafe {
            CreateFileW(
                wide.as_ptr(),
                access,
                share,
                ptr::null(),
                disposition,
                flags,
                ptr::null_mut(),
            )
        };
        if handle.is_null() || handle == INVALID_HANDLE_VALUE {
            Err(io::Error::last_os_error())
        } else {
            Ok(WinHandle(handle))
        }
    }

    fn attributes_and_tag(handle: HANDLE) -> io::Result<(u32, u32)> {
        let mut info = FILE_ATTRIBUTE_TAG_INFO::default();
        // SAFETY: `handle` is open and `info` is writable for the call.
        let ok = unsafe {
            GetFileInformationByHandleEx(
                handle,
                FileAttributeTagInfo,
                (&mut info as *mut FILE_ATTRIBUTE_TAG_INFO).cast(),
                mem::size_of::<FILE_ATTRIBUTE_TAG_INFO>() as u32,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        if info.FileAttributes & super::FILE_ATTRIBUTE_REPARSE_POINT == 0 {
            Ok((info.FileAttributes, 0))
        } else {
            Ok((info.FileAttributes, info.ReparseTag))
        }
    }

    struct Found {
        attrs: u32,
        tag: u32,
    }

    fn find_attributes(path: &Path) -> io::Result<Found> {
        let wide = wide_path(path)?;
        let mut data = WIN32_FIND_DATAW::default();
        // SAFETY: `wide` is NUL-terminated and `data` is writable.
        let handle = unsafe { FindFirstFileW(wide.as_ptr(), &mut data) };
        if handle.is_null() || handle == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        let _close = FindHandle(handle);
        let attrs = data.dwFileAttributes;
        let tag = if attrs & super::FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            data.dwReserved0
        } else {
            0
        };
        Ok(Found { attrs, tag })
    }

    fn access_mode(flags: OpenFlags) -> u32 {
        flags.bits()
            & (OpenFlags::RDONLY.bits() | OpenFlags::WRONLY.bits() | OpenFlags::RDWR.bits())
    }

    fn access_mask(flags: OpenFlags) -> u32 {
        let mode = access_mode(flags);
        if mode == OpenFlags::WRONLY.bits() {
            GENERIC_WRITE
        } else if mode == OpenFlags::RDWR.bits() {
            GENERIC_READ | GENERIC_WRITE
        } else {
            GENERIC_READ
        }
    }

    fn append_adjusted(access: u32) -> u32 {
        (access & !GENERIC_WRITE) | (FILE_GENERIC_WRITE & !FILE_WRITE_DATA)
    }

    pub(super) fn open_nofollow(
        path: &Path,
        flags: OpenFlags,
        mode: u32,
    ) -> Result<File, SecureFileError> {
        let mut access = access_mask(flags);
        if flags.contains(OpenFlags::APPEND)
            && access & GENERIC_WRITE != 0
            && !flags.contains(OpenFlags::TRUNC)
        {
            access = append_adjusted(access);
        }
        let disposition = if flags.contains(OpenFlags::CREAT) && flags.contains(OpenFlags::EXCL) {
            CREATE_NEW
        } else if flags.contains(OpenFlags::CREAT) {
            OPEN_ALWAYS
        } else {
            OPEN_EXISTING
        };
        let attributes = if flags.contains(OpenFlags::CREAT) && mode & S_IWRITE == 0 {
            windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_READONLY
        } else {
            FILE_ATTRIBUTE_NORMAL
        };
        let handle = match create_file(
            path,
            access,
            SHARE_ALL,
            disposition,
            attributes | FILE_FLAG_OPEN_REPARSE_POINT,
        ) {
            Ok(handle) => handle,
            Err(err) if err.raw_os_error() == Some(ERROR_ACCESS_DENIED as i32) => {
                if let Ok(found) = find_attributes(path) {
                    if super::classify_windows_link(0, found.attrs, found.tag) {
                        return Err(eloop(path));
                    }
                    if found.attrs & super::FILE_ATTRIBUTE_DIRECTORY != 0 {
                        return Err(eisdir(path));
                    }
                }
                return Err(path_io(path, err));
            }
            Err(err) => return Err(path_io(path, err)),
        };
        let file = handle.into_file();
        let (attrs, tag) =
            attributes_and_tag(file.as_raw_handle()).map_err(|err| path_io(path, err))?;
        if is_link_reparse_tag(tag) {
            return Err(eloop(path));
        }
        if attrs & super::FILE_ATTRIBUTE_DIRECTORY != 0 {
            return Err(eisdir(path));
        }
        if flags.contains(OpenFlags::TRUNC) && access_mode(flags) != OpenFlags::RDONLY.bits() {
            file.set_len(0).map_err(|err| path_io(path, err))?;
        }
        Ok(file)
    }

    pub(super) fn fsync_directory(path: &Path) -> Result<(), SecureFileError> {
        let handle = create_file(
            path,
            GENERIC_WRITE,
            SHARE_ALL,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS,
        )
        .map_err(|err| path_io(path, err))?;
        // SAFETY: `handle` is open for write. FlushFileBuffers is the directory
        // durability call. The error is captured before Drop closes the handle.
        let ok = unsafe { FlushFileBuffers(handle.0) };
        if ok == 0 {
            let err = io::Error::last_os_error();
            drop(handle);
            return Err(path_io(path, err));
        }
        Ok(())
    }

    struct SecurityInfo {
        owner: String,
        aces: Option<Vec<AceRecord>>,
    }

    fn read_security(handle: HANDLE) -> io::Result<SecurityInfo> {
        let mut owner: PSID = ptr::null_mut();
        let mut dacl: *mut ACL = ptr::null_mut();
        let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
        // SAFETY: `handle` is open with at least READ_CONTROL. On success the
        // descriptor is LocalAlloc'd and freed by LocalFreeGuard. Owner and
        // DACL pointers address memory inside that descriptor.
        let code = unsafe {
            GetSecurityInfo(
                handle,
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                &mut owner,
                ptr::null_mut(),
                &mut dacl,
                ptr::null_mut(),
                &mut descriptor,
            )
        };
        if code != 0 {
            return Err(io::Error::from_raw_os_error(code as i32));
        }
        let _descriptor = LocalFreeGuard(descriptor);
        let owner_sid = sid_to_string(owner)?;
        if dacl.is_null() {
            return Ok(SecurityInfo {
                owner: owner_sid,
                aces: None,
            });
        }
        // SAFETY: a non-null DACL from GetSecurityInfo points at an ACL header
        // inside the descriptor, which is still allocated.
        let count = unsafe { (*dacl).AceCount };
        let mut aces = Vec::with_capacity(usize::from(count));
        for index in 0..count {
            let mut pace: *mut std::ffi::c_void = ptr::null_mut();
            // SAFETY: `index` is below AceCount and the descriptor is alive.
            let ok = unsafe { GetAce(dacl, u32::from(index), &mut pace) };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            if pace.is_null() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "ACE pointer is null",
                ));
            }
            // SAFETY: GetAce wrote a pointer to an ACE in the ACL. The header
            // is the first field of every ACE. SidStart of ACCESS_ALLOWED_ACE
            // is the SID for the three ACE types parsed below; those layouts
            // share that offset (asserted above).
            let header = unsafe { &*pace.cast::<ACE_HEADER>() };
            let ace_type = header.AceType;
            let flags = header.AceFlags;
            let mut mask = 0u32;
            let mut sid = String::new();
            if ace_type == super::ACCESS_ALLOWED_ACE_TYPE
                || ace_type == super::ACCESS_DENIED_ACE_TYPE
                || ace_type == super::ACCESS_ALLOWED_CALLBACK_ACE_TYPE
            {
                let ace = unsafe { &*pace.cast::<ACCESS_ALLOWED_ACE>() };
                mask = ace.Mask;
                let sid_ptr = unsafe {
                    pace.cast::<u8>()
                        .add(offset_of!(ACCESS_ALLOWED_ACE, SidStart))
                        .cast()
                };
                sid = sid_to_string(sid_ptr)?;
            }
            aces.push(AceRecord {
                ace_type,
                flags,
                mask,
                sid,
            });
        }
        Ok(SecurityInfo {
            owner: owner_sid,
            aces: Some(aces),
        })
    }

    fn map_io(path: Option<&Path>, err: io::Error) -> SecureFileError {
        match path {
            Some(path) => path_io(path, err),
            None => SecureFileError::from(err),
        }
    }

    fn apply_private_acl(
        handle: HANDLE,
        directory: bool,
        path: Option<&Path>,
    ) -> Result<(), SecureFileError> {
        let info = read_security(handle).map_err(|err| map_io(path, err))?;
        let owners = owner_sids().map_err(SecureFileError::from)?;
        let trusted = trusted_sids().map_err(SecureFileError::from)?;
        match acl_problem(
            &info.owner,
            info.aces.as_deref(),
            directory,
            owners,
            trusted,
        ) {
            None => Ok(()),
            Some(PrivateProblem::Owner) => Err(eperm(path)),
            Some(PrivateProblem::Mode) => {
                write_private_dacl(handle, directory).map_err(|err| map_io(path, err))
            }
        }
    }

    fn write_private_dacl(handle: HANDLE, directory: bool) -> io::Result<()> {
        let sid = user_sid()?;
        let sid_len = sid.bytes.len;
        let mut size = mem::size_of::<ACL>() + mem::size_of::<ACCESS_ALLOWED_ACE>()
            - mem::size_of::<u32>()
            + sid_len;
        size = (size + 3) & !3;
        let mut buffer = vec![0u64; size.div_ceil(8)];
        let acl: *mut ACL = buffer.as_mut_ptr().cast();
        // SAFETY: `buffer` holds at least `size` bytes. InitializeAcl writes the
        // ACL header. AddAccessAllowedAceEx copies the SID into that buffer.
        // SetSecurityInfo reads it before these locals are dropped.
        let ok = unsafe { InitializeAcl(acl, size as u32, ACL_REVISION) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        let inherit = if directory {
            OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE
        } else {
            0
        };
        let ok = unsafe {
            AddAccessAllowedAceEx(
                acl,
                ACL_REVISION,
                inherit,
                FILE_ALL_ACCESS,
                sid.bytes.as_psid(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        let code = unsafe {
            SetSecurityInfo(
                handle,
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                ptr::null_mut(),
                ptr::null_mut(),
                acl,
                ptr::null_mut(),
            )
        };
        if code != 0 {
            return Err(io::Error::from_raw_os_error(code as i32));
        }
        Ok(())
    }

    enum Inspected<'a> {
        Link,
        Borrowed { file: &'a File, directory: bool },
        Owned { file: File, directory: bool },
    }

    fn inspect(target: SecureTarget<'_>) -> Result<Inspected<'_>, SecureFileError> {
        match target {
            SecureTarget::File(file) => {
                let (attrs, _tag) =
                    attributes_and_tag(file.as_raw_handle()).map_err(SecureFileError::from)?;
                Ok(Inspected::Borrowed {
                    file,
                    directory: attrs & super::FILE_ATTRIBUTE_DIRECTORY != 0,
                })
            }
            SecureTarget::Path(path) => {
                let opened = create_file(
                    path,
                    READ_CONTROL | FILE_READ_ATTRIBUTES,
                    SHARE_ALL,
                    OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL
                        | FILE_FLAG_OPEN_REPARSE_POINT
                        | FILE_FLAG_BACKUP_SEMANTICS,
                )
                .map_err(|err| path_io(path, err))?;
                let file = opened.into_file();
                let (attrs, tag) =
                    attributes_and_tag(file.as_raw_handle()).map_err(|err| path_io(path, err))?;
                if is_link_reparse_tag(tag) {
                    return Ok(Inspected::Link);
                }
                Ok(Inspected::Owned {
                    directory: attrs & super::FILE_ATTRIBUTE_DIRECTORY != 0,
                    file,
                })
            }
        }
    }

    fn inspected_parts(inspected: &Inspected<'_>) -> Option<(HANDLE, bool)> {
        match inspected {
            Inspected::Link => None,
            Inspected::Borrowed { file, directory } => Some((file.as_raw_handle(), *directory)),
            Inspected::Owned { file, directory } => Some((file.as_raw_handle(), *directory)),
        }
    }

    pub(super) fn private_problem(
        target: SecureTarget<'_>,
    ) -> Result<Option<PrivateProblem>, SecureFileError> {
        let inspected = inspect(target)?;
        let Some((handle, directory)) = inspected_parts(&inspected) else {
            return Ok(Some(PrivateProblem::Owner));
        };
        let info = read_security(handle).map_err(SecureFileError::from)?;
        let owners = owner_sids().map_err(SecureFileError::from)?;
        let trusted = trusted_sids().map_err(SecureFileError::from)?;
        Ok(acl_problem(
            &info.owner,
            info.aces.as_deref(),
            directory,
            owners,
            trusted,
        ))
    }

    pub(super) fn owned_by_current_user(target: SecureTarget<'_>) -> Result<bool, SecureFileError> {
        let inspected = inspect(target)?;
        let Some((handle, _directory)) = inspected_parts(&inspected) else {
            return Ok(false);
        };
        let info = read_security(handle).map_err(SecureFileError::from)?;
        let owners = owner_sids().map_err(SecureFileError::from)?;
        Ok(owners.iter().any(|sid| sid == &info.owner))
    }

    fn set_readonly(path: &Path, readonly: bool) -> io::Result<()> {
        let mut perms = std::fs::symlink_metadata(path)?.permissions();
        perms.set_readonly(readonly);
        std::fs::set_permissions(path, perms)
    }

    pub(super) fn make_private(path: &Path, mode: Option<u32>) -> Result<(), SecureFileError> {
        if let Some(mode) = mode
            && mode & 0o077 != 0
        {
            return set_readonly(path, mode & S_IWRITE == 0).map_err(|err| path_io(path, err));
        }
        let opened = create_file(
            path,
            READ_CONTROL | WRITE_DAC | FILE_READ_ATTRIBUTES,
            SHARE_ALL,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS,
        )
        .map_err(|err| path_io(path, err))?;
        let file = opened.into_file();
        let (attrs, tag) =
            attributes_and_tag(file.as_raw_handle()).map_err(|err| path_io(path, err))?;
        if is_link_reparse_tag(tag) {
            return Err(eloop(path));
        }
        let directory = attrs & super::FILE_ATTRIBUTE_DIRECTORY != 0;
        apply_private_acl(file.as_raw_handle(), directory, Some(path))?;
        if let Some(mode) = mode
            && mode & S_IWRITE == 0
        {
            set_readonly(path, true).map_err(|err| path_io(path, err))?;
        }
        Ok(())
    }

    fn reopen(handle: HANDLE, access: u32, share: u32, flags: u32) -> io::Result<WinHandle> {
        // SAFETY: `handle` belongs to an open File the caller still holds.
        // ReOpenFile returns a new handle to the same file object.
        let reopened = unsafe { ReOpenFile(handle, access, share, flags) };
        if reopened.is_null() || reopened == INVALID_HANDLE_VALUE {
            Err(io::Error::last_os_error())
        } else {
            Ok(WinHandle(reopened))
        }
    }

    pub(super) fn make_private_fd(file: &File, mode: u32) -> Result<(), SecureFileError> {
        if mode & 0o077 != 0 {
            return Err(SecureFileError::ModeNotPrivate { mode });
        }
        let reopened = reopen(file.as_raw_handle(), READ_CONTROL | WRITE_DAC, SHARE_ALL, 0)
            .map_err(SecureFileError::from)?;
        apply_private_acl(reopened.0, false, None)
    }

    fn move_replace(src: &Path, dst: &Path) -> io::Result<()> {
        let src_w = wide_path(src)?;
        let dst_w = wide_path(dst)?;
        // SAFETY: both paths are NUL-terminated. MOVEFILE_REPLACE_EXISTING is
        // the flag os.replace passes to MoveFileExW.
        let ok = unsafe { MoveFileExW(src_w.as_ptr(), dst_w.as_ptr(), MOVEFILE_REPLACE_EXISTING) };
        if ok == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    fn replace_io(src: &Path, dst: &Path, err: io::Error) -> SecureFileError {
        SecureFileError::Io {
            message: format!("replace {} -> {}: {err}", src.display(), dst.display()),
            source: err,
        }
    }

    fn is_permission(err: &io::Error) -> bool {
        err.kind() == io::ErrorKind::PermissionDenied
            || matches!(
                err.raw_os_error(),
                Some(code) if code == ERROR_ACCESS_DENIED as i32 || code == ERROR_SHARING_VIOLATION as i32
            )
    }

    fn is_transient(err: &SecureFileError) -> bool {
        matches!(
            err.raw_os_error(),
            Some(code) if code == ERROR_ACCESS_DENIED as i32 || code == ERROR_SHARING_VIOLATION as i32
        )
    }

    fn replace_once(src: &Path, dst: &Path) -> Result<(), SecureFileError> {
        match move_replace(src, dst) {
            Ok(()) => return Ok(()),
            Err(err) if is_permission(&err) => {
                let info = std::fs::symlink_metadata(dst).ok();
                let readonly = info.as_ref().is_some_and(|meta| {
                    !meta.is_dir()
                        && meta.file_attributes()
                            & windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_READONLY
                            != 0
                });
                if !readonly {
                    return Err(replace_io(src, dst, err));
                }
            }
            Err(err) => return Err(replace_io(src, dst, err)),
        }
        set_readonly(dst, false).map_err(|err| path_io(dst, err))?;
        match move_replace(src, dst) {
            Ok(()) => Ok(()),
            Err(err) => {
                let _ = set_readonly(dst, true);
                Err(replace_io(src, dst, err))
            }
        }
    }

    pub(super) fn replace(src: &Path, dst: &Path) -> Result<(), SecureFileError> {
        const REPLACE_RETRY_DELAYS: [Duration; 8] = [
            Duration::from_millis(5),
            Duration::from_millis(10),
            Duration::from_millis(20),
            Duration::from_millis(50),
            Duration::from_millis(100),
            Duration::from_millis(200),
            Duration::from_millis(300),
            Duration::from_millis(300),
        ];
        for delay in REPLACE_RETRY_DELAYS {
            match replace_once(src, dst) {
                Ok(()) => return Ok(()),
                Err(err) if is_transient(&err) && std::fs::symlink_metadata(src).is_ok() => {
                    std::thread::sleep(delay);
                }
                Err(err) => return Err(err),
            }
        }
        replace_once(src, dst)
    }

    fn pin_stages(path: &Path) -> Vec<PathBuf> {
        let mut stages = Vec::new();
        let mut current = PathBuf::new();
        for component in path.components() {
            match component {
                Component::Prefix(prefix) => current.push(prefix.as_os_str()),
                Component::RootDir => {
                    current.push(component);
                    stages.push(current.clone());
                }
                Component::Normal(part) => {
                    current.push(part);
                    stages.push(current.clone());
                }
                Component::CurDir => {}
                Component::ParentDir => {
                    current.pop();
                }
            }
        }
        stages
    }

    pub(super) fn pin_directory(path: &Path) -> Result<PinnedDirectory, SecureFileError> {
        let absolute = std::path::absolute(path).map_err(|err| path_io(path, err))?;
        let stages = pin_stages(&absolute);
        if stages.is_empty() {
            return Err(SecureFileError::from(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot pin a path with no directory",
            )));
        }
        let mut pinned = PinnedDirectory {
            path: absolute,
            files: Vec::with_capacity(stages.len()),
        };
        for stage in stages {
            let opened = create_file(
                &stage,
                FILE_READ_DATA | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                OPEN_EXISTING,
                FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS,
            )
            .map_err(|err| path_io(&stage, err))?;
            let file = opened.into_file();
            let (attrs, tag) =
                attributes_and_tag(file.as_raw_handle()).map_err(|err| path_io(&stage, err))?;
            if is_link_reparse_tag(tag) {
                pinned.files.push(file);
                return Err(eloop(&stage));
            }
            if attrs & super::FILE_ATTRIBUTE_DIRECTORY == 0 {
                pinned.files.push(file);
                return Err(enotdir(&stage));
            }
            pinned.files.push(file);
        }
        Ok(pinned)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::fs;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::Path;

    use super::*;

    fn assert_kind(err: &SecureFileError, kind: std::io::ErrorKind) {
        assert_eq!(err.io_kind(), Some(kind), "{err}");
    }

    fn symlink_file(target: &Path, link: &Path) -> bool {
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(target, link).expect("symlink");
            true
        }
        #[cfg(windows)]
        {
            std::os::windows::fs::symlink_file(target, link).is_ok()
        }
    }

    #[test]
    fn private_modes_are_owner_only() {
        assert_eq!(IS_WINDOWS, cfg!(windows));
        assert_eq!(PRIVATE_FILE_MODE, 0o600);
        assert_eq!(PRIVATE_DIR_MODE, 0o700);
        assert_eq!(PRIVATE_FILE_MODE & 0o077, 0);
        assert_eq!(PRIVATE_DIR_MODE & 0o077, 0);
    }

    #[test]
    fn make_private_clears_private_problem_on_a_normal_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        fs::write(&path, b"x").unwrap();
        make_private(&path, None).unwrap();
        assert_eq!(
            private_problem(SecureTarget::Path(&path), None, false).unwrap(),
            None
        );
        assert!(owned_by_current_user(SecureTarget::Path(&path), None, false).unwrap());
    }

    #[test]
    fn open_nofollow_round_trip_is_binary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data");
        let mut file =
            open_nofollow(&path, OpenFlags::RDWR | OpenFlags::CREAT, PRIVATE_FILE_MODE).unwrap();
        file.write_all(b"a\nb\r\n\x1a tail").unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, b"a\nb\r\n\x1a tail");
        #[cfg(unix)]
        {
            let flags = rustix::io::fcntl_getfd(&file).unwrap();
            assert!(flags.contains(rustix::io::FdFlags::CLOEXEC));
        }
        #[cfg(windows)]
        assert!(!win::handle_is_inheritable(&file).unwrap());
    }

    #[test]
    fn open_nofollow_refuses_a_symlink_leaf() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        fs::write(&target, b"keep").unwrap();
        let link = dir.path().join("link");
        if !symlink_file(&target, &link) {
            return;
        }
        for flags in [
            OpenFlags::RDONLY,
            OpenFlags::RDWR | OpenFlags::CREAT,
            OpenFlags::WRONLY | OpenFlags::CREAT | OpenFlags::TRUNC,
            OpenFlags::WRONLY | OpenFlags::APPEND | OpenFlags::CREAT,
        ] {
            let err = open_nofollow(&link, flags, PRIVATE_FILE_MODE).unwrap_err();
            assert_kind(&err, filesystem_loop_kind());
            #[cfg(unix)]
            assert_eq!(
                err.raw_os_error(),
                Some(rustix::io::Errno::LOOP.raw_os_error())
            );
        }
        assert_eq!(fs::read(&target).unwrap(), b"keep");
        assert_eq!(fs::read(&link).unwrap(), b"keep");
    }

    #[test]
    fn open_nofollow_refuses_a_dangling_symlink_without_creating() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing");
        let link = dir.path().join("link");
        if !symlink_file(&missing, &link) {
            return;
        }
        let err = open_nofollow(&link, OpenFlags::RDWR | OpenFlags::CREAT, PRIVATE_FILE_MODE)
            .unwrap_err();
        assert_kind(&err, filesystem_loop_kind());
        assert!(!missing.exists());
    }

    #[test]
    fn open_nofollow_excl_trunc_and_append() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        let file = open_nofollow(
            &path,
            OpenFlags::WRONLY | OpenFlags::CREAT | OpenFlags::EXCL,
            PRIVATE_FILE_MODE,
        )
        .unwrap();
        drop(file);
        let err = open_nofollow(
            &path,
            OpenFlags::WRONLY | OpenFlags::CREAT | OpenFlags::EXCL,
            PRIVATE_FILE_MODE,
        )
        .unwrap_err();
        assert_kind(&err, std::io::ErrorKind::AlreadyExists);

        let mut file = open_nofollow(&path, OpenFlags::WRONLY | OpenFlags::APPEND, 0).unwrap();
        file.write_all(b"1").unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(b"2").unwrap();
        drop(file);
        assert_eq!(fs::read(&path).unwrap(), b"12");

        let _ = open_nofollow(&path, OpenFlags::WRONLY | OpenFlags::TRUNC, 0).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"");

        let err = open_nofollow(dir.path().join("absent"), OpenFlags::RDONLY, 0).unwrap_err();
        assert_kind(&err, std::io::ErrorKind::NotFound);
    }

    #[test]
    fn make_private_fd_then_private_problem_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        let file =
            open_nofollow(&path, OpenFlags::RDWR | OpenFlags::CREAT, PRIVATE_FILE_MODE).unwrap();
        make_private_fd(&file, PRIVATE_FILE_MODE).unwrap();
        assert_eq!(
            private_problem(SecureTarget::File(&file), None, false).unwrap(),
            None
        );
        assert!(owned_by_current_user(SecureTarget::File(&file), None, false).unwrap());
        drop(file);
        assert_eq!(
            private_problem(SecureTarget::Path(&path), None, false).unwrap(),
            None
        );
    }

    #[test]
    fn fsync_directory_on_a_directory_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("entry"), b"x").unwrap();
        fsync_directory(dir.path()).unwrap();
    }

    #[test]
    fn replace_overwrites_a_file_and_keeps_the_source_when_the_target_is_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        fs::write(&src, b"new").unwrap();
        fs::write(&dst, b"old").unwrap();
        replace(&src, &dst).unwrap();
        assert_eq!(fs::read(&dst).unwrap(), b"new");
        assert!(!src.exists());

        let src = dir.path().join("src2");
        fs::write(&src, b"new").unwrap();
        let dst_dir = dir.path().join("dir");
        fs::create_dir(&dst_dir).unwrap();
        assert!(replace(&src, &dst_dir).is_err());
        assert_eq!(fs::read(&src).unwrap(), b"new");
    }

    #[test]
    fn windows_link_tags_and_dacl_rules() {
        const S_IFREG: u32 = 0o100000;
        const S_IFDIR: u32 = 0o040000;
        let reparse = FILE_ATTRIBUTE_REPARSE_POINT;
        let cloud = 0x9000_601Au32;
        assert!(classify_windows_link(
            S_IFLNK,
            reparse,
            IO_REPARSE_TAG_SYMLINK
        ));
        assert!(classify_windows_link(
            S_IFDIR,
            reparse,
            IO_REPARSE_TAG_MOUNT_POINT
        ));
        assert!(!classify_windows_link(S_IFREG, reparse, cloud));
        assert!(!classify_windows_link(S_IFREG, 0x20, 0));
        assert!(classify_windows_link(
            S_IFREG,
            reparse,
            IO_REPARSE_TAG_SYMLINK
        ));
        assert!(classify_windows_link(
            S_IFREG,
            reparse,
            IO_REPARSE_TAG_MOUNT_POINT
        ));
        assert!(!classify_windows_link(S_IFDIR, 0, 0));
        assert!(!classify_windows_link(S_IFREG, reparse, 0));
        assert!(!is_link_reparse_tag(0));
        assert!(!is_link_reparse_tag(cloud));

        let user = "S-1-5-21-1-2-3-1001";
        let foreign = "S-1-5-21-9-9-9-1002";
        let everyone = "S-1-1-0";
        let owners = vec![user.to_owned(), SID_ADMINISTRATORS.to_owned()];
        let mut trusted = owners.clone();
        trusted.push(SID_SYSTEM.to_owned());
        trusted.push(SID_OWNER_RIGHTS.to_owned());
        let allow = |sid: &str| AceRecord {
            ace_type: ACCESS_ALLOWED_ACE_TYPE,
            flags: 0,
            mask: 0x001F_01FF,
            sid: sid.to_owned(),
        };
        let problem = |owner: &str, aces: Option<&[AceRecord]>, directory: bool| {
            acl_problem(owner, aces, directory, &owners, &trusted)
        };

        assert_eq!(problem(user, Some(&[]), false), None);
        assert_eq!(problem(user, None, false), Some(PrivateProblem::Mode));
        assert_eq!(
            problem(foreign, Some(&[allow(user)]), false),
            Some(PrivateProblem::Owner)
        );
        assert_eq!(problem("", Some(&[]), false), Some(PrivateProblem::Owner));
        assert_eq!(problem(user, Some(&[allow(user)]), false), None);
        assert_eq!(problem(user, Some(&[allow(SID_SYSTEM)]), false), None);
        assert_eq!(
            problem(user, Some(&[allow(SID_ADMINISTRATORS)]), false),
            None
        );
        assert_eq!(problem(user, Some(&[allow(SID_OWNER_RIGHTS)]), false), None);
        assert_eq!(
            problem(user, Some(&[allow(everyone)]), false),
            Some(PrivateProblem::Mode)
        );
        assert_eq!(
            problem(user, Some(&[allow("S-1-5-32-545")]), false),
            Some(PrivateProblem::Mode)
        );
        let mut zero = allow(everyone);
        zero.mask = 0;
        assert_eq!(problem(user, Some(&[zero]), false), None);
        let mut inherit = allow(everyone);
        inherit.flags = INHERIT_ONLY_ACE;
        assert_eq!(problem(user, Some(&[inherit.clone()]), false), None);
        assert_eq!(
            problem(user, Some(&[inherit]), true),
            Some(PrivateProblem::Mode)
        );
        let deny = AceRecord {
            ace_type: ACCESS_DENIED_ACE_TYPE,
            flags: 0,
            mask: 0x001F_01FF,
            sid: everyone.to_owned(),
        };
        assert_eq!(problem(user, Some(&[deny, allow(user)]), false), None);
        let audit = AceRecord {
            ace_type: 0x2,
            flags: 0,
            mask: 0,
            sid: String::new(),
        };
        assert_eq!(problem(user, Some(&[audit, allow(user)]), false), None);
        let object_allow = AceRecord {
            ace_type: 0x5,
            flags: 0,
            mask: 1,
            sid: user.to_owned(),
        };
        assert_eq!(
            problem(user, Some(&[object_allow]), false),
            Some(PrivateProblem::Mode)
        );
        let callback = AceRecord {
            ace_type: ACCESS_ALLOWED_CALLBACK_ACE_TYPE,
            flags: 0,
            mask: 1,
            sid: everyone.to_owned(),
        };
        assert_eq!(
            problem(user, Some(&[callback]), false),
            Some(PrivateProblem::Mode)
        );
        let callback_user = AceRecord {
            ace_type: ACCESS_ALLOWED_CALLBACK_ACE_TYPE,
            flags: 0,
            mask: 1,
            sid: user.to_owned(),
        };
        assert_eq!(problem(user, Some(&[callback_user]), false), None);
        assert!(is_deny_ace(0x1) && is_deny_ace(0x6) && is_deny_ace(0xA) && is_deny_ace(0xC));
        assert!(is_sacl_ace(0x2) && is_sacl_ace(0x14));
        assert!(!is_sacl_ace(0x15));
        assert!(!is_deny_ace(0x0));
    }

    #[cfg(unix)]
    #[test]
    fn posix_is_link_stat_ignores_reparse_tags() {
        #[allow(clippy::unnecessary_cast, clippy::cast_possible_truncation)]
        let symlink_mode = rustix::fs::FileType::Symlink.as_raw_mode() as u32;
        assert_eq!(symlink_mode, S_IFLNK);
        #[allow(clippy::unnecessary_cast, clippy::cast_possible_truncation)]
        let regular = 0o100000u32 as rustix::fs::RawMode;
        assert_ne!(
            rustix::fs::FileType::from_raw_mode(regular),
            rustix::fs::FileType::Symlink
        );
        let cloud_like = LinkStat {
            mode: 0o100000,
            file_attributes: FILE_ATTRIBUTE_REPARSE_POINT,
            reparse_tag: IO_REPARSE_TAG_SYMLINK,
        };
        assert!(!is_link_stat(cloud_like));
        assert!(classify_windows_link(
            cloud_like.mode,
            cloud_like.file_attributes,
            cloud_like.reparse_tag
        ));
        assert!(is_link_stat(LinkStat {
            mode: S_IFLNK,
            file_attributes: 0,
            reparse_tag: 0,
        }));
    }

    #[cfg(unix)]
    fn permission_bits(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        fs::symlink_metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[cfg(unix)]
    fn set_mode(path: &Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn posix_chmod_and_fchmod_match_the_mode_bits() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        fs::write(&path, b"x").unwrap();
        set_mode(&path, 0o644);
        assert_eq!(
            private_problem(SecureTarget::Path(&path), None, false).unwrap(),
            Some(PrivateProblem::Mode)
        );
        make_private(&path, Some(0o600)).unwrap();
        assert_eq!(permission_bits(&path), 0o600);
        assert_eq!(
            private_problem(SecureTarget::Path(&path), None, false).unwrap(),
            None
        );

        set_mode(&path, 0o640);
        let file = std::fs::File::open(&path).unwrap();
        assert_eq!(
            private_problem(SecureTarget::File(&file), None, false).unwrap(),
            Some(PrivateProblem::Mode)
        );
        assert!(owned_by_current_user(SecureTarget::File(&file), None, false).unwrap());
        make_private_fd(&file, 0o600).unwrap();
        let mode = file.metadata().unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        assert_eq!(
            private_problem(SecureTarget::File(&file), None, false).unwrap(),
            None
        );
        make_private_fd(&file, 0o640).unwrap();
        assert_eq!(
            private_problem(SecureTarget::File(&file), None, false).unwrap(),
            Some(PrivateProblem::Mode)
        );
    }

    #[cfg(unix)]
    #[test]
    fn posix_directory_default_is_0700_and_snapshot_skips_stat() {
        let dir = tempfile::tempdir().unwrap();
        let private_dir = dir.path().join("private");
        fs::create_dir(&private_dir).unwrap();
        set_mode(&private_dir, 0o755);
        assert_eq!(
            private_problem(SecureTarget::Path(&private_dir), None, false).unwrap(),
            Some(PrivateProblem::Mode)
        );
        make_private(&private_dir, None).unwrap();
        assert_eq!(permission_bits(&private_dir), 0o700);
        assert_eq!(
            private_problem(SecureTarget::Path(&private_dir), None, false).unwrap(),
            None
        );

        let me = current_user_id(false);
        assert_eq!(me, rustix::process::getuid().as_raw());
        assert_eq!(current_user_id(true), rustix::process::geteuid().as_raw());
        let missing = Path::new("/no/such/securefile-snapshot");
        assert_eq!(
            private_problem(
                SecureTarget::Path(missing),
                Some(StatSnapshot {
                    uid: me.wrapping_add(1),
                    mode: 0o600,
                }),
                false,
            )
            .unwrap(),
            Some(PrivateProblem::Owner)
        );
        assert_eq!(
            private_problem(
                SecureTarget::Path(missing),
                Some(StatSnapshot {
                    uid: me,
                    mode: 0o644
                }),
                false,
            )
            .unwrap(),
            Some(PrivateProblem::Mode)
        );
        assert_eq!(
            private_problem(
                SecureTarget::Path(missing),
                Some(StatSnapshot {
                    uid: me,
                    mode: 0o100600,
                }),
                false,
            )
            .unwrap(),
            None
        );
        assert_eq!(
            private_problem(
                SecureTarget::Path(missing),
                Some(StatSnapshot {
                    uid: me,
                    mode: 0o100640,
                }),
                false,
            )
            .unwrap(),
            Some(PrivateProblem::Mode)
        );
        assert_eq!(
            private_problem(
                SecureTarget::Path(missing),
                Some(StatSnapshot {
                    uid: me,
                    mode: 0o4700
                }),
                false,
            )
            .unwrap(),
            None
        );
        assert!(
            !owned_by_current_user(
                SecureTarget::Path(missing),
                Some(StatSnapshot {
                    uid: me.wrapping_add(1),
                    mode: 0o600,
                }),
                false,
            )
            .unwrap()
        );
    }

    #[cfg(unix)]
    #[test]
    fn posix_open_and_fsync_follow_only_where_the_original_call_does() {
        let dir = tempfile::tempdir().unwrap();
        let file = open_nofollow(dir.path(), OpenFlags::RDONLY, 0).unwrap();
        assert!(file.metadata().unwrap().is_dir());

        let real = dir.path().join("real");
        fs::create_dir(&real).unwrap();
        let link = dir.path().join("linkdir");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        fsync_directory(&link).unwrap();

        let regular = dir.path().join("file");
        fs::write(&regular, b"x").unwrap();
        let err = fsync_directory(&regular).unwrap_err();
        assert_kind(&err, std::io::ErrorKind::NotADirectory);
        assert_eq!(
            err.raw_os_error(),
            Some(rustix::io::Errno::NOTDIR.raw_os_error())
        );

        set_mode(&regular, 0o644);
        let via = dir.path().join("via");
        std::os::unix::fs::symlink(&regular, &via).unwrap();
        make_private(&via, None).unwrap();
        assert_eq!(permission_bits(&regular), 0o600);
        assert_eq!(
            private_problem(SecureTarget::Path(&via), None, false).unwrap(),
            None
        );
        let err = open_nofollow(&via, OpenFlags::RDONLY, 0).unwrap_err();
        assert_kind(&err, filesystem_loop_kind());

        let info = rustix::fs::lstat(&via).unwrap();
        assert!(is_link_stat(LinkStat {
            mode: info.st_mode as u32,
            file_attributes: 0,
            reparse_tag: 0,
        }));
        let info = rustix::fs::lstat(&regular).unwrap();
        assert!(!is_link_stat(LinkStat {
            mode: info.st_mode as u32,
            file_attributes: 0,
            reparse_tag: 0,
        }));
    }

    #[cfg(unix)]
    #[test]
    fn posix_replace_overwrites_a_read_only_file_and_pin_is_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        fs::write(&src, b"new").unwrap();
        fs::write(&dst, b"old").unwrap();
        set_mode(&dst, 0o444);
        replace(&src, &dst).unwrap();
        assert_eq!(fs::read(&dst).unwrap(), b"new");
        assert!(!src.exists());

        let err = pin_directory(dir.path()).unwrap_err();
        assert!(matches!(err, SecureFileError::PinDirectoryUnsupported));
        assert_eq!(
            err.to_string(),
            "pin_directory is the Windows stand-in for O_DIRECTORY/dir_fd walks; POSIX callers use dir_fd directly"
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_open_flags_match_the_crt() {
        assert_eq!(OpenFlags::WRONLY.bits(), 1);
        assert_eq!(OpenFlags::RDWR.bits(), 2);
        assert_eq!(OpenFlags::APPEND.bits(), 8);
        assert_eq!(OpenFlags::CREAT.bits(), 0x100);
        assert_eq!(OpenFlags::TRUNC.bits(), 0x200);
        assert_eq!(OpenFlags::EXCL.bits(), 0x400);
    }

    #[cfg(windows)]
    #[test]
    fn windows_directory_open_is_rejected_and_snapshot_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let err = open_nofollow(dir.path(), OpenFlags::RDONLY, 0).unwrap_err();
        assert_kind(&err, std::io::ErrorKind::IsADirectory);

        let path = dir.path().join("secret");
        fs::write(&path, b"x").unwrap();
        make_private(&path, Some(PRIVATE_FILE_MODE)).unwrap();
        let fake = StatSnapshot {
            uid: 0,
            mode: 0o777,
        };
        assert_eq!(
            private_problem(SecureTarget::Path(&path), Some(fake), true).unwrap(),
            None
        );
        let sid = current_user_id(false).unwrap();
        assert!(sid.starts_with("S-1-5-"));
        assert_eq!(current_user_id(true).unwrap(), sid);
    }

    #[cfg(windows)]
    #[test]
    fn windows_make_private_fd_rejects_a_wide_mode_and_directory_children_inherit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        let file = open_nofollow(
            &path,
            OpenFlags::WRONLY | OpenFlags::CREAT,
            PRIVATE_FILE_MODE,
        )
        .unwrap();
        let err = make_private_fd(&file, 0o644).unwrap_err();
        assert!(matches!(
            err,
            SecureFileError::ModeNotPrivate { mode: 0o644 }
        ));
        drop(file);

        let private_dir = dir.path().join("private");
        fs::create_dir(&private_dir).unwrap();
        make_private(&private_dir, Some(PRIVATE_DIR_MODE)).unwrap();
        assert_eq!(
            private_problem(SecureTarget::Path(&private_dir), None, false).unwrap(),
            None
        );
        let child = private_dir.join("child");
        fs::write(&child, b"x").unwrap();
        assert_eq!(
            private_problem(SecureTarget::Path(&child), None, false).unwrap(),
            None
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_replace_overwrites_a_read_only_file() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        fs::write(&src, b"new").unwrap();
        fs::write(&dst, b"old").unwrap();
        let mut perms = fs::metadata(&dst).unwrap().permissions();
        perms.set_readonly(true);
        fs::set_permissions(&dst, perms).unwrap();
        replace(&src, &dst).unwrap();
        assert_eq!(fs::read(&dst).unwrap(), b"new");
        assert!(!src.exists());
    }

    #[cfg(windows)]
    #[test]
    fn windows_symlink_is_not_private_and_make_private_refuses_it() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        fs::write(&target, b"x").unwrap();
        let link = dir.path().join("link");
        if !symlink_file(&target, &link) {
            return;
        }
        assert_eq!(
            private_problem(SecureTarget::Path(&link), None, false).unwrap(),
            Some(PrivateProblem::Owner)
        );
        assert!(!owned_by_current_user(SecureTarget::Path(&link), None, false).unwrap());
        let err = make_private(&link, Some(PRIVATE_FILE_MODE)).unwrap_err();
        assert_kind(&err, filesystem_loop_kind());
    }

    #[cfg(windows)]
    #[test]
    fn windows_pin_directory_opens_children_and_rejects_a_bad_component() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("a").join("b");
        fs::create_dir_all(&nested).unwrap();
        {
            let pinned = pin_directory(&nested).unwrap();
            let created = pinned
                .open(
                    "new",
                    OpenFlags::WRONLY | OpenFlags::CREAT | OpenFlags::EXCL,
                    PRIVATE_FILE_MODE,
                )
                .unwrap();
            drop(created);
            replace(pinned.child("new").unwrap(), pinned.child("final").unwrap()).unwrap();
            assert!(pinned.child("..").is_err());
            assert!(pinned.child(".").is_err());
            assert!(pinned.child("x:y").is_err());
            assert!(pinned.child(r"x\y").is_err());
            assert!(pinned.child("").is_err());
            assert!(pinned.fd().is_ok());
            assert!(pinned.stat().unwrap().is_dir());
            assert!(fs::rename(&nested, dir.path().join("a").join("moved")).is_err());
            assert!(fs::rename(dir.path().join("a"), dir.path().join("moved")).is_err());
        }
        fs::rename(&nested, dir.path().join("a").join("moved")).unwrap();
        assert!(dir.path().join("a").join("moved").join("final").is_file());

        let plain = dir.path().join("plain");
        fs::write(&plain, b"x").unwrap();
        let err = pin_directory(&plain).unwrap_err();
        assert_kind(&err, std::io::ErrorKind::NotADirectory);

        let real = dir.path().join("real");
        fs::create_dir(&real).unwrap();
        let link = dir.path().join("linkdir");
        if std::os::windows::fs::symlink_dir(&real, &link).is_ok() {
            let err = pin_directory(&link).unwrap_err();
            assert_kind(&err, filesystem_loop_kind());
        }
    }
}
