//! Owner-private content-addressed attachment files.
//!
//! [`AttachmentStore`] keeps one directory beside a ledger:
//! `absolute(expanduser(ledger_path)).parent / "attachments"`. That absolute
//! step does not normalize `..` or follow a symlink. That directory
//! is backed up with the ledger. The store does not take a storage directory
//! from a caller-supplied root, an HTTP field, or attachment metadata. The
//! ledger path is the only location input, and the attachment directory is
//! derived from it.
//!
//! Each owner lives under `root / sha256(owner)`. Bytes live in
//! `blobs/{sha256}` and the reference is `refs/{file_id}.json`. The blob is
//! published first. A failed retry of the same bytes does not create a second
//! reference. Directories are mode `0o700`.
//!
//! On Windows, publication holds the directory with
//! [`pin_directory`](crate::securefile::pin_directory) and publishes the file
//! with [`replace`](crate::securefile::replace), including that replace's
//! existing limited retries. On POSIX, `pin_directory` is not available. Publication
//! walks the directory with no-follow directory descriptors and renames
//! inside the held directory, the same guarantee those primitives give.
//!
//! [`delivery_body`] only formats text. It does not read the store. Nothing
//! in this module is called by conversation, HTTP, the CLI, dashboard,
//! daemon delivery, or a conversation adapter. `url` is a metadata string
//! shaped like `/api/attachments/{file_id}`. This module does not serve that
//! path.
//!
//! # What CI runs
//!
//! Linux and macOS CI run the storage API against a temporary ledger: upload
//! checks, content addressing, owner isolation, corrupt references, symlink
//! rejection, and the POSIX directory-descriptor publish. Windows CI compiles
//! the `pin_directory` / `replace` publish path and runs the same storage
//! tests. A symlink test skips when the process cannot create one.

use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use sha2::{Digest, Sha256};

use crate::securefile::{OpenFlags, PRIVATE_DIR_MODE, open_nofollow};

#[cfg(unix)]
use crate::securefile::{LinkStat, is_link_stat, make_private_fd};

#[cfg(windows)]
use crate::securefile::make_private;

/// Largest accepted upload, 10 MiB.
///
/// ```
/// assert_eq!(vyane_core::MAX_ATTACHMENT_BYTES, 10 * 1024 * 1024);
/// ```
pub const MAX_ATTACHMENT_BYTES: usize = 10 * 1024 * 1024;

/// Filename extension and the only MIME type that extension accepts.
///
/// The extension match is case-insensitive. A MIME parameter after `;` is
/// ignored. The comparison uses the lowercased type and subtype.
///
/// ```
/// assert_eq!(vyane_core::ATTACHMENT_TYPES.len(), 13);
/// assert!(vyane_core::ATTACHMENT_TYPES.contains(&(".png", "image/png")));
/// assert!(vyane_core::ATTACHMENT_TYPES.contains(&(".yml", "application/yaml")));
/// ```
pub const ATTACHMENT_TYPES: &[(&str, &str)] = &[
    (".txt", "text/plain"),
    (".md", "text/markdown"),
    (".log", "text/plain"),
    (".csv", "text/csv"),
    (".json", "application/json"),
    (".yaml", "application/yaml"),
    (".yml", "application/yaml"),
    (".png", "image/png"),
    (".jpg", "image/jpeg"),
    (".jpeg", "image/jpeg"),
    (".gif", "image/gif"),
    (".webp", "image/webp"),
    (".pdf", "application/pdf"),
];

/// A rejected attachment operation.
///
/// `code` and `message` are fixed strings. `status` is the HTTP-shaped class
/// the storage rule uses (`400`, `403`, `404`, `413`, `415`, or `503`). This
/// type does not write a response.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct AttachmentError {
    /// Stable machine code, such as `invalid_filename` or `storage_unavailable`.
    pub code: &'static str,
    /// Status class. This library does not serve HTTP.
    pub status: u16,
    /// Fixed human-readable message.
    pub message: &'static str,
}

impl AttachmentError {
    const fn new(code: &'static str, status: u16, message: &'static str) -> Self {
        Self {
            code,
            status,
            message,
        }
    }
}

/// Public metadata rebuilt from the owner and the bytes on disk.
///
/// `id` and `file_id` are the same value. `file_type` is the metadata field
/// named `type`: `"image"` when [`Self::mime_type`] starts with `image/`,
/// otherwise `"file"`. [`Self::url`] is only a string. This module does not
/// serve it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentMetadata {
    /// Same value as [`Self::file_id`].
    pub id: String,
    /// `file_` plus 64 lowercase hex digits.
    pub file_id: String,
    /// Original filename. The extension was checked; the name is not rewritten.
    pub name: String,
    /// MIME type with any parameter removed, in lowercase.
    pub mime_type: String,
    /// Byte length of the stored blob.
    pub size_bytes: u64,
    /// Lowercase sha256 of the blob.
    pub sha256: String,
    /// `"image"` or `"file"`.
    pub file_type: String,
    /// Metadata string `/api/attachments/{file_id}`. Not a route.
    pub url: String,
}

/// One resolved file passed to [`delivery_body`].
///
/// The function does not open `path`. The caller supplies the name, the MIME
/// type, and the local path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryAttachment<'a> {
    /// Filename recorded for the attachment.
    pub name: &'a str,
    /// Normalized MIME type.
    pub mime_type: &'a str,
    /// Local path a file tool would read.
    pub path: &'a Path,
}

/// Format `body` with the local attachment lines.
///
/// An empty `attachments` slice returns `body` unchanged. Otherwise the text
/// appended after `body` starts with
/// `本次用户附件（本地指 ROG；请用文件工具读取）：`. Each file contributes
/// `附件：` plus a JSON object of `name` and `mime_type`, then a JSON string of
/// the local path. This function does not read the filesystem and is not
/// called by a conversation adapter.
///
/// ```
/// use std::path::Path;
/// let path = Path::new("note.txt");
/// let text = vyane_core::delivery_body(
///     "hello",
///     &[vyane_core::DeliveryAttachment {
///         name: "n.txt",
///         mime_type: "text/plain",
///         path,
///     }],
/// );
/// assert!(text.starts_with("hello\n\n本次用户附件（本地指 ROG；请用文件工具读取）：\n"));
/// assert!(text.contains("附件：{\"name\": \"n.txt\", \"mime_type\": \"text/plain\"}\n"));
/// ```
#[must_use]
pub fn delivery_body(body: &str, attachments: &[DeliveryAttachment<'_>]) -> String {
    if attachments.is_empty() {
        return body.to_owned();
    }
    let mut out = String::new();
    out.push_str(body);
    out.push_str("\n\n本次用户附件（本地指 ROG；请用文件工具读取）：\n");
    for (index, item) in attachments.iter().enumerate() {
        if index > 0 {
            out.push('\n');
        }
        out.push_str("附件：");
        out.push_str("{\"name\": ");
        out.push_str(&json_string(item.name));
        out.push_str(", \"mime_type\": ");
        out.push_str(&json_string(item.mime_type));
        out.push('}');
        out.push('\n');
        out.push_str(&json_string(&path_text(item.path)));
    }
    out
}

/// Check a filename and MIME type before any byte is stored.
///
/// The filename must be non-empty, at most 255 Unicode scalar values, and not
/// `.` or `..`. It must not contain `/`, `\`, or a character whose code point
/// is below 32. Rust strings cannot contain a surrogate, so a surrogate in the
/// filename cannot pass this function. The extension is the last `.` that is
/// not the first or last character, matching `PurePath.suffix`. It must be in
/// [`ATTACHMENT_TYPES`]. `mime_type` loses any `;` parameter, then surrounding
/// whitespace, then is lowercased, and must equal that extension's type.
///
/// Returns the original filename and the normalized MIME type.
///
/// ```
/// let (name, mime) = vyane_core::validate_upload("Notes.TXT", "text/plain; charset=utf-8")
///     .unwrap();
/// assert_eq!(name, "Notes.TXT");
/// assert_eq!(mime, "text/plain");
/// ```
#[must_use = "the normalized MIME type is the value stored with the file"]
pub fn validate_upload<'a>(
    name: &'a str,
    mime_type: &str,
) -> Result<(&'a str, String), AttachmentError> {
    if name.is_empty()
        || name.chars().count() > 255
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || name.chars().any(|ch| u32::from(ch) < 32)
    {
        return Err(invalid_filename());
    }
    let suffix = python_suffix(name).to_ascii_lowercase();
    let mime = normalized_mime(mime_type);
    let matches = ATTACHMENT_TYPES
        .iter()
        .any(|(extension, expected)| *extension == suffix && *expected == mime);
    if !matches {
        return Err(unsupported_type());
    }
    Ok((name, mime))
}

/// Content-addressed files for one ledger.
///
/// Construct it with the ledger path. The attachment directory is
/// [`Self::root`], beside that ledger, and is the directory a backup of the
/// ledger should include. Do not pass a directory taken from a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentStore {
    /// `<absolute ledger parent>/attachments`.
    pub root: PathBuf,
}

impl AttachmentStore {
    /// Root the store beside `ledger_path`.
    ///
    /// `~` and `~/...` expand to the current user's home directory (`HOME` on
    /// POSIX, `USERPROFILE` or `HOMEDRIVE`+`HOMEPATH` on Windows). A `~user`
    /// path is left unchanged. The path is then made absolute without
    /// normalizing `..` or resolving symlinks, and the store root is that
    /// path's parent joined with `attachments`. On Windows the ledger path is
    /// not passed to `GetFullPathNameW`, which would resolve `..`.
    #[must_use]
    pub fn new(ledger_path: impl AsRef<Path>) -> Self {
        Self {
            root: attachments_root(ledger_path.as_ref()),
        }
    }

    /// Store `data` for `owner` and return the public metadata.
    ///
    /// Validation runs before any directory is created. The blob is published
    /// before the reference. The same owner, bytes, name, and MIME type
    /// produce the same `file_id`.
    pub fn put(
        &self,
        owner: &str,
        name: &str,
        mime_type: &str,
        data: &[u8],
    ) -> Result<AttachmentMetadata, AttachmentError> {
        let (name, mime) = validate_upload(name, mime_type)?;
        validate_content(data, &mime)?;
        let sha = sha256_hex(data);
        let file_id = file_id_for(owner, &sha, name, &mime);
        let size_bytes = byte_len(data)?;
        let metadata = build_metadata(&file_id, name, &mime, size_bytes, &sha);
        let directory = self.owner_dir(owner)?;
        checked_path(&directory)?;
        ensure_dir(&self.root)?;
        ensure_dir(&directory)?;
        publish_file(&directory.join("blobs").join(&sha), data)?;
        publish_file(
            &directory.join("refs").join(format!("{file_id}.json")),
            &metadata_bytes(&metadata),
        )?;
        Ok(metadata)
    }

    /// Read `file_id` for `owner`.
    ///
    /// The returned metadata is rebuilt from the owner, the blob bytes, and
    /// the name and MIME type after [`validate_upload`]. Extra fields in the
    /// reference are ignored. A missing reference or blob is
    /// [`file_not_found`](AttachmentError). A symlink, a reference larger than
    /// 4096 bytes, a blob over [`MAX_ATTACHMENT_BYTES`], or a hash, size, or
    /// identity mismatch is `storage_unavailable`.
    ///
    /// An id that is not `file_` plus 64 lowercase hex digits is
    /// `file_not_found` before the owner is checked.
    pub fn get(
        &self,
        owner: &str,
        file_id: &str,
    ) -> Result<(AttachmentMetadata, PathBuf, Vec<u8>), AttachmentError> {
        if !is_file_id(file_id) {
            return Err(file_not_found());
        }
        let directory = self.owner_dir(owner)?;
        let reference = directory.join("refs").join(format!("{file_id}.json"));
        let reference_bytes = read_regular(&reference, 4096)?;
        let stored = parse_stored(&reference_bytes)?;
        if !is_lower_hex(&stored.sha256, 64) {
            return Err(storage_unavailable());
        }
        let (name, mime) =
            validate_upload(&stored.name, &stored.mime_type).map_err(|_| storage_unavailable())?;
        let expected = file_id_for(owner, &stored.sha256, name, &mime);
        if expected != file_id || stored.id != file_id || stored.file_id != file_id {
            return Err(storage_unavailable());
        }
        let blob = directory.join("blobs").join(&stored.sha256);
        let data = read_regular(
            &blob,
            u64::try_from(MAX_ATTACHMENT_BYTES).unwrap_or(u64::MAX),
        )?;
        let size_bytes = byte_len(&data)?;
        if sha256_hex(&data) != stored.sha256 || size_bytes != stored.size_bytes {
            return Err(storage_unavailable());
        }
        validate_content(&data, &mime).map_err(|_| storage_unavailable())?;
        let metadata = build_metadata(file_id, name, &mime, size_bytes, &stored.sha256);
        Ok((metadata, blob, data))
    }

    /// Normalize attachment references. This does not look at the store.
    ///
    /// `items` must be an array of at most 32 objects. Each object contributes
    /// `file_id`, or `id` when `file_id` is absent. When both are present and
    /// their JSON values differ, the error is `invalid_attachment` 400,
    /// message `附件 id 不一致`. A value that is not `file_` plus 64 lowercase
    /// hex digits is `file_not_found` 404. A non-array, or an array longer
    /// than 32, is `invalid_attachment` 400, message
    /// `附件必须是最多 32 项的数组`. A non-object element is
    /// `invalid_attachment` 400, message `附件必须引用 file_id`.
    pub fn reference_ids(items: &serde_json::Value) -> Result<Vec<String>, AttachmentError> {
        let Some(entries) = items.as_array() else {
            return Err(too_many_attachments());
        };
        if entries.len() > 32 {
            return Err(too_many_attachments());
        }
        let mut file_ids = Vec::with_capacity(entries.len());
        for item in entries {
            let Some(object) = item.as_object() else {
                return Err(attachment_must_reference_file_id());
            };
            let has_id = object.contains_key("id");
            let has_file_id = object.contains_key("file_id");
            if has_id && has_file_id && object.get("id") != object.get("file_id") {
                return Err(inconsistent_attachment_id());
            }
            let chosen = if has_file_id {
                object.get("file_id")
            } else {
                object.get("id")
            };
            let Some(file_id) = chosen.and_then(serde_json::Value::as_str) else {
                return Err(file_not_found());
            };
            if !is_file_id(file_id) {
                return Err(file_not_found());
            }
            file_ids.push(file_id.to_owned());
        }
        Ok(file_ids)
    }

    /// [`Self::reference_ids`] then [`Self::get`] for each id.
    ///
    /// The result is public metadata only. The first error from either step
    /// is returned as-is.
    pub fn resolve(
        &self,
        owner: &str,
        items: &serde_json::Value,
    ) -> Result<Vec<AttachmentMetadata>, AttachmentError> {
        let mut resolved = Vec::new();
        for file_id in Self::reference_ids(items)? {
            let (metadata, _, _) = self.get(owner, &file_id)?;
            resolved.push(metadata);
        }
        Ok(resolved)
    }

    fn owner_dir(&self, owner: &str) -> Result<PathBuf, AttachmentError> {
        if python_strip(owner).is_empty() {
            return Err(owner_required());
        }
        Ok(self.root.join(sha256_hex(owner.as_bytes())))
    }
}

fn invalid_filename() -> AttachmentError {
    AttachmentError::new("invalid_filename", 400, "文件名不合法")
}

fn unsupported_type() -> AttachmentError {
    AttachmentError::new(
        "unsupported_type",
        415,
        "文件扩展名与 MIME 类型不支持或不匹配",
    )
}

fn owner_required() -> AttachmentError {
    AttachmentError::new("owner_required", 403, "需要可信 owner")
}

fn storage_unavailable() -> AttachmentError {
    AttachmentError::new("storage_unavailable", 503, "附件存储不可用")
}

fn file_not_found() -> AttachmentError {
    AttachmentError::new("file_not_found", 404, "文件不存在")
}

fn too_many_attachments() -> AttachmentError {
    AttachmentError::new("invalid_attachment", 400, "附件必须是最多 32 项的数组")
}

fn attachment_must_reference_file_id() -> AttachmentError {
    AttachmentError::new("invalid_attachment", 400, "附件必须引用 file_id")
}

fn inconsistent_attachment_id() -> AttachmentError {
    AttachmentError::new("invalid_attachment", 400, "附件 id 不一致")
}

fn attachments_root(ledger_path: &Path) -> PathBuf {
    let expanded = expanduser(ledger_path);
    let absolute = ledger_absolute(&expanded);
    match absolute.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join("attachments"),
        _ => PathBuf::from("attachments"),
    }
}

/// `Path.absolute()`: make `path` absolute and keep every `..`.
///
/// POSIX [`std::path::absolute`] already keeps `..`. Windows
/// [`std::path::absolute`] is `GetFullPathNameW`, which resolves `..` and
/// would drop a symlink that appears only before that `..`.
fn ledger_absolute(path: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        absolute_like_python(path)
    }
    #[cfg(not(windows))]
    {
        std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf())
    }
}

#[cfg(windows)]
fn absolute_like_python(path: &Path) -> PathBuf {
    use std::path::Component;

    if path.as_os_str().is_empty() || path.is_absolute() {
        return path.to_path_buf();
    }
    // Relative, or root-relative (`\foo`, `/foo`). `join` keeps a root and
    // appends a relative tail without resolving `..`.
    let has_prefix = matches!(path.components().next(), Some(Component::Prefix(_)));
    if !has_prefix {
        return match std::env::current_dir() {
            Ok(cwd) => cwd.join(path),
            Err(_) => path.to_path_buf(),
        };
    }
    // Drive-relative `C:name\..\rest`. The anchor is `ntpath.abspath("C:")`,
    // the current directory on that drive. The tail is appended as text.
    let Some(prefix) = path
        .components()
        .next()
        .and_then(|component| match component {
            Component::Prefix(prefix) => Some(prefix),
            _ => None,
        })
    else {
        return path.to_path_buf();
    };
    let anchor = std::path::absolute(Path::new(prefix.as_os_str()))
        .unwrap_or_else(|_| PathBuf::from(prefix.as_os_str()));
    let mut tail = PathBuf::new();
    for component in path.components().skip(1) {
        match component {
            Component::CurDir | Component::RootDir | Component::Prefix(_) => {}
            Component::ParentDir => tail.push(".."),
            Component::Normal(name) => tail.push(name),
        }
    }
    if tail.as_os_str().is_empty() {
        return anchor;
    }
    let mut joined = std::ffi::OsString::from(anchor.as_os_str());
    let needs_sep = joined
        .as_encoded_bytes()
        .last()
        .is_some_and(|byte| *byte != b'\\' && *byte != b'/');
    if needs_sep {
        joined.push(r"\");
    }
    joined.push(tail.as_os_str());
    PathBuf::from(joined)
}

fn expanduser(path: &Path) -> PathBuf {
    let Some(text) = path.to_str() else {
        return path.to_path_buf();
    };
    if text == "~" {
        return home_dir().unwrap_or_else(|| path.to_path_buf());
    }
    if let Some(rest) = text.strip_prefix("~/").or_else(|| text.strip_prefix("~\\"))
        && let Some(home) = home_dir()
    {
        return home.join(rest);
    }
    path.to_path_buf()
}

fn home_dir() -> Option<PathBuf> {
    #[cfg(unix)]
    {
        let home = std::env::var_os("HOME")?;
        if home.is_empty() {
            None
        } else {
            Some(PathBuf::from(home))
        }
    }
    #[cfg(windows)]
    {
        if let Some(profile) = std::env::var_os("USERPROFILE")
            && !profile.is_empty()
        {
            return Some(PathBuf::from(profile));
        }
        let drive = std::env::var_os("HOMEDRIVE")?;
        let path = std::env::var_os("HOMEPATH")?;
        if drive.is_empty() || path.is_empty() {
            return None;
        }
        let mut home = std::ffi::OsString::new();
        home.push(drive);
        home.push(path);
        Some(PathBuf::from(home))
    }
}

fn python_strip(value: &str) -> &str {
    value.trim_matches(is_python_space)
}

fn is_python_space(ch: char) -> bool {
    ch.is_whitespace() || matches!(ch, '\u{1c}' | '\u{1d}' | '\u{1e}' | '\u{1f}')
}

/// `PurePath.suffix`: the last dot, when it is neither the first nor the last
/// character. `.txt` and `foo.` have an empty suffix.
fn python_suffix(name: &str) -> &str {
    match name.rfind('.') {
        Some(index) if index > 0 && index + 1 < name.len() => &name[index..],
        _ => "",
    }
}

fn normalized_mime(mime_type: &str) -> String {
    let head = match mime_type.split_once(';') {
        Some((head, _)) => head,
        None => mime_type,
    };
    python_strip(head).to_ascii_lowercase()
}

fn validate_content(data: &[u8], mime: &str) -> Result<(), AttachmentError> {
    if data.is_empty() {
        return Err(AttachmentError::new("empty_file", 400, "不能上传空文件"));
    }
    if data.len() > MAX_ATTACHMENT_BYTES {
        return Err(AttachmentError::new(
            "file_too_large",
            413,
            "文件不能超过 10 MiB",
        ));
    }
    let valid = match mime {
        "image/png" => data.starts_with(b"\x89PNG\r\n\x1a\n"),
        "image/jpeg" => data.starts_with(b"\xff\xd8\xff"),
        "image/gif" => data.starts_with(b"GIF87a") || data.starts_with(b"GIF89a"),
        "image/webp" => data.len() >= 12 && &data[..4] == b"RIFF" && &data[8..12] == b"WEBP",
        "application/pdf" => data.starts_with(b"%PDF-"),
        _ => std::str::from_utf8(data).is_ok() && !data.contains(&0),
    };
    if !valid {
        return Err(AttachmentError::new(
            "invalid_content",
            415,
            "文件内容与类型不匹配",
        ));
    }
    Ok(())
}

fn file_id_for(owner: &str, sha: &str, name: &str, mime: &str) -> String {
    let identity = format!(
        "[{}, {}, {}, {}]",
        json_string(owner),
        json_string(sha),
        json_string(name),
        json_string(mime),
    );
    format!("file_{}", sha256_hex(identity.as_bytes()))
}

fn build_metadata(
    file_id: &str,
    name: &str,
    mime: &str,
    size_bytes: u64,
    sha: &str,
) -> AttachmentMetadata {
    AttachmentMetadata {
        id: file_id.to_owned(),
        file_id: file_id.to_owned(),
        name: name.to_owned(),
        mime_type: mime.to_owned(),
        size_bytes,
        sha256: sha.to_owned(),
        file_type: if mime.starts_with("image/") {
            "image"
        } else {
            "file"
        }
        .to_owned(),
        url: format!("/api/attachments/{file_id}"),
    }
}

fn metadata_bytes(metadata: &AttachmentMetadata) -> Vec<u8> {
    format!(
        "{{\"id\": {id}, \"file_id\": {file_id}, \"name\": {name}, \"mime_type\": {mime}, \"size_bytes\": {size}, \"sha256\": {sha}, \"type\": {kind}, \"url\": {url}}}",
        id = json_string(&metadata.id),
        file_id = json_string(&metadata.file_id),
        name = json_string(&metadata.name),
        mime = json_string(&metadata.mime_type),
        size = metadata.size_bytes,
        sha = json_string(&metadata.sha256),
        kind = json_string(&metadata.file_type),
        url = json_string(&metadata.url),
    )
    .into_bytes()
}

/// JSON string spelling used by `json.dumps(..., ensure_ascii=False)`.
fn json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{000c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if u32::from(ch) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", u32::from(ch)));
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn path_text(path: &Path) -> String {
    path.display().to_string()
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

fn is_file_id(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("file_") else {
        return false;
    };
    is_lower_hex(rest, 64)
}

fn is_lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn byte_len(data: &[u8]) -> Result<u64, AttachmentError> {
    u64::try_from(data.len()).map_err(|_| storage_unavailable())
}

struct StoredFields {
    id: String,
    file_id: String,
    name: String,
    mime_type: String,
    size_bytes: u64,
    sha256: String,
}

fn parse_stored(bytes: &[u8]) -> Result<StoredFields, AttachmentError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| storage_unavailable())?;
    let Some(object) = value.as_object() else {
        return Err(storage_unavailable());
    };
    let string_field = |key: &str| -> Result<String, AttachmentError> {
        object
            .get(key)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(storage_unavailable)
    };
    let Some(size_bytes) = object.get("size_bytes").and_then(serde_json::Value::as_u64) else {
        return Err(storage_unavailable());
    };
    Ok(StoredFields {
        id: string_field("id")?,
        file_id: string_field("file_id")?,
        name: string_field("name")?,
        mime_type: string_field("mime_type")?,
        size_bytes,
        sha256: string_field("sha256")?,
    })
}

fn checked_path(path: &Path) -> Result<(), AttachmentError> {
    let mut current = path.to_path_buf();
    loop {
        if current.as_os_str().is_empty() {
            break;
        }
        match std::fs::symlink_metadata(&current) {
            Ok(meta) if metadata_is_link(&meta) => return Err(storage_unavailable()),
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return Err(storage_unavailable()),
        }
        let Some(parent) = current.parent().map(Path::to_path_buf) else {
            break;
        };
        if parent == current || parent.as_os_str().is_empty() {
            break;
        }
        current = parent;
    }
    Ok(())
}

fn metadata_is_link(meta: &std::fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // `S_ISLNK` via the same decision `securefile::is_link_stat` uses.
        is_link_stat(LinkStat {
            mode: meta.mode(),
            file_attributes: 0,
            reparse_tag: 0,
        })
    }
    #[cfg(windows)]
    {
        // `Metadata` has no reparse tag on this toolchain. `FileType::is_symlink`
        // reports the name-surrogate bit, which is set for a symlink or a
        // junction. A reparse point without that bit is not a link. A leaf
        // open still goes through `open_nofollow`.
        meta.file_type().is_symlink()
    }
}

fn ensure_dir(path: &Path) -> Result<(), AttachmentError> {
    if path.as_os_str().is_empty() {
        return Err(storage_unavailable());
    }
    checked_path(path)?;
    match std::fs::symlink_metadata(path) {
        Ok(meta) if metadata_is_link(&meta) => Err(storage_unavailable()),
        Ok(meta) if meta.is_dir() => Ok(()),
        Ok(_) => Err(storage_unavailable()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent()
                && !parent.as_os_str().is_empty()
                && parent != path
            {
                ensure_dir(parent)?;
            }
            create_private_dir(path)
        }
        Err(_) => Err(storage_unavailable()),
    }
}

fn create_private_dir(path: &Path) -> Result<(), AttachmentError> {
    checked_path(path)?;
    match create_dir_mode(path) {
        Ok(()) => tighten_new_dir(path),
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            checked_path(path)?;
            if is_real_directory(path) {
                Ok(())
            } else {
                Err(storage_unavailable())
            }
        }
        Err(_) => Err(storage_unavailable()),
    }
}

fn create_dir_mode(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .mode(PRIVATE_DIR_MODE)
            .create(path)
    }
    #[cfg(windows)]
    {
        std::fs::create_dir(path)
    }
}

fn tighten_new_dir(path: &Path) -> Result<(), AttachmentError> {
    #[cfg(unix)]
    {
        let directory = posix::open_dir_chain(path)?;
        let file = File::from(directory);
        make_private_fd(&file, PRIVATE_DIR_MODE).map_err(|_| storage_unavailable())
    }
    #[cfg(windows)]
    {
        checked_path(path)?;
        make_private(path, Some(PRIVATE_DIR_MODE)).map_err(|_| storage_unavailable())
    }
}

fn is_real_directory(path: &Path) -> bool {
    match std::fs::symlink_metadata(path) {
        Ok(meta) => meta.is_dir() && !metadata_is_link(&meta),
        Err(_) => false,
    }
}

fn publish_file(path: &Path, data: &[u8]) -> Result<(), AttachmentError> {
    checked_path(path)?;
    let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        return Err(storage_unavailable());
    };
    ensure_dir(parent)?;
    checked_path(path)?;
    publish_bytes(path, data)
}

fn publish_bytes(path: &Path, data: &[u8]) -> Result<(), AttachmentError> {
    #[cfg(unix)]
    {
        posix::publish_bytes(path, data)
    }
    #[cfg(windows)]
    {
        windows::publish_bytes(path, data)
    }
}

fn component_name(path: &Path) -> Result<&str, AttachmentError> {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty() && !name.contains('/') && !name.contains('\\'))
        .ok_or_else(storage_unavailable)
}

fn unique_upload_name() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    format!(".upload-{}-{n}", std::process::id())
}

fn open_leaf(path: &Path) -> Result<File, AttachmentError> {
    checked_path(path)?;
    match open_nofollow(path, OpenFlags::RDONLY, 0) {
        Ok(file) => {
            let meta = file.metadata().map_err(|_| storage_unavailable())?;
            if metadata_is_link(&meta) || !meta.is_file() {
                Err(storage_unavailable())
            } else {
                Ok(file)
            }
        }
        Err(err) if err.io_kind() == Some(io::ErrorKind::NotFound) => Err(file_not_found()),
        Err(_) => Err(storage_unavailable()),
    }
}

fn read_regular(path: &Path, max_len: u64) -> Result<Vec<u8>, AttachmentError> {
    let file = open_leaf(path)?;
    let len = file.metadata().map_err(|_| storage_unavailable())?.len();
    if len > max_len {
        return Err(storage_unavailable());
    }
    let mut data = Vec::new();
    file.take(max_len.saturating_add(1))
        .read_to_end(&mut data)
        .map_err(|_| storage_unavailable())?;
    if u64::try_from(data.len()).ok() != Some(len) {
        return Err(storage_unavailable());
    }
    Ok(data)
}

#[cfg(unix)]
mod posix {
    use std::ffi::OsStr;
    use std::fs::File;
    use std::io::{self, Write};
    use std::os::fd::OwnedFd;
    use std::path::{Component, Path};

    use rustix::fs::{AtFlags, Mode, OFlags};

    use super::{
        AttachmentError, checked_path, component_name, storage_unavailable, unique_upload_name,
    };
    use crate::securefile::{PRIVATE_FILE_MODE, make_private_fd};

    pub(super) fn publish_bytes(path: &Path, data: &[u8]) -> Result<(), AttachmentError> {
        let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        else {
            return Err(storage_unavailable());
        };
        let final_name = component_name(path)?;
        let directory = open_dir_chain(parent)?;
        if !same_directory(&directory, parent)? {
            return Err(storage_unavailable());
        }
        let (temp_name, mut file) = create_exclusive(&directory)?;
        let outcome = write_and_rename(
            &directory, parent, path, final_name, &temp_name, &mut file, data,
        );
        drop(file);
        discard_temp(&directory, &temp_name);
        outcome
    }

    pub(super) fn open_dir_chain(path: &Path) -> Result<OwnedFd, AttachmentError> {
        let mut components = path.components();
        if !matches!(components.next(), Some(Component::RootDir)) {
            return Err(storage_unavailable());
        }
        let flags = directory_flags();
        let mut current = rustix::fs::open(Path::new("/"), flags, Mode::empty())
            .map_err(|_| storage_unavailable())?;
        for component in components {
            let name = match component {
                Component::Normal(name) => name,
                Component::CurDir => OsStr::new("."),
                Component::ParentDir => OsStr::new(".."),
                Component::RootDir | Component::Prefix(_) => {
                    return Err(storage_unavailable());
                }
            };
            current = rustix::fs::openat(&current, name, flags, Mode::empty())
                .map_err(|_| storage_unavailable())?;
        }
        Ok(current)
    }

    fn write_and_rename(
        directory: &OwnedFd,
        parent: &Path,
        path: &Path,
        final_name: &str,
        temp_name: &str,
        file: &mut File,
        data: &[u8],
    ) -> Result<(), AttachmentError> {
        make_private_fd(file, PRIVATE_FILE_MODE).map_err(|_| storage_unavailable())?;
        created_in_directory(directory, file, temp_name)?;
        file.write_all(data).map_err(|_| storage_unavailable())?;
        file.sync_all().map_err(|_| storage_unavailable())?;
        checked_path(path)?;
        if !same_directory(directory, parent)? {
            return Err(storage_unavailable());
        }
        if destination_is_link(directory, final_name)? {
            return Err(storage_unavailable());
        }
        rustix::fs::renameat(directory, temp_name, directory, final_name)
            .map_err(|_| storage_unavailable())
    }

    fn create_exclusive(directory: &OwnedFd) -> Result<(String, File), AttachmentError> {
        let flags =
            OFlags::RDWR | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        for _ in 0..8 {
            let name = unique_upload_name();
            match rustix::fs::openat(
                directory,
                name.as_str(),
                flags,
                owner_mode(PRIVATE_FILE_MODE),
            ) {
                Ok(fd) => return Ok((name, File::from(fd))),
                Err(err) if io_kind(err) == io::ErrorKind::AlreadyExists => continue,
                Err(_) => return Err(storage_unavailable()),
            }
        }
        Err(storage_unavailable())
    }

    fn created_in_directory(
        directory: &OwnedFd,
        file: &File,
        name: &str,
    ) -> Result<(), AttachmentError> {
        let file_stat = rustix::fs::fstat(file).map_err(|_| storage_unavailable())?;
        let name_stat = rustix::fs::statat(directory, name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|_| storage_unavailable())?;
        if file_stat.st_dev == name_stat.st_dev && file_stat.st_ino == name_stat.st_ino {
            Ok(())
        } else {
            Err(storage_unavailable())
        }
    }

    fn same_directory(directory: &OwnedFd, path: &Path) -> Result<bool, AttachmentError> {
        let held = rustix::fs::fstat(directory).map_err(|_| storage_unavailable())?;
        let current = rustix::fs::stat(path).map_err(|_| storage_unavailable())?;
        Ok(held.st_dev == current.st_dev && held.st_ino == current.st_ino)
    }

    fn destination_is_link(directory: &OwnedFd, name: &str) -> Result<bool, AttachmentError> {
        match rustix::fs::statat(directory, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(stat) => Ok(stat_is_symlink(&stat)),
            Err(err) if io_kind(err) == io::ErrorKind::NotFound => Ok(false),
            Err(_) => Err(storage_unavailable()),
        }
    }

    fn stat_is_symlink(stat: &rustix::fs::Stat) -> bool {
        #[allow(clippy::unnecessary_cast, clippy::cast_possible_truncation)]
        let raw = stat.st_mode as rustix::fs::RawMode;
        rustix::fs::FileType::from_raw_mode(raw) == rustix::fs::FileType::Symlink
    }

    fn discard_temp(directory: &OwnedFd, name: &str) {
        let _ = rustix::fs::unlinkat(directory, name, AtFlags::empty());
    }

    fn directory_flags() -> OFlags {
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC
    }

    fn owner_mode(mode: u32) -> Mode {
        #[allow(clippy::unnecessary_cast, clippy::cast_possible_truncation)]
        let raw = mode as rustix::fs::RawMode;
        Mode::from_raw_mode(raw)
    }

    fn io_kind(err: rustix::io::Errno) -> io::ErrorKind {
        io::Error::from(err).kind()
    }
}

#[cfg(windows)]
mod windows {
    use std::fs::File;
    use std::io::{self, Write};
    use std::path::Path;

    use super::{
        AttachmentError, checked_path, component_name, metadata_is_link, storage_unavailable,
        unique_upload_name,
    };
    use crate::securefile::{
        OpenFlags, PRIVATE_FILE_MODE, PinnedDirectory, make_private_fd, pin_directory, replace,
    };

    pub(super) fn publish_bytes(path: &Path, data: &[u8]) -> Result<(), AttachmentError> {
        let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        else {
            return Err(storage_unavailable());
        };
        let final_name = component_name(path)?;
        let pinned = pin_directory(parent).map_err(|_| storage_unavailable())?;
        checked_path(path)?;
        let (temp_name, mut file) = create_exclusive(&pinned)?;
        let temp_path = pinned
            .child(&temp_name)
            .map_err(|_| storage_unavailable())?;
        let dest = pinned
            .child(final_name)
            .map_err(|_| storage_unavailable())?;
        let outcome = (|| -> Result<(), AttachmentError> {
            make_private_fd(&file, PRIVATE_FILE_MODE).map_err(|_| storage_unavailable())?;
            file.write_all(data).map_err(|_| storage_unavailable())?;
            file.sync_all().map_err(|_| storage_unavailable())?;
            Ok(())
        })();
        drop(file);
        if let Err(err) = outcome {
            let _ = std::fs::remove_file(&temp_path);
            return Err(err);
        }
        let published = (|| -> Result<(), AttachmentError> {
            checked_path(path)?;
            if path_is_link(&dest)? {
                return Err(storage_unavailable());
            }
            replace(&temp_path, &dest).map_err(|_| storage_unavailable())
        })();
        let _ = std::fs::remove_file(&temp_path);
        published
    }

    fn create_exclusive(pinned: &PinnedDirectory) -> Result<(String, File), AttachmentError> {
        let flags = OpenFlags::RDWR | OpenFlags::CREAT | OpenFlags::EXCL;
        for _ in 0..8 {
            let name = unique_upload_name();
            match pinned.open(&name, flags, PRIVATE_FILE_MODE) {
                Ok(file) => return Ok((name, file)),
                Err(err) if err.io_kind() == Some(io::ErrorKind::AlreadyExists) => continue,
                Err(_) => return Err(storage_unavailable()),
            }
        }
        Err(storage_unavailable())
    }

    fn path_is_link(path: &Path) -> Result<bool, AttachmentError> {
        match std::fs::symlink_metadata(path) {
            Ok(meta) => Ok(metadata_is_link(&meta)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(_) => Err(storage_unavailable()),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::thread;

    use serde_json::json;

    use super::*;

    fn canonical_temp() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let canonical = fs::canonicalize(dir.path()).unwrap();
        (dir, canonical)
    }

    fn scratch() -> (tempfile::TempDir, AttachmentStore) {
        let (dir, base) = canonical_temp();
        let store = AttachmentStore::new(base.join("ledger.jsonl"));
        (dir, store)
    }

    fn assert_code(err: &AttachmentError, code: &str, status: u16, message: &str) {
        assert_eq!(err.code, code, "{err}");
        assert_eq!(err.status, status);
        assert_eq!(err.message, message);
        assert_eq!(err.to_string(), message);
    }

    fn owner_dir(store: &AttachmentStore, owner: &str) -> PathBuf {
        store.root.join(sha256_hex(owner.as_bytes()))
    }

    fn walk(path: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let child = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                walk(&child, out);
            }
            out.push(child);
        }
    }

    fn names_under(path: &Path) -> Vec<String> {
        let mut found = Vec::new();
        walk(path, &mut found);
        found
            .iter()
            .filter_map(|item| {
                item.file_name()
                    .and_then(|name| name.to_str())
                    .map(str::to_owned)
            })
            .collect()
    }

    fn symlink_dir(target: &Path, link: &Path) -> bool {
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(target, link).unwrap();
            true
        }
        #[cfg(windows)]
        {
            std::os::windows::fs::symlink_dir(target, link).is_ok()
        }
    }

    fn symlink_file(target: &Path, link: &Path) -> bool {
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(target, link).unwrap();
            true
        }
        #[cfg(windows)]
        {
            std::os::windows::fs::symlink_file(target, link).is_ok()
        }
    }

    #[test]
    fn identity_matches_python_json_dumps() {
        let sha = "ab".repeat(32);
        assert_eq!(
            file_id_for("maple", &sha, "边界.txt", "text/plain"),
            "file_2f4842b2a3e6bf50acc6ca7e3326e238f0f070a9fab62e7964ae3820495abd3f"
        );
        assert_eq!(
            file_id_for(
                "maple",
                "ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f73",
                "n.txt",
                "text/plain"
            ),
            "file_7dcfd4b68dc29d130a3d1d5e6193f72abefba8a66c9c8423f3948062d1dbcdd3"
        );
        assert_eq!(
            sha256_hex(b"content"),
            "ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f73"
        );
        for sample in [
            "",
            "a\"b\\c",
            "边界.txt",
            "a\u{0001}b\u{0008}c",
            "a/b",
            "a\u{2028}b",
        ] {
            assert_eq!(json_string(sample), serde_json::to_string(sample).unwrap());
        }
    }

    #[test]
    fn suffix_matches_pure_path() {
        assert_eq!(python_suffix(".txt"), "");
        assert_eq!(python_suffix("foo."), "");
        assert_eq!(python_suffix(".hidden.txt"), ".txt");
        assert_eq!(python_suffix("..txt"), ".txt");
        assert_eq!(python_suffix("a.b.c"), ".c");
    }

    #[test]
    fn root_follows_the_ledger_parent_without_resolving_symlinks() {
        let cwd = std::env::current_dir().unwrap();
        assert_eq!(
            AttachmentStore::new("ledger.jsonl").root,
            cwd.join("attachments")
        );
        assert_eq!(
            AttachmentStore::new("~root/ledger.jsonl").root,
            cwd.join("~root").join("attachments")
        );
        #[cfg(unix)]
        {
            let store = AttachmentStore::new("/tmp/vyane-ledger/ledger.jsonl");
            assert_eq!(store.root, Path::new("/tmp/vyane-ledger/attachments"));
            let dotted = AttachmentStore::new("/tmp/vyane-a/../vyane-b/ledger.jsonl");
            assert_eq!(
                dotted.root,
                Path::new("/tmp/vyane-a/../vyane-b/attachments")
            );
            if let Some(home) = std::env::var_os("HOME").filter(|home| !home.is_empty()) {
                let home = PathBuf::from(home);
                assert_eq!(
                    AttachmentStore::new("~/ledger.jsonl").root,
                    home.join("attachments")
                );
                assert_eq!(
                    AttachmentStore::new("~/sub/ledger.jsonl").root,
                    home.join("sub").join("attachments")
                );
            }
        }
        #[cfg(windows)]
        {
            let ledger = cwd.join("vyane-ledger").join("ledger.jsonl");
            assert_eq!(
                AttachmentStore::new(&ledger).root,
                cwd.join("vyane-ledger").join("attachments")
            );
            let dotted = cwd
                .join("vyane-a")
                .join("..")
                .join("vyane-b")
                .join("ledger.jsonl");
            assert_eq!(
                AttachmentStore::new(&dotted).root,
                cwd.join("vyane-a")
                    .join("..")
                    .join("vyane-b")
                    .join("attachments")
            );
            if let Some(home) = std::env::var_os("USERPROFILE").filter(|home| !home.is_empty()) {
                let home = PathBuf::from(home);
                assert_eq!(
                    AttachmentStore::new("~/ledger.jsonl").root,
                    home.join("attachments")
                );
                assert_eq!(
                    AttachmentStore::new(r"~\sub\ledger.jsonl").root,
                    home.join("sub").join("attachments")
                );
            }
        }
    }

    #[test]
    fn upload_checks_reject_before_creating_a_directory() {
        let (_dir, store) = scratch();
        let cases = [
            (
                "",
                "text/plain",
                &b"x"[..],
                "invalid_filename",
                400,
                "文件名不合法",
            ),
            (
                ".",
                "text/plain",
                b"x",
                "invalid_filename",
                400,
                "文件名不合法",
            ),
            (
                "..",
                "text/plain",
                b"x",
                "invalid_filename",
                400,
                "文件名不合法",
            ),
            (
                "../secret.md",
                "text/markdown",
                b"x",
                "invalid_filename",
                400,
                "文件名不合法",
            ),
            (
                "a\\b.txt",
                "text/plain",
                b"x",
                "invalid_filename",
                400,
                "文件名不合法",
            ),
            (
                "bad\nname.md",
                "text/markdown",
                b"x",
                "invalid_filename",
                400,
                "文件名不合法",
            ),
            (
                "a.svg",
                "image/svg+xml",
                b"<svg/>",
                "unsupported_type",
                415,
                "文件扩展名与 MIME 类型不支持或不匹配",
            ),
            (
                "a.exe",
                "application/octet-stream",
                b"x",
                "unsupported_type",
                415,
                "文件扩展名与 MIME 类型不支持或不匹配",
            ),
            (
                "a.png",
                "text/plain",
                b"x",
                "unsupported_type",
                415,
                "文件扩展名与 MIME 类型不支持或不匹配",
            ),
            (
                ".txt",
                "text/plain",
                b"x",
                "unsupported_type",
                415,
                "文件扩展名与 MIME 类型不支持或不匹配",
            ),
            (
                "a.png",
                "image/png",
                b"not-png",
                "invalid_content",
                415,
                "文件内容与类型不匹配",
            ),
            (
                "a.pdf",
                "application/pdf",
                b"not-pdf",
                "invalid_content",
                415,
                "文件内容与类型不匹配",
            ),
            (
                "a.jpg",
                "image/jpeg",
                b"\xff\xd8\x00",
                "invalid_content",
                415,
                "文件内容与类型不匹配",
            ),
            (
                "a.gif",
                "image/gif",
                b"GIF99a",
                "invalid_content",
                415,
                "文件内容与类型不匹配",
            ),
            (
                "a.webp",
                "image/webp",
                b"RIFF\x00\x00\x00\x00WAVE",
                "invalid_content",
                415,
                "文件内容与类型不匹配",
            ),
            (
                "a.txt",
                "text/plain",
                b"\xff",
                "invalid_content",
                415,
                "文件内容与类型不匹配",
            ),
            (
                "a.txt",
                "text/plain",
                b"hello\0world",
                "invalid_content",
                415,
                "文件内容与类型不匹配",
            ),
            (
                "a.txt",
                "text/plain",
                b"",
                "empty_file",
                400,
                "不能上传空文件",
            ),
        ];
        for (name, mime, data, code, status, message) in cases {
            let err = store.put("maple", name, mime, data).unwrap_err();
            assert_code(&err, code, status, message);
            assert!(!store.root.exists(), "{name}");
        }
        let long_name = format!("{}.txt", "a".repeat(252));
        assert_eq!(long_name.chars().count(), 256);
        let err = store
            .put("maple", &long_name, "text/plain", b"x")
            .unwrap_err();
        assert_code(&err, "invalid_filename", 400, "文件名不合法");
        let boundary = format!("{}.txt", "a".repeat(251));
        assert_eq!(boundary.chars().count(), 255);
        assert!(validate_upload(&boundary, "text/plain").is_ok());
        let (name, mime) = validate_upload("Notes.TXT", " Text/Plain ; charset=utf-8 ").unwrap();
        assert_eq!(name, "Notes.TXT");
        assert_eq!(mime, "text/plain");
        assert!(validate_upload("a\u{7f}.txt", "text/plain").is_ok());
        assert!(validate_upload("..txt", "text/plain").is_ok());
    }

    #[test]
    fn put_then_get_rebuilds_public_metadata_and_round_trips_bytes() {
        let (_dir, store) = scratch();
        let data = "真正的文件内容：附件探针。".as_bytes();
        let stored = store
            .put("maple", "边界.txt", "text/plain; charset=utf-8", data)
            .unwrap();
        assert_eq!(stored.id, stored.file_id);
        assert!(is_file_id(&stored.file_id));
        assert_eq!(stored.name, "边界.txt");
        assert_eq!(stored.mime_type, "text/plain");
        assert_eq!(stored.size_bytes, data.len() as u64);
        assert_eq!(stored.sha256, sha256_hex(data));
        assert_eq!(stored.file_type, "file");
        assert_eq!(stored.url, format!("/api/attachments/{}", stored.file_id));
        assert_eq!(
            stored.file_id,
            file_id_for("maple", &stored.sha256, "边界.txt", "text/plain")
        );

        let reference = owner_dir(&store, "maple")
            .join("refs")
            .join(format!("{}.json", stored.file_id));
        let text = fs::read_to_string(&reference).unwrap();
        assert!(text.contains("边界.txt"));
        assert!(text.contains("\"type\": \"file\""));
        assert!(text.contains(&format!("\"url\": \"/api/attachments/{}\"", stored.file_id)));
        assert!(!text.contains("charset"));

        let mut tampered: serde_json::Value = serde_json::from_str(&text).unwrap();
        tampered["url"] = json!("file:///etc/passwd");
        tampered["type"] = json!("image");
        tampered["path"] = json!("/etc/passwd");
        tampered["owner"] = json!("root");
        fs::write(&reference, serde_json::to_vec(&tampered).unwrap()).unwrap();

        let (got, path, bytes) = store.get("maple", &stored.file_id).unwrap();
        assert_eq!(got, stored);
        assert_eq!(bytes, data);
        assert_eq!(path.file_name().unwrap(), stored.sha256.as_str());
        assert!(path.starts_with(&store.root));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::symlink_metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
            for directory in [
                store.root.clone(),
                owner_dir(&store, "maple"),
                path.parent().unwrap().to_path_buf(),
                reference.parent().unwrap().to_path_buf(),
            ] {
                assert_eq!(
                    fs::symlink_metadata(&directory)
                        .unwrap()
                        .permissions()
                        .mode()
                        & 0o777,
                    0o700,
                    "{directory:?}"
                );
            }
        }
    }

    #[test]
    fn magic_bytes_images_and_pdf_round_trip() {
        let (_dir, store) = scratch();
        let png = b"\x89PNG\r\n\x1a\nrest";
        let jpeg = b"\xff\xd8\xff\xe0";
        let gif = b"GIF87a";
        let webp = b"RIFF\x00\x00\x00\x00WEBP";
        let pdf = b"%PDF-1.4";
        let image = store.put("maple", "a.png", "image/png", png).unwrap();
        assert_eq!(image.file_type, "image");
        assert_eq!(store.get("maple", &image.file_id).unwrap().2, png);
        assert_eq!(
            store
                .put("maple", "a.jpg", "image/jpeg", jpeg)
                .unwrap()
                .file_type,
            "image"
        );
        assert_eq!(
            store
                .put("maple", "a.gif", "image/gif", gif)
                .unwrap()
                .mime_type,
            "image/gif"
        );
        assert!(store.put("maple", "b.gif", "image/gif", b"GIF89a").is_ok());
        assert_eq!(
            store
                .put("maple", "a.webp", "image/webp", webp)
                .unwrap()
                .file_type,
            "image"
        );
        let document = store.put("maple", "a.pdf", "application/pdf", pdf).unwrap();
        assert_eq!(document.file_type, "file");
        assert_eq!(store.get("maple", &document.file_id).unwrap().2, pdf);
    }

    #[test]
    fn size_limit_is_ten_mib_and_rejection_creates_nothing() {
        let (_dir, store) = scratch();
        let mut data = vec![b'x'; MAX_ATTACHMENT_BYTES + 1];
        let err = store
            .put("maple", "a.txt", "text/plain", &data)
            .unwrap_err();
        assert_code(&err, "file_too_large", 413, "文件不能超过 10 MiB");
        assert!(!store.root.exists());
        data.pop();
        let stored = store.put("maple", "a.txt", "text/plain", &data).unwrap();
        assert_eq!(stored.size_bytes, MAX_ATTACHMENT_BYTES as u64);
        assert_eq!(store.get("maple", &stored.file_id).unwrap().2, data);
    }

    #[test]
    fn whitespace_owner_is_rejected_and_spaced_owner_is_a_distinct_namespace() {
        let (_dir, store) = scratch();
        for owner in ["", " ", "\t", "\n", " \u{3000} ", "\u{1c}"] {
            let err = store.put(owner, "n.txt", "text/plain", b"x").unwrap_err();
            assert_code(&err, "owner_required", 403, "需要可信 owner");
            assert!(!store.root.exists(), "{owner:?}");
        }
        let err = store.put("", "n.txt", "text/plain", b"").unwrap_err();
        assert_eq!(err.code, "empty_file");
        let err = store.get(" ", "not-an-id").unwrap_err();
        assert_eq!(err.status, 404);
        let err = store.get(" ", &format!("file_{:064x}", 1)).unwrap_err();
        assert_eq!(err.status, 403);

        let spaced = store
            .put(" maple ", "n.txt", "text/plain", b"content")
            .unwrap();
        let plain = store
            .put("maple", "n.txt", "text/plain", b"content")
            .unwrap();
        assert_ne!(spaced.file_id, plain.file_id);
        assert!(
            owner_dir(&store, " maple ")
                .join("refs")
                .join(format!("{}.json", spaced.file_id))
                .is_file()
        );
        assert_eq!(store.get(" maple ", &spaced.file_id).unwrap().2, b"content");
        let missing = store.get("maple", &spaced.file_id).unwrap_err();
        assert_eq!(missing.status, 404);
    }

    #[test]
    fn same_bytes_share_a_blob_and_owners_do_not_share_a_namespace() {
        let (_dir, store) = scratch();
        let data = vec![b'x'; 32];
        let first = store
            .put("maple", "边界.txt", "text/plain; charset=utf-8", &data)
            .unwrap();
        let renamed = store
            .put("maple", "renamed.txt", "text/plain", &data)
            .unwrap();
        let foreign = store.put("other", "边界.txt", "text/plain", &data).unwrap();
        assert_ne!(first.file_id, renamed.file_id);
        assert_ne!(first.file_id, foreign.file_id);
        assert_eq!(first.sha256, renamed.sha256);
        assert_eq!(first.sha256, foreign.sha256);
        let blobs = names_under(&store.root)
            .into_iter()
            .filter(|name| name == &first.sha256)
            .count();
        assert_eq!(blobs, 2);
        assert_eq!(store.get("maple", &first.file_id).unwrap().2, data);
        let crossed = store.get("maple", &foreign.file_id).unwrap_err();
        assert_code(&crossed, "file_not_found", 404, "文件不存在");

        let quoted = store
            .put("maple", "a\"b.txt", "text/plain", b"content")
            .unwrap();
        assert_eq!(
            store.get("maple", &quoted.file_id).unwrap().0.name,
            "a\"b.txt"
        );
        let colon = store
            .put("maple", "a:b.txt", "text/plain", b"content")
            .unwrap();
        let (got, path, _) = store.get("maple", &colon.file_id).unwrap();
        assert_eq!(got.name, "a:b.txt");
        assert_ne!(path.file_name().unwrap(), "a:b.txt");
    }

    #[test]
    fn forged_cross_owner_reference_fails_closed() {
        let (_dir, store) = scratch();
        let foreign = store
            .put("other", "n.txt", "text/plain", b"private")
            .unwrap();
        let _maple = store.put("maple", "a.txt", "text/plain", b"mine").unwrap();
        let other_dir = owner_dir(&store, "other");
        let maple_dir = owner_dir(&store, "maple");
        fs::copy(
            other_dir.join("blobs").join(&foreign.sha256),
            maple_dir.join("blobs").join(&foreign.sha256),
        )
        .unwrap();
        fs::copy(
            other_dir
                .join("refs")
                .join(format!("{}.json", foreign.file_id)),
            maple_dir
                .join("refs")
                .join(format!("{}.json", foreign.file_id)),
        )
        .unwrap();
        let err = store.get("maple", &foreign.file_id).unwrap_err();
        assert_code(&err, "storage_unavailable", 503, "附件存储不可用");
        assert_eq!(store.get("other", &foreign.file_id).unwrap().2, b"private");
    }

    #[test]
    fn corrupt_reference_and_content_fail_closed_and_retry_repairs() {
        let (_dir, store) = scratch();
        let stored = store
            .put("maple", "n.txt", "text/plain", b"content")
            .unwrap();
        let reference = owner_dir(&store, "maple")
            .join("refs")
            .join(format!("{}.json", stored.file_id));
        let blob = owner_dir(&store, "maple")
            .join("blobs")
            .join(&stored.sha256);

        let mut metadata: serde_json::Value =
            serde_json::from_slice(&fs::read(&reference).unwrap()).unwrap();
        metadata["mime_type"] = json!(123);
        fs::write(&reference, serde_json::to_vec(&metadata).unwrap()).unwrap();
        let err = store.get("maple", &stored.file_id).unwrap_err();
        assert_eq!(err.status, 503);

        metadata["mime_type"] = json!("text/plain");
        metadata["size_bytes"] = json!(1);
        fs::write(&reference, serde_json::to_vec(&metadata).unwrap()).unwrap();
        assert_eq!(store.get("maple", &stored.file_id).unwrap_err().status, 503);

        fs::write(&blob, b"corrupted").unwrap();
        fs::write(&reference, metadata_bytes(&stored)).unwrap();
        assert_eq!(store.get("maple", &stored.file_id).unwrap_err().status, 503);

        let nested = format!("{}0{}", "[".repeat(1500), "]".repeat(1500));
        assert!(nested.len() <= 4096);
        fs::write(&reference, nested.as_bytes()).unwrap();
        assert_eq!(
            store.get("maple", &stored.file_id).unwrap_err().code,
            "storage_unavailable"
        );
        fs::write(&reference, vec![b'{'; 4097]).unwrap();
        assert_eq!(store.get("maple", &stored.file_id).unwrap_err().status, 503);

        let repaired = store
            .put("maple", "n.txt", "text/plain", b"content")
            .unwrap();
        assert_eq!(repaired, stored);
        assert_eq!(store.get("maple", &stored.file_id).unwrap().2, b"content");
        let refs = names_under(&owner_dir(&store, "maple").join("refs"));
        assert_eq!(
            refs.iter().filter(|name| name.ends_with(".json")).count(),
            1
        );
    }

    #[test]
    fn failed_reference_publish_keeps_the_blob_and_retry_does_not_duplicate() {
        let (_dir, store) = scratch();
        let stored = store
            .put("maple", "n.txt", "text/plain", b"content")
            .unwrap();
        let owner = owner_dir(&store, "maple");
        let refs = owner.join("refs");
        let reference = refs.join(format!("{}.json", stored.file_id));
        fs::remove_file(&reference).unwrap();
        let parked = owner.join("refs-real");
        fs::rename(&refs, &parked).unwrap();
        fs::write(&refs, b"not a directory").unwrap();

        let err = store
            .put("maple", "n.txt", "text/plain", b"content")
            .unwrap_err();
        assert_eq!(err.status, 503);
        let blob = owner.join("blobs").join(&stored.sha256);
        assert_eq!(fs::read(&blob).unwrap(), b"content");
        assert!(!reference.exists());
        let names = names_under(&store.root);
        assert!(names.iter().all(|name| !name.starts_with(".upload-")));
        assert!(names.iter().all(|name| !name.ends_with(".json")));

        fs::remove_file(&refs).unwrap();
        fs::rename(&parked, &refs).unwrap();
        let again = store
            .put("maple", "n.txt", "text/plain", b"content")
            .unwrap();
        assert_eq!(again, stored);
        assert_eq!(fs::read(&blob).unwrap(), b"content");
        assert_eq!(
            names_under(&owner.join("refs"))
                .iter()
                .filter(|name| name.ends_with(".json"))
                .count(),
            1
        );
        assert!(
            names_under(&store.root)
                .iter()
                .all(|name| !name.starts_with(".upload-"))
        );
    }

    #[test]
    fn concurrent_puts_of_the_same_bytes_deduplicate() {
        let (_dir, store) = scratch();
        let mut results = Vec::new();
        thread::scope(|scope| {
            let mut joins = Vec::new();
            for _ in 0..8 {
                joins.push(scope.spawn(|| store.put("maple", "n.txt", "text/plain", b"content")));
            }
            for join in joins {
                results.push(join.join().unwrap().unwrap());
            }
        });
        assert!(results.iter().all(|item| item == &results[0]));
        let owner = owner_dir(&store, "maple");
        assert_eq!(
            names_under(&owner.join("refs"))
                .iter()
                .filter(|name| name.ends_with(".json"))
                .count(),
            1
        );
        assert_eq!(
            names_under(&owner.join("blobs"))
                .iter()
                .filter(|name| *name == &results[0].sha256)
                .count(),
            1
        );
        assert_eq!(
            store.get("maple", &results[0].file_id).unwrap().2,
            b"content"
        );
        assert!(
            names_under(&store.root)
                .iter()
                .all(|name| !name.starts_with(".upload-"))
        );
    }

    #[test]
    fn dotdot_component_still_publishes_in_the_real_directory() {
        let (_dir, base) = canonical_temp();
        fs::create_dir(base.join("real")).unwrap();
        let ledger = base
            .join("real")
            .join("..")
            .join("real")
            .join("ledger.jsonl");
        let store = AttachmentStore::new(&ledger);
        let stored = store
            .put("maple", "n.txt", "text/plain", b"content")
            .unwrap();
        let (_, path, bytes) = store.get("maple", &stored.file_id).unwrap();
        assert_eq!(bytes, b"content");
        let canonical = fs::canonicalize(&path).unwrap();
        assert!(canonical.starts_with(base.join("real").join("attachments")));
    }

    #[test]
    fn symlink_on_the_path_chain_does_not_write_outside() {
        let (_dir, base) = canonical_temp();
        let outside = base.join("outside");
        fs::create_dir(&outside).unwrap();
        let link = base.join("link");
        if !symlink_dir(&outside, &link) {
            return;
        }
        let store = AttachmentStore::new(link.join("ledger.jsonl"));
        let err = store
            .put("maple", "n.txt", "text/plain", b"content")
            .unwrap_err();
        assert_code(&err, "storage_unavailable", 503, "附件存储不可用");
        assert!(fs::read_dir(&outside).unwrap().next().is_none());

        let (_dir, store) = scratch();
        let stored = store
            .put("maple", "n.txt", "text/plain", b"sentinel")
            .unwrap();
        let external = base.join("external.txt");
        fs::write(&external, b"original-external").unwrap();
        let blob = owner_dir(&store, "maple")
            .join("blobs")
            .join(&stored.sha256);
        fs::remove_file(&blob).unwrap();
        if !symlink_file(&external, &blob) {
            return;
        }
        let err = store.get("maple", &stored.file_id).unwrap_err();
        assert_eq!(err.status, 503);
        let err = store
            .put("maple", "n.txt", "text/plain", b"sentinel")
            .unwrap_err();
        assert_eq!(err.status, 503);
        assert_eq!(fs::read(&external).unwrap(), b"original-external");

        let owner = owner_dir(&store, "maple");
        let blobs = owner.join("blobs");
        if blobs.exists() {
            let _ = fs::remove_file(&blob);
            if blobs.is_dir() {
                let parked = owner.join("blobs-real");
                fs::rename(&blobs, &parked).unwrap();
                if symlink_dir(&outside, &blobs) {
                    let err = store
                        .put("maple", "other.txt", "text/plain", b"different")
                        .unwrap_err();
                    assert_eq!(err.status, 503);
                    assert!(fs::read_dir(&outside).unwrap().next().is_none());
                }
            }
        }
    }

    #[test]
    fn reference_ids_cap_and_alias_consistency_do_not_read_the_store() {
        let (_dir, store) = scratch();
        let stored = store
            .put("maple", "n.txt", "text/plain", b"content")
            .unwrap();
        let good = json!([{"file_id": stored.file_id}]);
        assert_eq!(
            AttachmentStore::reference_ids(&good).unwrap(),
            vec![stored.file_id.clone()]
        );
        assert_eq!(
            AttachmentStore::reference_ids(&json!([{"id": stored.file_id}])).unwrap(),
            vec![stored.file_id.clone()]
        );
        assert_eq!(
            AttachmentStore::reference_ids(&json!([{
                "id": stored.file_id,
                "file_id": stored.file_id,
                "path": "/etc/passwd",
                "name": "forged.txt"
            }]))
            .unwrap(),
            vec![stored.file_id.clone()]
        );
        let missing = format!("file_{:064x}", 2);
        assert!(AttachmentStore::reference_ids(&json!([{"id": missing}])).is_ok());
        let resolved = store.resolve("maple", &good).unwrap();
        assert_eq!(resolved, vec![stored.clone()]);

        let mut many = Vec::new();
        for _ in 0..32 {
            many.push(json!({"id": stored.file_id}));
        }
        assert_eq!(store.resolve("maple", &json!(many)).unwrap().len(), 32);
        many.push(json!({"id": stored.file_id}));
        let err = AttachmentStore::reference_ids(&json!(many)).unwrap_err();
        assert_code(
            &err,
            "invalid_attachment",
            400,
            "附件必须是最多 32 项的数组",
        );

        let err = AttachmentStore::reference_ids(&json!([{
            "id": stored.file_id,
            "file_id": format!("file_{:064x}", 3),
        }]))
        .unwrap_err();
        assert_code(&err, "invalid_attachment", 400, "附件 id 不一致");
        let err = AttachmentStore::reference_ids(&json!([stored.file_id])).unwrap_err();
        assert_code(&err, "invalid_attachment", 400, "附件必须引用 file_id");
        let err = AttachmentStore::reference_ids(&json!({"file_id": stored.file_id})).unwrap_err();
        assert_code(
            &err,
            "invalid_attachment",
            400,
            "附件必须是最多 32 项的数组",
        );
        let err = AttachmentStore::reference_ids(&json!([{"file_id": "file_ABC"}])).unwrap_err();
        assert_eq!(err.status, 404);
        let err = AttachmentStore::reference_ids(&json!([{}])).unwrap_err();
        assert_eq!(err.status, 404);
        let err = AttachmentStore::reference_ids(&json!([{"file_id": 1}])).unwrap_err();
        assert_eq!(err.status, 404);
        let err = store
            .resolve("maple", &json!([{"file_id": format!("file_{:064x}", 4)}]))
            .unwrap_err();
        assert_eq!(err.status, 404);
    }

    #[test]
    fn delivery_body_is_pure_and_uses_the_exact_wording() {
        assert_eq!(delivery_body("hello", &[]), "hello");
        assert_eq!(delivery_body("", &[]), "");
        let path = Path::new("/tmp/attachments/blob");
        let text = delivery_body(
            "hello",
            &[DeliveryAttachment {
                name: "n.txt",
                mime_type: "text/plain",
                path,
            }],
        );
        let expected = format!(
            "hello\n\n本次用户附件（本地指 ROG；请用文件工具读取）：\n附件：{{\"name\": \"n.txt\", \"mime_type\": \"text/plain\"}}\n{}",
            serde_json::to_string(&path.display().to_string()).unwrap()
        );
        assert_eq!(text, expected);

        let (_dir, store) = scratch();
        let first = store
            .put("maple", "边界.txt", "text/plain", b"one")
            .unwrap();
        let second = store.put("maple", "n.md", "text/markdown", b"two").unwrap();
        let (first_meta, first_path, _) = store.get("maple", &first.file_id).unwrap();
        let (second_meta, second_path, _) = store.get("maple", &second.file_id).unwrap();
        let rendered = delivery_body(
            "",
            &[
                DeliveryAttachment {
                    name: &first_meta.name,
                    mime_type: &first_meta.mime_type,
                    path: &first_path,
                },
                DeliveryAttachment {
                    name: &second_meta.name,
                    mime_type: &second_meta.mime_type,
                    path: &second_path,
                },
            ],
        );
        let header = "\n\n本次用户附件（本地指 ROG；请用文件工具读取）：\n";
        assert!(rendered.starts_with(header));
        assert!(
            rendered.contains("附件：{\"name\": \"边界.txt\", \"mime_type\": \"text/plain\"}\n")
        );
        assert!(
            rendered.contains("附件：{\"name\": \"n.md\", \"mime_type\": \"text/markdown\"}\n")
        );
        assert!(
            rendered.contains(&serde_json::to_string(&first_path.display().to_string()).unwrap())
        );
        assert!(
            rendered.contains(&serde_json::to_string(&second_path.display().to_string()).unwrap())
        );
        let missing = Path::new("/missing/does-not-exist");
        let pure = delivery_body(
            "body",
            &[DeliveryAttachment {
                name: "n.txt",
                mime_type: "text/plain",
                path: missing,
            }],
        );
        assert!(pure.contains(&json_string(&missing.display().to_string())));
        assert!(!missing.exists());
    }
}
