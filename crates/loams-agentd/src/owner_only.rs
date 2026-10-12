//! Owner-only files and directories (design §50 §5.2 and §7).
//!
//! On Unix a directory is 0700 and a file 0600. On Windows each gets a
//! protected DACL that grants full access to the current user and SYSTEM
//! only; a directory's entries are inheritable, so files created in it start
//! owner-only too.

use std::io;
use std::path::Path;

/// Create `dir` (and missing parents) and make it owner-only. An existing
/// directory is tightened, in case an older build or the user widened it.
pub fn create_dir(dir: &Path) -> io::Result<()> {
    if let Some(parent) = dir.parent() {
        std::fs::create_dir_all(parent)?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
        match std::fs::DirBuilder::new().mode(0o700).create(dir) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists && dir.is_dir() => {}
            Err(error) => return Err(error),
        }
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
    }
    #[cfg(windows)]
    {
        std::fs::create_dir_all(dir)?;
        windows::restrict(dir, true)
    }
    #[cfg(not(any(unix, windows)))]
    {
        std::fs::create_dir_all(dir)
    }
}

/// Replace `path` with `bytes` atomically: a sibling temporary file is
/// written owner-only, flushed, then renamed over `path`. A reader sees the
/// old content or the new, never a partial file.
pub fn write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no parent directory"))?;
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no file name"))?;
    let mut temp_name = std::ffi::OsString::from(".");
    temp_name.push(name);
    temp_name.push(format!(".{}.tmp", std::process::id()));
    let temp = dir.join(temp_name);
    let result = write_new(&temp, bytes).and_then(|()| std::fs::rename(&temp, path));
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

fn write_new(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write;
    // A temporary file left by a crash of this pid is ours to replace.
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    #[cfg(windows)]
    windows::restrict(path, false)?;
    file.write_all(bytes)?;
    file.sync_all()
}

#[cfg(windows)]
pub use windows::{current_user_sid, dacl_sids};

#[cfg(windows)]
mod windows {
    use std::io;
    use std::os::windows::ffi::OsStrExt;
    use std::path::Path;

    use windows_sys::Win32::Foundation::{CloseHandle, ERROR_SUCCESS, HANDLE, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
        GetNamedSecurityInfoW, SDDL_REVISION_1, SE_FILE_OBJECT, SetNamedSecurityInfoW,
    };
    use windows_sys::Win32::Security::{
        ACCESS_ALLOWED_ACE, ACL, DACL_SECURITY_INFORMATION, GetAce, GetSecurityDescriptorDacl,
        GetTokenInformation, PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID,
        TOKEN_QUERY, TOKEN_USER, TokenUser,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    fn wide(text: &std::ffi::OsStr) -> Vec<u16> {
        text.encode_wide().chain(std::iter::once(0)).collect()
    }

    /// Read a NUL-terminated UTF-16 string that `LocalAlloc` returned, then free it.
    ///
    /// SAFETY (caller): `text` is a valid NUL-terminated string from `LocalAlloc`.
    #[allow(unsafe_code)]
    unsafe fn take_local_string(text: *mut u16) -> String {
        // SAFETY: the caller guarantees a NUL-terminated buffer; the loop stops at the NUL.
        let len = (0..).take_while(|&i| unsafe { *text.add(i) } != 0).count();
        // SAFETY: `len` units were just read from the same buffer.
        let value = String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(text, len) });
        // SAFETY: the buffer came from LocalAlloc and is not used again.
        unsafe { LocalFree(text.cast()) };
        value
    }

    /// The current user's SID in string form (`S-1-5-21-…`).
    #[allow(unsafe_code)]
    pub fn current_user_sid() -> io::Result<String> {
        let mut token: HANDLE = std::ptr::null_mut();
        // SAFETY: the pseudo-handle of this process and an out pointer to a local.
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let mut len = 0u32;
        // SAFETY: a size query (null buffer, length 0) on the token just opened.
        unsafe { GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &mut len) };
        // u64 storage keeps TOKEN_USER's pointer field aligned.
        let mut buffer = vec![0u64; (len as usize).div_ceil(8).max(1)];
        // SAFETY: the buffer holds at least `len` bytes, as the size query asked.
        let ok = unsafe {
            GetTokenInformation(token, TokenUser, buffer.as_mut_ptr().cast(), len, &mut len)
        };
        let error = io::Error::last_os_error();
        // SAFETY: closes the token handle opened above, once.
        unsafe { CloseHandle(token) };
        if ok == 0 {
            return Err(error);
        }
        // SAFETY: GetTokenInformation(TokenUser) filled the aligned buffer with a TOKEN_USER
        // whose SID points into the same buffer, which outlives this use.
        let sid = unsafe { (*buffer.as_ptr().cast::<TOKEN_USER>()).User.Sid };
        sid_string(sid)
    }

    #[allow(unsafe_code)]
    fn sid_string(sid: PSID) -> io::Result<String> {
        let mut text: *mut u16 = std::ptr::null_mut();
        // SAFETY: `sid` is a valid SID; `text` receives a LocalAlloc'd string.
        if unsafe { ConvertSidToStringSidW(sid, &mut text) } == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: ConvertSidToStringSidW returned a NUL-terminated LocalAlloc'd string.
        Ok(unsafe { take_local_string(text) })
    }

    /// Replace `path`'s DACL with a protected one: full access for the current
    /// user and SYSTEM, inherited by a directory's children when `inherit`.
    #[allow(unsafe_code)]
    pub fn restrict(path: &Path, inherit: bool) -> io::Result<()> {
        let user = current_user_sid()?;
        let flags = if inherit { "OICI" } else { "" };
        let sddl = format!("D:P(A;{flags};FA;;;{user})(A;{flags};FA;;;SY)");
        let sddl = wide(std::ffi::OsStr::new(&sddl));
        let mut descriptor: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        // SAFETY: a NUL-terminated SDDL string; `descriptor` receives a LocalAlloc'd buffer.
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        let mut present = 0;
        let mut defaulted = 0;
        let mut dacl: *mut ACL = std::ptr::null_mut();
        // SAFETY: `descriptor` is the valid descriptor converted above.
        let result = if unsafe {
            GetSecurityDescriptorDacl(descriptor, &mut present, &mut dacl, &mut defaulted)
        } == 0
        {
            Err(io::Error::last_os_error())
        } else {
            let name = wide(path.as_os_str());
            // SAFETY: a NUL-terminated path and the DACL inside `descriptor`, alive here.
            let status = unsafe {
                SetNamedSecurityInfoW(
                    name.as_ptr(),
                    SE_FILE_OBJECT,
                    DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    dacl,
                    std::ptr::null(),
                )
            };
            if status == ERROR_SUCCESS {
                Ok(())
            } else {
                Err(io::Error::from_raw_os_error(status as i32))
            }
        };
        // SAFETY: frees the LocalAlloc'd descriptor once; `dacl` pointed into it and is dead.
        unsafe { LocalFree(descriptor) };
        result
    }

    /// The SIDs of the access-allowed entries in `path`'s DACL, as strings.
    #[allow(unsafe_code)]
    pub fn dacl_sids(path: &Path) -> io::Result<Vec<String>> {
        let name = wide(path.as_os_str());
        let mut dacl: *mut ACL = std::ptr::null_mut();
        let mut descriptor: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        // SAFETY: a NUL-terminated path; out pointers to locals.
        let status = unsafe {
            GetNamedSecurityInfoW(
                name.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut dacl,
                std::ptr::null_mut(),
                &mut descriptor,
            )
        };
        if status != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(status as i32));
        }
        let mut sids = Vec::new();
        let mut result = Ok(());
        // SAFETY: `dacl` points into `descriptor`, which is alive until LocalFree below.
        let count = if dacl.is_null() {
            0
        } else {
            unsafe { (*dacl).AceCount }
        };
        for index in 0..u32::from(count) {
            let mut ace: *mut std::ffi::c_void = std::ptr::null_mut();
            // SAFETY: `index` is below the DACL's AceCount.
            if unsafe { GetAce(dacl, index, &mut ace) } == 0 {
                result = Err(io::Error::last_os_error());
                break;
            }
            let ace = ace.cast::<ACCESS_ALLOWED_ACE>();
            // SAFETY: every ACE starts with an ACE_HEADER; an access-allowed ACE (type 0)
            // stores its SID from `SidStart`.
            let sid = unsafe {
                if (*ace).Header.AceType != 0 {
                    continue;
                }
                std::ptr::addr_of_mut!((*ace).SidStart).cast::<std::ffi::c_void>()
            };
            match sid_string(sid) {
                Ok(sid) => sids.push(sid),
                Err(error) => {
                    result = Err(error);
                    break;
                }
            }
        }
        // SAFETY: frees the descriptor GetNamedSecurityInfoW allocated, once.
        unsafe { LocalFree(descriptor) };
        result.map(|()| sids)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    fn mode(path: &std::path::Path) -> u32 {
        std::fs::metadata(path)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777
    }

    #[test]
    fn a_wide_directory_is_tightened() {
        let root = tempfile::tempdir().expect("temp dir");
        let dir = root.path().join("agentd");
        std::fs::create_dir(&dir).expect("create");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        super::create_dir(&dir).expect("create_dir");
        assert_eq!(mode(&dir), 0o700);
    }

    #[test]
    fn write_replaces_a_wide_file_with_an_owner_only_one() {
        let root = tempfile::tempdir().expect("temp dir");
        let path = root.path().join("agentd.token");
        std::fs::write(&path, b"old").expect("write");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");
        super::write(&path, b"new").expect("write");
        assert_eq!(std::fs::read(&path).expect("read"), b"new");
        assert_eq!(mode(&path), 0o600);
        let leftovers: Vec<_> = std::fs::read_dir(root.path())
            .expect("list")
            .flatten()
            .map(|entry| entry.file_name())
            .filter(|name| name != "agentd.token")
            .collect();
        assert!(leftovers.is_empty(), "temporary files left: {leftovers:?}");
    }
}
