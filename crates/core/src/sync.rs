use std::fs::File;
use std::io;

#[cfg(target_os = "macos")]
pub(crate) fn fsync(file: &File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;

    // On macOS, `fsync` may stop at the drive cache. `F_FULLFSYNC` asks the
    // drive to flush data to stable storage.
    //
    // SAFETY: `file.as_raw_fd()` is valid while `file` is borrowed, and
    // `F_FULLFSYNC` does not take an extra argument.
    let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) };
    if ret == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
pub(crate) fn fsync(file: &File) -> io::Result<()> {
    file.sync_all()
}
