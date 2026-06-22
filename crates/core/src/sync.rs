use std::fs::File;
use std::io;

#[cfg(target_os = "macos")]
pub(crate) fn fsync(file: &File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;

    // Plain fsync on macOS reaches only the drive cache; F_FULLFSYNC (which subsumes
    // fsync) is the power-loss barrier.
    // SAFETY: the fd is valid for the borrow of `file`; F_FULLFSYNC takes no argument.
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
