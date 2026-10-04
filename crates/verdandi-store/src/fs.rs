use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

use rustix::fs::FallocateFlags;

const TMPFS_MAGIC: u64 = 0x0102_1994;
const RAMFS_MAGIC: u64 = 0x8584_58f6;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FsStat {
    pub memory_backed: bool,
    pub total: u64,
    pub free: u64,
}

pub trait FsFile: Send + Sync + 'static {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<()>;
    fn allocate(&self, len: u64) -> io::Result<()>;
    fn set_len(&self, len: u64) -> io::Result<()>;
    fn sync_data(&self) -> io::Result<()>;
    fn size(&self) -> io::Result<u64>;
}

pub trait Fs: Send + Sync + 'static {
    type File: FsFile;
    fn create(&self, path: &Path) -> io::Result<Self::File>;
    fn open(&self, path: &Path) -> io::Result<Self::File>;
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn remove(&self, path: &Path) -> io::Result<()>;
    fn list(&self, dir: &Path) -> io::Result<Vec<String>>;
    fn statfs(&self, path: &Path) -> io::Result<FsStat>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RealFs;

pub struct RealFile(File);

impl FsFile for RealFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.0.read_exact_at(buf, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.0.write_all_at(buf, offset)
    }

    fn allocate(&self, len: u64) -> io::Result<()> {
        Ok(rustix::fs::fallocate(
            &self.0,
            FallocateFlags::empty(),
            0,
            len,
        )?)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.0.sync_data()
    }

    fn size(&self) -> io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }
}

impl Fs for RealFs {
    type File = RealFile;

    fn create(&self, path: &Path) -> io::Result<RealFile> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        Ok(RealFile(file))
    }

    fn open(&self, path: &Path) -> io::Result<RealFile> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        Ok(RealFile(file))
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        File::open(dir)?.sync_all()
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            if let Ok(name) = entry?.file_name().into_string() {
                names.push(name);
            }
        }
        Ok(names)
    }

    fn statfs(&self, path: &Path) -> io::Result<FsStat> {
        let st = rustix::fs::statfs(path)?;
        let kind = st.f_type as u64;
        let block = st.f_frsize as u64;
        Ok(FsStat {
            memory_backed: kind == TMPFS_MAGIC || kind == RAMFS_MAGIC,
            total: st.f_blocks * block,
            free: st.f_bavail * block,
        })
    }
}
