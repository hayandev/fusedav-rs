use core::time;

use fuser::Filesystem;
use libc::ENOENT;
use tokio::runtime::Handle;

use super::{
    webdav_fs_file_downloader::WebDAVFSFileDownloader,
    webdav_fs_explorer::WebDAVFSExplorer,
};
use crate::webdav::WebDAVClient;

pub struct WebDAVFS {
    tokio_handle: Handle,
    explorer: WebDAVFSExplorer,
    downloader: WebDAVFSFileDownloader,
}

impl WebDAVFS {
    pub fn new(
        tokio_handle: Handle,
        client: WebDAVClient,
        temp_path: String,
        user_id: u32,
        group_id: u32,
    ) -> WebDAVFS {
        let explorer = WebDAVFSExplorer::new(client.clone(), user_id, group_id);
        let downloader = WebDAVFSFileDownloader::new(client, temp_path);
        WebDAVFS {
            tokio_handle,
            explorer,
            downloader,
        }
    }
}

impl Filesystem for WebDAVFS {
    fn lookup(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let mut explorer = self.explorer.clone();
        let name = name.to_os_string();
        self.tokio_handle.spawn(async move {
            match explorer.lookup(parent, name.to_str().unwrap()).await {
                Ok(info) => {
                    let ttl = time::Duration::from_secs(1);
                    reply.entry(&ttl, &info.file_attr, 0);
                }
                Err(e) => {
                    eprintln!("Lookup Error: {:?}", e);
                    reply.error(ENOENT);
                }
            }
        });
    }

    fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        let mut explorer = self.explorer.clone();
        self.tokio_handle.spawn(async move {
            match explorer.getattr(ino).await {
                Ok(info) => {
                    let ttl = time::Duration::from_secs(1);
                    reply.attr(&ttl, &info.file_attr);
                }
                Err(e) => {
                    eprintln!("Getattr Error: {:?}", e);
                    reply.error(ENOENT);
                }
            }
        });
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let downloader = self.downloader.clone();
        let mut explorer = self.explorer.clone();
        self.tokio_handle.spawn(async move {
            let attr_result = explorer.getattr(ino).await;
            if attr_result.is_err() {
                eprintln!("Get attr error: {:?}", attr_result.unwrap_err());
                reply.error(ENOENT);
                return;
            }

            let attr = attr_result.unwrap();
            let file_handle_result = downloader
                .download(&attr.path, attr.file_attr.size, offset as u64, size)
                .await;
            if file_handle_result.is_err() {
                eprintln!(
                    "Get file handle error: {:?}",
                    file_handle_result.err().unwrap()
                );
                reply.error(ENOENT);
                return;
            }

            let file_handle = file_handle_result.unwrap();
            let file = file_handle.get_file().await;
            if file.is_err() {
                eprintln!("Can not file open : {:?}", file.err().unwrap());
                reply.error(ENOENT);
                return;
            }

            let mut file = file.unwrap();
            let mut buf = vec![0; size as usize];
            let result = file.read(&mut buf, offset as u64).await;
            if result.is_err() {
                eprintln!("Read error: {:?}", result.unwrap_err());
                reply.error(ENOENT);
                return;
            }
            reply.data(&buf);
        });
    }

    fn opendir(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        _flags: i32,
        reply: fuser::ReplyOpen,
    ) {
        reply.opened(0, 0);
    }

    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        let mut explorer = self.explorer.clone();
        self.tokio_handle.spawn(async move {
            let list = explorer.list(ino).await;
            match list {
                Ok(list) => {
                    for (i, info) in list.into_iter().enumerate().skip(offset as usize) {
                        if reply.add(info.attr.ino, (i + 1) as i64, info.attr.kind, info.name) {
                            break;
                        };
                    }
                    reply.ok();
                }
                Err(e) => {
                    eprintln!("Readdir Error: {:?}", e);
                    reply.error(ENOENT);
                    return;
                }
            }
        });
    }
}
