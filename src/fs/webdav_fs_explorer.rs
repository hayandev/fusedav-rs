use std::sync::Arc;

use fuser::{FileAttr, FileType};
use tokio::sync::RwLock;

use crate::webdav::WebDAVClient;

use super::{
    errors::FSError,
    inode_info_map::{InodeInfo, InodeInfoMap},
};

pub(super) struct ListItemInfo {
    pub attr: FileAttr,
    pub name: String,
}

impl ListItemInfo {
    fn new_parent_dir_from(inode_info: &InodeInfo) -> ListItemInfo {
        ListItemInfo {
            attr: inode_info.file_attr.clone(),
            name: "..".to_string(),
        }
    }

    fn new_working_dir_from(inode_info: &InodeInfo) -> ListItemInfo {
        ListItemInfo {
            attr: inode_info.file_attr.clone(),
            name: ".".to_string(),
        }
    }

    fn new_child_from(inode_info: &InodeInfo) -> ListItemInfo {
        ListItemInfo {
            attr: inode_info.file_attr,
            name: inode_info.file_name().to_string(),
        }
    }
}

#[derive(Clone)]
pub(super) struct WebDAVFSExplorer {
    client: WebDAVClient,
    inode_info_map: Arc<RwLock<InodeInfoMap>>,
}

impl WebDAVFSExplorer {
    pub fn new(client: WebDAVClient, user_id: u32, group_id: u32) -> WebDAVFSExplorer {
        WebDAVFSExplorer {
            client,
            inode_info_map: Arc::new(RwLock::new(InodeInfoMap::new(user_id, group_id))),
        }
    }

    pub async fn lookup(&mut self, parent: u64, target: &str) -> Result<InodeInfo, FSError> {
        self.update_dir_cache_if_not_exists(parent).await?;

        let inode_info_map = self.inode_info_map.read().await;
        let inode_info = inode_info_map
            .find_by_path(parent, target)
            .ok_or(FSError::FileNotFoundInInode(target.to_string()))?;
        Ok(inode_info.clone())
    }

    pub async fn list(&mut self, ino: u64) -> Result<Vec<ListItemInfo>, FSError> {
        self.update_dir_cache_if_not_exists(ino).await?;

        let inode_info_map = self.inode_info_map.read().await;

        let mut result = Vec::new();

        inode_info_map
            .find_by_ino(ino)
            .map(|x| result.push(ListItemInfo::new_working_dir_from(x)));
        inode_info_map
            .parent(ino)
            .map(|x| result.push(ListItemInfo::new_parent_dir_from(x)));

        inode_info_map
            .childs(ino)
            .ok_or(FSError::INodeNotExists)?
            .iter()
            .for_each(|x| result.push(ListItemInfo::new_child_from(x)));

        Ok(result)
    }

    pub async fn getattr(&mut self, ino: u64) -> Result<InodeInfo, FSError> {
        let inode_info_map = self.inode_info_map.read().await;
        inode_info_map
            .find_by_ino(ino)
            .map_or(Err(FSError::INodeNotExists), |x| Ok(x.clone()))
    }

    async fn update_dir_cache_if_not_exists(&mut self, ino: u64) -> Result<(), FSError> {
        let inode_info_map = self.inode_info_map.read().await;
        if inode_info_map.is_cached_dir(ino) {
            return Ok(());
        }
        drop(inode_info_map);

        let mut inode_info_map = self.inode_info_map.write().await;
        let info = &inode_info_map
            .find_by_ino(ino)
            .ok_or(FSError::INodeNotExists)?;
        match info.file_attr.kind {
            FileType::Directory => {
                let mut list = self
                    .client
                    .list(&info.path)
                    .await
                    .map_err(|e| FSError::WebDAV(e))?;

                // Note : the first item in result of webdav is current path. so, remove it.
                list.remove(0);
                inode_info_map.update_cache(ino, list);
                Ok(())
            }
            _ => Err(FSError::InvalidOperation(info.path.clone())),
        }
    }
}
