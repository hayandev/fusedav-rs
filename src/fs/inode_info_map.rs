use std::{
    collections::{hash_map::Entry, HashMap},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use fuser::{FileAttr, FileType};

use crate::webdav::WebDAVList;

#[derive(Debug, Clone)]
pub(super) struct InodeInfo {
    pub file_attr: FileAttr,
    pub path: String,
}

impl InodeInfo {
    pub fn new(file_attr: FileAttr, path: String) -> InodeInfo {
        InodeInfo { file_attr, path }
    }

    pub fn file_name(&self) -> &str {
        if self.path == "/" {
            "/"
        } else {
            Path::new(&self.path).file_name().unwrap().to_str().unwrap()
        }
    }
}

pub(super) struct InodeInfoMap {
    ino_info_map: HashMap<u64, InodeInfo>,
    ino_item_list_map: HashMap<u64, Vec<u64>>,
    ino_parent_map: HashMap<u64, u64>,

    next_ino_id: u64,
    user_id: u32,
    group_id: u32,
}

impl InodeInfoMap {
    pub fn new(user_id: u32, group_id: u32) -> InodeInfoMap {
        let root = InodeInfo::new(
            InodeInfoMap::root_directory_attr(user_id, group_id),
            "/".to_string(),
        );
        InodeInfoMap {
            ino_info_map: HashMap::from([(1, root)]),
            ino_item_list_map: HashMap::new(),
            ino_parent_map: HashMap::from([(1, 1)]),

            next_ino_id: 2,
            user_id: user_id,
            group_id: group_id,
        }
    }

    pub fn find_by_path(&self, parent: u64, target: &str) -> Option<&InodeInfo> {
        let empty_vec = Vec::new();
        let ino_item_list = self
            .ino_item_list_map
            .get(&parent)
            .map_or(&empty_vec, |x| x);

        ino_item_list
            .iter()
            .find_map(|x| match self.ino_info_map.get(x) {
                Some(inode_info) => {
                    if inode_info.file_name() == target {
                        Some(inode_info)
                    } else {
                        None
                    }
                }
                None => None,
            })
    }

    pub fn find_by_ino(&self, ino: u64) -> Option<&InodeInfo> {
        self.ino_info_map.get(&ino)
    }

    pub fn parent(&self, ino: u64) -> Option<&InodeInfo> {
        self.ino_parent_map
            .get(&ino)
            .and_then(|x| self.ino_info_map.get(x))
    }

    pub fn is_cached_dir(&self, ino: u64) -> bool {
        self.ino_item_list_map.contains_key(&ino)
    }

    pub fn childs(&self, ino: u64) -> Option<Vec<&InodeInfo>> {
        if let Some(ino_item_list) = self.ino_item_list_map.get(&ino) {
            let mut result = Vec::new();
            for item_ino in ino_item_list {
                if let Some(attr) = self.ino_info_map.get(item_ino) {
                    result.push(attr);
                }
            }
            Some(result)
        } else {
            None
        }
    }

    pub fn update_cache(&mut self, current_ino: u64, list: Vec<WebDAVList>) {
        let mut list = list
            .iter()
            .filter(|x| match x {
                WebDAVList::Err => false,
                _ => true,
            })
            .collect::<Vec<&WebDAVList>>();
        list.sort_by(Self::sort_webdav_list);

        for item in list {
            if let Some(inode_info) = self.convert_web_dav_list_to_file_attr(item) {
                let ino_item_list: &mut Vec<u64> = match self.ino_item_list_map.entry(current_ino) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => entry.insert(Vec::new()),
                };

                ino_item_list.push(inode_info.file_attr.ino);
                self.ino_parent_map
                    .insert(inode_info.file_attr.ino, current_ino);
                self.ino_info_map
                    .insert(inode_info.file_attr.ino, inode_info);
            }
        }
    }

    fn convert_web_dav_list_to_file_attr(&mut self, item: &WebDAVList) -> Option<InodeInfo> {
        if let WebDAVList::Err = item {
            return None;
        }

        let next_inode_id = self.next_ino_id;
        self.next_ino_id += 1;
        match item {
            WebDAVList::File(f) => Some(InodeInfo::new(
                FileAttr {
                    ino: next_inode_id,
                    size: f.content_length,
                    blocks: 0,
                    atime: SystemTime::now(),
                    mtime: UNIX_EPOCH
                        + std::time::Duration::from_secs(f.last_modified.timestamp() as u64),
                    ctime: UNIX_EPOCH
                        + std::time::Duration::from_secs(f.last_modified.timestamp() as u64),
                    crtime: UNIX_EPOCH
                        + std::time::Duration::from_secs(f.last_modified.timestamp() as u64),
                    kind: FileType::RegularFile,
                    perm: 0o664,
                    nlink: 2,
                    uid: self.user_id,
                    gid: self.group_id,
                    rdev: 0,
                    flags: 0,
                    blksize: 512,
                },
                f.path.clone(),
            )),
            WebDAVList::Folder(d) => Some(InodeInfo::new(
                FileAttr {
                    ino: next_inode_id,
                    size: d.quota_used_bytes.map_or(4096, |x| x as u64),
                    blocks: 0,
                    atime: SystemTime::now(),
                    mtime: UNIX_EPOCH
                        + std::time::Duration::from_secs(d.last_modified.timestamp() as u64),
                    ctime: UNIX_EPOCH
                        + std::time::Duration::from_secs(d.last_modified.timestamp() as u64),
                    crtime: UNIX_EPOCH
                        + std::time::Duration::from_secs(d.last_modified.timestamp() as u64),
                    kind: FileType::Directory,
                    perm: 0o755,
                    nlink: 2,
                    uid: self.user_id,
                    gid: self.group_id,
                    rdev: 0,
                    flags: 0,
                    blksize: 512,
                },
                d.path.clone(),
            )),
            _ => None,
        }
    }

    fn sort_webdav_list(l: &&WebDAVList, r: &&WebDAVList) -> std::cmp::Ordering {
        let lpath = match l {
            WebDAVList::File(f) => &f.path,
            WebDAVList::Folder(d) => &d.path,
            WebDAVList::Err => "",
        };
        let rpath = match r {
            WebDAVList::File(f) => &f.path,
            WebDAVList::Folder(d) => &d.path,
            WebDAVList::Err => "",
        };
        lpath.cmp(rpath)
    }

    fn root_directory_attr(user_id: u32, group_id: u32) -> FileAttr {
        return FileAttr {
            ino: 1,
            size: 4096,
            blocks: 0,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            crtime: SystemTime::now(),
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: user_id,
            gid: group_id,
            rdev: 0,
            blksize: 512,
            flags: 0,
        };
    }
}
