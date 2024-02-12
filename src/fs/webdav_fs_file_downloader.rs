use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use tokio::sync::Mutex;

use super::errors::FSError;
use crate::{blockfile::BlockFile, webdav::WebDAVClient};

const BLOCK_SIZE: u32 = 16 * 1024 * 1024;

#[derive(Clone)]
pub(super) struct WebDAVFSFileHandle {
    real_path: String,
    mutex: Arc<Mutex<PhantomData<bool>>>,
}

impl WebDAVFSFileHandle {
    pub fn new(real_path: String) -> Self {
        WebDAVFSFileHandle {
            real_path,
            mutex: Arc::new(Mutex::new(PhantomData)),
        }
    }

    pub async fn get_file(&self) -> Result<BlockFile, FSError> {
        BlockFile::open(&self.real_path, false)
            .await
            .map_err(|err| FSError::IO(err))
    }

    async fn get_file_for_write(&self) -> Result<BlockFile, FSError> {
        BlockFile::open(&self.real_path, true)
            .await
            .map_err(|err| FSError::IO(err))
    }
}

#[derive(Clone)]
pub(super) struct WebDAVFSFileDownloader {
    client: WebDAVClient,
    temp_path: String,

    path_to_cache_map: Arc<Mutex<HashMap<String, WebDAVFSFileHandle>>>,
}

impl WebDAVFSFileDownloader {
    pub fn new(client: WebDAVClient, temp_path: String) -> Self {
        WebDAVFSFileDownloader {
            client,
            temp_path,
            path_to_cache_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn download(
        &self,
        uri_path: &str,
        file_size: u64,
        offset: u64,
        size: u32,
    ) -> Result<WebDAVFSFileHandle, FSError> {
        let mut path_to_cache_map = self.path_to_cache_map.lock().await;

        let handle = path_to_cache_map.get(uri_path);
        let (handle, mut file) = match handle {
            Some(handle) => {
                let mut file = handle.get_file_for_write().await?;
                if file
                    .is_data_ready(offset, size as u64)
                    .await
                    .map_err(|err| FSError::IO(err))?
                {
                    return Ok(handle.clone());
                }
                (handle.clone(), file)
            }
            None => {
                let temp_path = self.gen_temp_path();
                let file = BlockFile::create(&temp_path, file_size, BLOCK_SIZE)
                    .await
                    .map_err(|err| FSError::IO(err))?;

                let file_handle = WebDAVFSFileHandle::new(temp_path.clone());
                path_to_cache_map.insert(uri_path.to_string(), file_handle.clone());
                (file_handle, file)
            }
        };
        
        let _ = handle.mutex.lock().await;
        drop(path_to_cache_map);

        let (begin, end) = file.calc_block_range_from(offset, size as u64);
        self.client
            .download(uri_path, &mut file, begin, end - begin)
            .await
            .map_err(|x| FSError::WebDAV(x))?;
        Ok(handle)
    }

    fn gen_temp_path(&self) -> String {
        let uuid = uuid::Uuid::new_v4();
        std::path::Path::new(&self.temp_path)
            .join(uuid.to_string())
            .to_str()
            .unwrap()
            .to_string()
    }
}
