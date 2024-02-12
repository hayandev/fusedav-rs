use std::{
    fmt::Display,
    string::FromUtf8Error,
};

use chrono::{DateTime, Utc};
use reqwest_dav::list_cmd::ListEntity;
use urlencoding::decode;

use crate::blockfile::BlockFile;

#[derive(Debug, Clone)]
pub enum WebDAVList {
    File(WebDAVFile),
    Folder(WebDAVDirectory),
    Err,
}

#[derive(Debug, Clone)]
pub struct WebDAVFile {
    pub href: String,
    pub path: String,
    pub last_modified: DateTime<Utc>,
    pub content_length: u64,
    pub content_type: String,
}

#[derive(Debug, Clone)]
pub struct WebDAVDirectory {
    pub href: String,
    pub path: String,
    pub last_modified: DateTime<Utc>,
    pub quota_used_bytes: Option<u64>,
    pub quota_available_bytes: Option<u64>,
}

#[derive(Debug)]
pub enum Error {
    ReqwestDAV(reqwest_dav::Error),
    IO(std::io::Error),
    EncodingError(FromUtf8Error),
}

#[derive(Clone)]
pub struct WebDAVClient {
    client: reqwest_dav::Client,
}

impl WebDAVClient {
    pub fn new(url: String, user: String, password: String) -> Result<WebDAVClient, Error> {
        let mut url = url;
        if url.ends_with("/") {
            url.remove(url.len() - 1);
        }

        let client = reqwest_dav::ClientBuilder::new()
            .set_auth(reqwest_dav::Auth::Basic(user, password))
            .set_host(url)
            .build()
            .map_err(|e| Error::ReqwestDAV(e))?;
        Ok(WebDAVClient { client })
    }

    pub async fn list(&self, path: &str) -> Result<Vec<WebDAVList>, Error> {
        let result = self
            .client
            .list(path, reqwest_dav::Depth::Number(1))
            .await
            .map_err(|e| Error::ReqwestDAV(e))?;

        result
            .into_iter()
            .map(|x| WebDAVList::try_from(&self.client.host, x))
            .collect()
    }

    pub async fn download(
        &self,
        path: &str,
        file: &mut BlockFile,
        offset: u64,
        size: u64,
    ) -> Result<(), Error> {
        let mut response = self
            .client
            .get_range(path, offset, size)
            .await
            .map_err(|e| Error::ReqwestDAV(e))?;

        let file_size: u64 = response.headers().get("Content-Range").map_or(0, |v| {
            v.to_str()
                .unwrap()
                .split("/")
                .last()
                .unwrap()
                .parse()
                .unwrap()
        });

        if file_size == 0 {
            return Ok(());
        }

        let mut offset = offset;
        loop {
            let chunk_result = response.chunk().await;
            match chunk_result {
                Ok(chunk) => match chunk {
                    Some(chunk) => {
                        let write_result = file.write(&chunk, offset).await;
                        if let Err(err) = write_result {
                            return Err(Error::IO(err));
                        }
                        offset += write_result.unwrap() as u64;
                    }
                    None => {
                        break;
                    }
                },
                Err(err) => {
                    return Err(Error::ReqwestDAV(reqwest_dav::Error::Reqwest(err)));
                }
            }
        }
        Ok(())
    }
}

impl WebDAVList {
    fn try_from(root: &str, value: ListEntity) -> Result<WebDAVList, Error> {
        match value {
            ListEntity::File(f) => {
                let href = f.href.replace(root, "");
                let path = decode(&href)
                    .map_err(|e| Error::EncodingError(e))?
                    .to_string();

                Ok(WebDAVList::File(WebDAVFile {
                    href: f.href,
                    path: path,
                    last_modified: f.last_modified,
                    content_length: f.content_length as u64,
                    content_type: f.content_type,
                }))
            }
            ListEntity::Folder(f) => {
                let href = f.href.replace(root, "");
                let path = decode(&href)
                    .map_err(|e| Error::EncodingError(e))?
                    .to_string();

                Ok(WebDAVList::Folder(WebDAVDirectory {
                    href: f.href,
                    path: path,
                    last_modified: f.last_modified,
                    quota_used_bytes: f.quota_used_bytes.map_or(None, |x| Some(x as u64)),
                    quota_available_bytes: f.quota_available_bytes.map_or(None, |x| Some(x as u64)),
                }))
            }
            _ => Ok(WebDAVList::Err),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::ReqwestDAV(e) => write!(f, "WebDAVLibError: {}", e),
            Error::EncodingError(e) => write!(f, "EncodingError: {}", e),
            Error::IO(e) => write!(f, "IOError: {}", e),
        }
    }
}

impl std::error::Error for Error {}
