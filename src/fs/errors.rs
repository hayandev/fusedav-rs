use std::io;

use crate::webdav::{self};

#[derive(Debug)]
pub enum FSError {
    WebDAV(webdav::Error),
    IO(io::Error),
    INodeNotExists,
    FileNotFoundInInode(String),
    InvalidOperation(String),
}
