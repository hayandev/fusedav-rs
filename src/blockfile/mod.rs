use std::io::SeekFrom;

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

const FILE_FORMAT_SIGNATURE: &[u8] = b"FDrs";

struct BlockInfo {
    block_info_index: u32,

    pub used: bool,
    pub block_index: u32,
    pub usage: u32,
}

impl BlockInfo {
    async fn from(file: &mut File, index: u32) -> std::io::Result<BlockInfo> {
        file.seek(SeekFrom::Start(
            BlockFileHeader::first_block_info_start_pos() + BlockInfo::size() * index as u64,
        ))
        .await?;

        let used = file.read_u8().await? == 1;
        let block_index = file.read_u32().await?;
        let usage = file.read_u32().await?;
        Ok(BlockInfo {
            block_info_index: index,
            used,
            block_index,
            usage,
        })
    }

    async fn write(&self, file: &mut File) -> std::io::Result<()> {
        file.seek(SeekFrom::Start(
            BlockFileHeader::first_block_info_start_pos()
                + BlockInfo::size() * self.block_info_index as u64,
        ))
        .await?;

        file.write_u8(self.used as u8).await?;
        file.write_u32(self.block_index).await?;
        file.write_u32(self.usage).await?;
        Ok(())
    }

    async fn reload(&mut self, file: &mut File) -> std::io::Result<()> {
        *self = BlockInfo::from(file, self.block_info_index).await?;
        Ok(())
    }

    fn size() -> u64 {
        9
    }
}

struct BlockFileHeader {
    block_info_list: Vec<BlockInfo>,

    file_size: u64,
    block_size: u32,
    next_block_index: u32,
}

impl BlockFileHeader {
    fn new(file_size: u64, block_size: u32) -> Self {
        let block_count = (file_size as f64 / block_size as f64).ceil() as usize;
        let empty_blocks = (0..block_count)
            .map(|index| BlockInfo {
                block_info_index: index as u32,
                used: false,
                block_index: 0,
                usage: 0,
            })
            .collect();

        BlockFileHeader {
            block_info_list: empty_blocks,
            file_size,
            block_size,
            next_block_index: 0,
        }
    }

    async fn from(file: &mut File) -> std::io::Result<BlockFileHeader> {
        BlockFileHeader::validate_file_header(file).await?;
        let file_size = BlockFileHeader::read_file_size_from(file).await?;
        let block_size = BlockFileHeader::read_block_size_from(file).await?;
        let block_info_list = BlockFileHeader::read_block_info_list_from(file).await?;
        let next_block_index = BlockFileHeader::find_last_block_index(&block_info_list)
            .map_or(0, |last_block_index| last_block_index + 1);

        Ok(BlockFileHeader {
            block_info_list,
            file_size,
            block_size,
            next_block_index,
        })
    }

    async fn write_file_header(&self, file: &mut File) -> std::io::Result<()> {
        file.write_all(&FILE_FORMAT_SIGNATURE).await?;
        file.write_u64(self.file_size).await?;
        file.write_u32(self.block_size).await?;
        BlockFileHeader::write_all_block_info(self, file).await?;
        file.flush().await?;
        Ok(())
    }

    async fn validate_file_header(file: &mut File) -> std::io::Result<()> {
        file.seek(SeekFrom::Start(BlockFileHeader::signatire_pose()))
            .await?;

        let mut header = [0u8; 4];
        file.read(&mut header).await?;

        if header != FILE_FORMAT_SIGNATURE {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid file format",
            ))
        } else {
            Ok(())
        }
    }

    async fn read_file_size_from(file: &mut File) -> std::io::Result<u64> {
        file.seek(SeekFrom::Start(BlockFileHeader::file_size_pos()))
            .await?;
        file.read_u64().await
    }

    async fn read_block_size_from(file: &mut File) -> std::io::Result<u32> {
        file.seek(SeekFrom::Start(BlockFileHeader::block_size_pose()))
            .await?;
        file.read_u32().await
    }

    async fn read_block_info_list_from(file: &mut File) -> std::io::Result<Vec<BlockInfo>> {
        file.seek(SeekFrom::Start(
            BlockFileHeader::block_info_list_len_start_pos(),
        ))
        .await?;

        let block_info_list_len = file.read_u32().await? as usize;
        let mut block_info_list = Vec::with_capacity(block_info_list_len);
        for i in 0..block_info_list_len {
            block_info_list.push(BlockInfo::from(file, i as u32).await?);
        }
        Ok(block_info_list)
    }

    async fn write_all_block_info(&self, file: &mut File) -> std::io::Result<()> {
        file.seek(SeekFrom::Start(
            BlockFileHeader::block_info_list_len_start_pos(),
        ))
        .await?;

        file.write_u32(self.block_info_list.len() as u32).await?;
        for index in 0..self.block_info_list.len() {
            let block_info = &self.block_info_list[index];
            block_info.write(file).await?;
        }
        Ok(())
    }

    fn get_header_size(&self) -> u64 {
        BlockFileHeader::first_block_info_start_pos()
            + self.block_info_list.len() as u64 * BlockInfo::size()
    }

    fn get_mut_block_info(&mut self, pos: u64) -> std::io::Result<&mut BlockInfo> {
        BlockFileHeader::get_mut_block_info_from(
            &mut self.block_info_list,
            self.block_size as u64,
            pos,
        )
    }

    fn get_mut_or_allocate_block(&mut self, pos: u64) -> std::io::Result<&mut BlockInfo> {
        let block_info = BlockFileHeader::get_mut_block_info_from(
            &mut self.block_info_list,
            self.block_size as u64,
            pos,
        )?;

        if !block_info.used {
            block_info.used = true;
            block_info.block_index = self.next_block_index;
            block_info.usage = 0;
            self.next_block_index += 1;
        }
        Ok(block_info)
    }

    fn get_mut_block_info_from(
        block_info_list: &mut [BlockInfo],
        block_size: u64,
        pos: u64,
    ) -> std::io::Result<&mut BlockInfo> {
        let block_info_index = (pos / block_size) as usize;
        let block_info = block_info_list.get_mut(block_info_index).map_or(
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid position",
            )),
            |block_info| Ok(block_info),
        )?;
        Ok(block_info)
    }

    fn find_last_block_index(block_info_list: &[BlockInfo]) -> Option<u32> {
        block_info_list
            .iter()
            .filter(|block_info| block_info.used)
            .max_by_key(|block_info| block_info.block_index)
            .map_or(None, |block_info| Some(block_info.block_index))
    }

    fn signatire_pose() -> u64 {
        0
    }

    fn file_size_pos() -> u64 {
        4
    }

    fn block_size_pose() -> u64 {
        12
    }

    fn block_info_list_len_start_pos() -> u64 {
        16
    }

    fn first_block_info_start_pos() -> u64 {
        20
    }
}

pub struct BlockFile {
    header: BlockFileHeader,
    file: File,
}

impl BlockFile {
    pub async fn create(path: &str, file_size: u64, block_size: u32) -> std::io::Result<BlockFile> {
        let mut file = File::create(path).await?;
        let header = BlockFileHeader::new(file_size, block_size);
        header.write_file_header(&mut file).await?;
        Ok(BlockFile { header, file })
    }

    pub async fn open(path: &str, write: bool) -> std::io::Result<BlockFile> {
        let mut file = File::options().write(write).read(true).open(path).await?;
        let header = BlockFileHeader::from(&mut file).await?;
        Ok(BlockFile { header, file })
    }

    pub async fn is_data_ready(&mut self, begin: u64, size: u64) -> std::io::Result<bool> {
        let size = if self.header.file_size < begin + size {
            self.header.file_size - begin
        } else {
            size
        };

        let (begin1, end) = self.find_block_info_range(begin, size);

        let block_info_list = &mut self.header.block_info_list[begin1 as usize..(end + 1) as usize];
        for block_info in block_info_list.iter_mut() {
            block_info.reload(&mut self.file).await?;
            if !block_info.used {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub async fn read(&mut self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        if self.header.file_size < offset {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid position",
            ));
        }

        let header_size = self.header.get_header_size();

        let end = buf.len().min((self.header.file_size - offset) as usize);
        let mut total_read_size: usize = 0;
        while total_read_size < end {
            let offset = offset + total_read_size as u64;
            let block_size = self.header.block_size as u64;
            let block_cursor = offset % block_size;
            let block_info = self.header.get_mut_block_info(offset)?;

            block_info.reload(&mut self.file).await?;
            BlockFile::try_move_cursor(
                &mut self.file,
                header_size,
                block_size,
                block_info,
                block_cursor,
            )
            .await?;

            let end_index = buf
                .len()
                .min(total_read_size + self.header.block_size as usize - block_cursor as usize);
            total_read_size += self.file.read(&mut buf[total_read_size..end_index]).await?;
        }
        Ok(total_read_size)
    }

    pub async fn write(&mut self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        let header_size = self.header.get_header_size();

        let mut total_wrote_size = 0;
        while total_wrote_size < buf.len() {
            let offset = offset + total_wrote_size as u64;
            let block_size = self.header.block_size as u64;
            let block_cursor = offset % block_size;
            let block_info = self.header.get_mut_or_allocate_block(offset)?;

            BlockFile::try_move_cursor(
                &mut self.file,
                header_size,
                block_size,
                block_info,
                block_cursor,
            )
            .await?;

            let end_index = buf
                .len()
                .min((total_wrote_size + block_size as usize - block_cursor as usize) as usize);
            let wrote_size = self
                .file
                .write(&buf[total_wrote_size..end_index as usize])
                .await?;

            block_info.usage += wrote_size as u32;
            total_wrote_size += wrote_size;
            block_info.write(&mut self.file).await?;
        }
        Ok(total_wrote_size)
    }

    pub fn calc_block_range_from(&self, offset: u64, size: u64) -> (u64, u64) {
        let (begin, end) = self.find_block_info_range(offset, size);
        (
            begin * self.header.block_size as u64,
            end * self.header.block_size as u64 + self.header.block_size as u64,
        )
    }

    fn find_block_info_range(&self, offset: u64, size: u64) -> (u64, u64) {
        let end = if size == 0 { offset } else { offset + size - 1 };

        let begin_block_info_index = offset / self.header.block_size as u64;
        let end_block_info_index = end / self.header.block_size as u64;

        (begin_block_info_index, end_block_info_index)
    }

    async fn try_move_cursor(
        file: &mut File,
        header_size: u64,
        block_size: u64,
        block_info: &BlockInfo,
        block_cursor: u64,
    ) -> std::io::Result<()> {
        if !block_info.used {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "access not exists block {} {} {} {}",
                    block_info.used, block_info.block_index, block_info.usage, block_cursor
                ),
            ));
        }

        let block_pos = block_info.block_index as u64 * block_size;
        let block_cursor_pos = block_cursor % block_size;
        let pos = header_size + block_pos + block_cursor_pos;
        file.seek(SeekFrom::Start(pos)).await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::blockfile::BlockFile;
    use rand::prelude::*;
    use rand::seq::SliceRandom;

    #[tokio::test]
    async fn block_file_test() {
        let charlist = ('a' as u8..('z' as u8) + 1).collect::<Vec<u8>>();
        let mut rng = rand::thread_rng();
        let file_size = 600;
        for _ in 0..1 {
            for s in 3..33 {
                let mut file = BlockFile::create("./test", file_size, 16).await.unwrap();

                let mut text: Vec<u8> = Vec::new();
                for _ in 0..file_size {
                    let a = rng.gen_range(0..charlist.len());
                    text.push(charlist[a]);
                    if text.len() == file_size as usize {
                        break;
                    }
                }

                let mut a: Vec<usize> = (0..text.len()).collect();
                a.shuffle(&mut rng);
                let mut total_write_size = 0;
                for i in a {
                    if i as usize * s >= text.len() {
                        continue;
                    }

                    let end = (i * s + s).min(text.len());
                    let buf = &text[i as usize * s..end];
                    total_write_size += file.write(&buf, i as u64 * s as u64).await.unwrap();
                    if total_write_size == text.len() {
                        break;
                    }
                }
                drop(file);

                let mut file = BlockFile::open("./test", false).await.unwrap();
                let mut a: Vec<usize> = (0..text.len()).collect();
                a.shuffle(&mut rng);
                for i in a {
                    if i + 1 >= text.len() {
                        continue;
                    }
                    let end = rng.gen_range(i + 1..text.len());

                    let mut buf: Vec<u8> = (i..end).map(|_| 0).collect();

                    file.read(&mut buf, i as u64).await.unwrap();
                    let buf2: Vec<u8> = buf.into_iter().filter(|x| *x != 0).collect();
                    assert_eq!(
                        String::from_utf8_lossy(&text[i..end]),
                        String::from_utf8_lossy(&buf2)
                    );
                }
                drop(file);

                let mut file = BlockFile::open("./test", false).await.unwrap();
                let mut a: Vec<u8> = (0..file_size).map(|_| 0).collect();
                file.read(&mut a, 0).await.unwrap();
                assert_eq!(String::from_utf8_lossy(&a), String::from_utf8_lossy(&text));
            }
        }
    }
}
