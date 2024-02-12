use clap::Parser;
use fuser::MountOption;

mod blockfile;
mod fs;
mod webdav;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    url: String,
    #[arg(long, default_value_t=String::new())]
    user: String,
    #[arg(short, long, default_value_t=String::new())]
    password: String,

    #[arg(short, long)]
    tmp_path: String,
    #[arg(short, long)]
    mount_path: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let client = webdav::WebDAVClient::new(args.url, args.user, args.password).unwrap();

    let user_id = unsafe { libc::getuid() };
    let group_id = unsafe { libc::getgid() };

    let webdavfs = fs::WebDAVFS::new(
        tokio::runtime::Handle::current(),
        client,
        args.tmp_path,
        user_id,
        group_id,
    );
    let options = vec![
        MountOption::RO,
        MountOption::Async,
        MountOption::FSName("fusedav-rs".to_string()),
    ];
    fuser::mount2(webdavfs, args.mount_path, &options).unwrap();
}
