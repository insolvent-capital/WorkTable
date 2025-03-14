use std::io::{BufReader, Read};
use std::path::Path;

mod persistence;
mod worktable;

pub fn check_if_files_are_same(got: String, expected: String) -> bool {
    let got = std::fs::File::open(got).unwrap();
    let expected = std::fs::File::open(expected).unwrap();

    // Check if file sizes are different
    if got.metadata().unwrap().len() != expected.metadata().unwrap().len() {
        return false;
    }

    // Use buf readers since they are much faster
    let f1 = BufReader::new(got);
    let f2 = BufReader::new(expected);

    // Do a byte to byte comparison of the two files
    for (b1, b2) in f1.bytes().zip(f2.bytes()) {
        if b1.unwrap() != b2.unwrap() {
            return false;
        }
    }

    true
}

pub fn check_if_dirs_are_same(got: String, expected: String) -> bool {
    let paths = std::fs::read_dir(&expected).unwrap();
    for file in paths {
        let file_name = file.unwrap().file_name();
        if !check_if_files_are_same(
            format!("{}/{}", got, file_name.to_str().unwrap()),
            format!("{}/{}", expected, file_name.to_str().unwrap()),
        ) {
            return false;
        }
    }

    true
}

pub async fn remove_file_if_exists(path: String) {
    if Path::new(path.as_str()).exists() {
        tokio::fs::remove_file(path.as_str()).await.unwrap();
    }
}

pub async fn remove_dir_if_exists(path: String) {
    if Path::new(path.as_str()).exists() {
        tokio::fs::remove_dir_all(path).await.unwrap()
    }
}
