#[cfg(test)]
pub mod test {
    use std::{
        fs::{self, File},
        io::{Error, Write},
        path::PathBuf,
    };

    use rand::Rng;

    type result<T> = Result<T, Error>;

    pub fn save_data_1(path: PathBuf, data: &[u8]) -> result<()> {
        let mut fp = File::create(path)?;
        fp.write_all(data)?;

        Ok(())
    }

    pub fn save_data_2(path: PathBuf, data: &[u8]) -> result<()> {
        let mut rng = rand::thread_rng();
        let random_int = rng.gen_range(0..i32::MAX);

        let tmp = format!("{}.tmp.{random_int}", path.to_string_lossy().to_string());

        let mut fp = File::create(&path)?;
        match fp.write_all(data) {
            Ok(_) => {
                fs::rename(tmp, &path)?;
            }
            Err(_) => {
                fs::remove_file(tmp)?;
            }
        };

        Ok(())
    }

    pub fn save_data_3(path: PathBuf, data: &[u8]) -> result<()> {
        let mut rng = rand::thread_rng();
        let random_int = rng.gen_range(0..i32::MAX);

        let tmp = format!("{}.tmp.{random_int}", path.to_string_lossy().to_string());

        let mut fp = File::create(&path)?;
        match fp.write_all(data) {
            Ok(_) => match fp.sync_all() {
                Ok(_) => {
                    fs::rename(tmp, &path)?;
                }
                Err(_) => {
                    fs::remove_file(tmp)?;
                }
            },
            Err(_) => {
                fs::remove_file(tmp)?;
            }
        };

        Ok(())
    }
}
