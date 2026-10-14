use std::{
    collections::{HashMap, hash_map::Entry},
    fs,
    fs::File,
    io,
    io::Write,
    path::PathBuf,
};

use chrono::{DateTime, Utc};
use flate2::{Compression, write::GzEncoder};
use tracing::{error, info};

/// One gzip segment per symbol and rotation period.
///
/// A segment is named `<symbol>_<YYYYmmdd>_<HHMMSS>.gz` after the receipt time of its first line
/// and is written as `<name>.gz.open` until it is finished, so a file without the `.open` suffix
/// is always complete. Segments are created with `create_new`: a restart starts a new segment
/// instead of writing over an existing file.
///
/// Rotation only moves forward. A line whose receipt time falls in an earlier period, such as a
/// REST snapshot that raced a websocket frame across the boundary, stays in the current segment;
/// the converter sorts events by timestamp.
pub struct RotatingFile {
    prefix: String,
    rotate_ns: i64,
    period: i64,
    open_path: PathBuf,
    final_path: PathBuf,
    file: Option<GzEncoder<File>>,
    dirty: bool,
}

impl RotatingFile {
    fn create(
        datetime: DateTime<Utc>,
        prefix: &str,
    ) -> Result<(PathBuf, PathBuf, GzEncoder<File>), io::Error> {
        let stem = format!("{prefix}_{}", datetime.format("%Y%m%d_%H%M%S"));
        for n in 0.. {
            let name = if n == 0 {
                stem.clone()
            } else {
                format!("{stem}_{n}")
            };
            let final_path = PathBuf::from(format!("{name}.gz"));
            let open_path = PathBuf::from(format!("{name}.gz.open"));
            if final_path.exists() {
                continue;
            }
            match File::options()
                .write(true)
                .create_new(true)
                .open(&open_path)
            {
                Ok(file) => {
                    info!(path = %open_path.display(), "segment opened");
                    return Ok((
                        open_path,
                        final_path,
                        GzEncoder::new(file, Compression::default()),
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        unreachable!()
    }

    pub fn new(
        datetime: DateTime<Utc>,
        prefix: String,
        rotate_secs: u64,
    ) -> Result<Self, io::Error> {
        let rotate_ns = rotate_secs as i64 * 1_000_000_000;
        let (open_path, final_path, file) = Self::create(datetime, &prefix)?;
        Ok(Self {
            period: Self::period_of(datetime, rotate_ns),
            prefix,
            rotate_ns,
            open_path,
            final_path,
            file: Some(file),
            dirty: false,
        })
    }

    fn period_of(datetime: DateTime<Utc>, rotate_ns: i64) -> i64 {
        datetime
            .timestamp_nanos_opt()
            .unwrap()
            .div_euclid(rotate_ns)
    }

    pub fn write(&mut self, datetime: DateTime<Utc>, data: String) -> Result<(), io::Error> {
        let period = Self::period_of(datetime, self.rotate_ns);
        if period > self.period || self.file.is_none() {
            self.finish()?;
            let (open_path, final_path, file) = Self::create(datetime, &self.prefix)?;
            self.open_path = open_path;
            self.final_path = final_path;
            self.file = Some(file);
            self.period = period;
        }
        let timestamp = datetime.timestamp_nanos_opt().unwrap();
        self.dirty = true;
        self.file
            .as_mut()
            .unwrap()
            .write_all(format!("{timestamp} {data}\n").as_bytes())
    }

    /// Flushes the compressor with a sync flush, so a killed process loses at most the lines
    /// written since the last flush. Segments without new lines are left alone.
    pub fn flush(&mut self) -> Result<(), io::Error> {
        if let Some(file) = self.file.as_mut()
            && self.dirty
        {
            file.flush()?;
            self.dirty = false;
        }
        Ok(())
    }

    /// Writes the gzip trailer, syncs the file and removes the `.open` suffix.
    pub fn finish(&mut self) -> Result<(), io::Error> {
        if let Some(file) = self.file.take() {
            let file = file.finish()?;
            file.sync_all()?;
            fs::rename(&self.open_path, &self.final_path)?;
            info!(path = %self.final_path.display(), "segment finished");
        }
        Ok(())
    }
}

impl Drop for RotatingFile {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            error!(?error, path = %self.open_path.display(), "couldn't finish the segment");
        }
    }
}

pub struct Writer {
    path: String,
    rotate_secs: u64,
    file: HashMap<String, RotatingFile>,
}

impl Writer {
    pub fn new(path: &str, rotate_secs: u64) -> Self {
        Self {
            path: path.to_string(),
            rotate_secs,
            file: Default::default(),
        }
    }

    pub fn write(
        &mut self,
        recv_time: DateTime<Utc>,
        symbol: String,
        data: String,
    ) -> Result<(), anyhow::Error> {
        match self.file.entry(symbol.to_lowercase()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().write(recv_time, data)?;
            }
            Entry::Vacant(entry) => {
                let symbol = entry.key().clone();
                let path = self.path.as_str();
                entry
                    .insert(RotatingFile::new(
                        recv_time,
                        format!("{path}/{symbol}"),
                        self.rotate_secs,
                    )?)
                    .write(recv_time, data)?;
            }
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), io::Error> {
        for file in self.file.values_mut() {
            file.flush()?;
        }
        Ok(())
    }

    /// Finishes every open segment. Tries all of them and returns the first error.
    pub fn close(&mut self) -> Result<(), io::Error> {
        let mut result = Ok(());
        for (_, mut file) in self.file.drain() {
            if let Err(error) = file.finish() {
                error!(?error, "couldn't finish a segment");
                if result.is_ok() {
                    result = Err(error);
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use chrono::TimeZone;
    use flate2::read::MultiGzDecoder;

    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "collector-test-{name}-{}-{}",
                std::process::id(),
                Utc::now().timestamp_nanos_opt().unwrap()
            ));
            fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn path(&self) -> &str {
            self.0.to_str().unwrap()
        }

        fn files(&self) -> Vec<String> {
            let mut names: Vec<String> = fs::read_dir(&self.0)
                .unwrap()
                .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                .collect();
            names.sort();
            names
        }

        fn read(&self, name: &str) -> (String, io::Result<usize>) {
            let mut out = Vec::new();
            let result =
                MultiGzDecoder::new(File::open(self.0.join(name)).unwrap()).read_to_end(&mut out);
            (String::from_utf8(out).unwrap(), result)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn at(h: u32, m: u32, s: u32, ms: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 15, h, m, s).unwrap() + chrono::Duration::milliseconds(ms)
    }

    fn ns(datetime: DateTime<Utc>) -> i64 {
        datetime.timestamp_nanos_opt().unwrap()
    }

    #[test]
    fn rotates_forward_only_and_finishes_segments() {
        let dir = TempDir::new("rotate");
        let mut writer = Writer::new(dir.path(), 3600);
        writer
            .write(at(12, 59, 59, 900), "XAUUSDT".into(), "a".into())
            .unwrap();
        writer
            .write(at(13, 0, 0, 5), "XAUUSDT".into(), "b".into())
            .unwrap();
        // A late line from the previous hour stays in the current segment.
        writer
            .write(at(12, 59, 59, 990), "XAUUSDT".into(), "late".into())
            .unwrap();
        assert_eq!(
            dir.files(),
            [
                "xauusdt_20261015_125959.gz",
                "xauusdt_20261015_130000.gz.open"
            ]
        );
        writer.close().unwrap();
        assert_eq!(
            dir.files(),
            ["xauusdt_20261015_125959.gz", "xauusdt_20261015_130000.gz"]
        );
        let (first, result) = dir.read("xauusdt_20261015_125959.gz");
        result.unwrap();
        assert_eq!(first, format!("{} a\n", ns(at(12, 59, 59, 900))));
        let (second, result) = dir.read("xauusdt_20261015_130000.gz");
        result.unwrap();
        assert_eq!(
            second,
            format!(
                "{} b\n{} late\n",
                ns(at(13, 0, 0, 5)),
                ns(at(12, 59, 59, 990))
            )
        );
    }

    #[test]
    fn restart_never_overwrites() {
        let dir = TempDir::new("restart");
        for line in ["first", "second", "third"] {
            let mut writer = Writer::new(dir.path(), 3600);
            writer
                .write(at(13, 5, 7, 0), "XAUUSDT".into(), line.into())
                .unwrap();
            writer.close().unwrap();
        }
        assert_eq!(
            dir.files(),
            [
                "xauusdt_20261015_130507.gz",
                "xauusdt_20261015_130507_1.gz",
                "xauusdt_20261015_130507_2.gz"
            ]
        );
        for (name, line) in [
            ("xauusdt_20261015_130507.gz", "first"),
            ("xauusdt_20261015_130507_1.gz", "second"),
            ("xauusdt_20261015_130507_2.gz", "third"),
        ] {
            let (text, result) = dir.read(name);
            result.unwrap();
            assert_eq!(text, format!("{} {line}\n", ns(at(13, 5, 7, 0))));
        }
    }

    #[test]
    fn flushed_lines_survive_a_kill() {
        let dir = TempDir::new("kill");
        let mut writer = Writer::new(dir.path(), 3600);
        writer
            .write(at(13, 0, 0, 0), "XAUUSDT".into(), "kept".into())
            .unwrap();
        writer.flush().unwrap();
        writer
            .write(at(13, 0, 1, 0), "XAUUSDT".into(), "unflushed".into())
            .unwrap();
        // Simulates a kill: the process dies without finishing the segment.
        std::mem::forget(writer);
        let name = "xauusdt_20261015_130000.gz.open";
        assert_eq!(dir.files(), [name]);
        let (text, result) = dir.read(name);
        assert!(result.is_err(), "a killed segment has no gzip trailer");
        assert_eq!(text, format!("{} kept\n", ns(at(13, 0, 0, 0))));
    }
}
