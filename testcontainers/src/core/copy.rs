use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tar::EntryType;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone)]
pub struct CopyToContainerCollection(Vec<CopyToContainer>);

#[derive(Debug, Clone)]
pub struct CopyToContainer {
    target: CopyTargetOptions,
    source: CopyDataSource,
}

#[derive(Debug, Clone)]
pub struct CopyTargetOptions {
    target: String,
    mode: Option<u32>,
}

#[derive(Debug, Clone)]
pub enum CopyDataSource {
    File(PathBuf),
    Data(Vec<u8>),
}

/// Errors that can occur while materializing data copied from a container.
#[derive(Debug, thiserror::Error)]
pub enum CopyFromContainerError {
    #[error("io failed with error: {0}")]
    Io(#[from] std::io::Error),
    #[error("archive did not contain any regular files")]
    EmptyArchive,
    #[error("requested container path is a directory")]
    IsDirectory,
    #[error("archive entry type '{0:?}' is not supported for requested target")]
    UnsupportedEntry(EntryType),
}

/// Abstraction for materializing the bytes read from a source into a concrete destination.
///
/// Implementors typically persist the incoming bytes to disk or buffer them in memory. Some return
/// a value that callers can work with (for example, the collected bytes), while others simply
/// report success with `()`. Implementations must consume the provided reader until EOF or return
/// an error. Destinations are allowed to discard any existing data to make room for the incoming
/// bytes.
#[async_trait(?Send)]
pub trait CopyFileFromContainer {
    type Output;

    /// Writes all bytes from the reader into `self`, returning a value that represents the completed operation (or `()` for sinks that only confirm success).
    ///
    /// Implementations may mutate `self` and must propagate I/O errors via [`CopyFromContainerError`].
    async fn copy_from_reader<R>(self, reader: R) -> Result<Self::Output, CopyFromContainerError>
    where
        R: AsyncRead + Unpin;
}

#[async_trait(?Send)]
impl CopyFileFromContainer for Vec<u8> {
    type Output = Vec<u8>;

    async fn copy_from_reader<R>(
        mut self,
        reader: R,
    ) -> Result<Self::Output, CopyFromContainerError>
    where
        R: AsyncRead + Unpin,
    {
        let mut_ref = &mut self;
        mut_ref.copy_from_reader(reader).await?;
        Ok(self)
    }
}

#[async_trait(?Send)]
impl CopyFileFromContainer for &mut Vec<u8> {
    type Output = ();

    async fn copy_from_reader<R>(
        mut self,
        mut reader: R,
    ) -> Result<Self::Output, CopyFromContainerError>
    where
        R: AsyncRead + Unpin,
    {
        self.clear();
        reader
            .read_to_end(&mut self)
            .await
            .map_err(CopyFromContainerError::Io)?;
        Ok(())
    }
}

#[async_trait(?Send)]
impl CopyFileFromContainer for PathBuf {
    type Output = ();

    async fn copy_from_reader<R>(self, reader: R) -> Result<Self::Output, CopyFromContainerError>
    where
        R: AsyncRead + Unpin,
    {
        self.as_path().copy_from_reader(reader).await
    }
}

#[async_trait(?Send)]
impl CopyFileFromContainer for &Path {
    type Output = ();

    async fn copy_from_reader<R>(
        self,
        mut reader: R,
    ) -> Result<Self::Output, CopyFromContainerError>
    where
        R: AsyncRead + Unpin,
    {
        if let Some(parent) = self.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(CopyFromContainerError::Io)?;
            }
        }

        let mut file = tokio::fs::File::create(self)
            .await
            .map_err(CopyFromContainerError::Io)?;

        tokio::io::copy(&mut reader, &mut file)
            .await
            .map_err(CopyFromContainerError::Io)?;

        file.flush().await.map_err(CopyFromContainerError::Io)?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CopyToContainerError {
    #[error("io failed with error: {0}")]
    IoError(std::io::Error),
    #[error("failed to get the path name: {0}")]
    PathNameError(String),
}

impl CopyToContainerCollection {
    pub fn new(collection: Vec<CopyToContainer>) -> Self {
        Self(collection)
    }

    pub fn add(&mut self, entry: CopyToContainer) {
        self.0.push(entry);
    }

    pub(crate) async fn tar(&self) -> Result<bytes::Bytes, CopyToContainerError> {
        encode(self.0.clone()).await
    }
}

impl CopyToContainer {
    pub fn new(source: impl Into<CopyDataSource>, target: impl Into<CopyTargetOptions>) -> Self {
        Self {
            source: source.into(),
            target: target.into(),
        }
    }

    pub(crate) async fn tar(&self) -> Result<bytes::Bytes, CopyToContainerError> {
        encode(vec![self.clone()]).await
    }

    fn append_tar<W: std::io::Write>(
        &self,
        ar: &mut tar::Builder<W>,
    ) -> Result<(), CopyToContainerError> {
        self.source.append_tar(ar, &self.target)
    }
}

impl CopyTargetOptions {
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
            mode: None,
        }
    }

    pub fn with_mode(mut self, mode: u32) -> Self {
        self.mode = Some(mode);
        self
    }

    pub fn target(&self) -> &str {
        &self.target
    }

    pub fn mode(&self) -> Option<u32> {
        self.mode
    }
}

impl<T> From<T> for CopyTargetOptions
where
    T: Into<String>,
{
    fn from(value: T) -> Self {
        CopyTargetOptions::new(value.into())
    }
}

impl From<&Path> for CopyDataSource {
    fn from(value: &Path) -> Self {
        CopyDataSource::File(value.to_path_buf())
    }
}

impl From<PathBuf> for CopyDataSource {
    fn from(value: PathBuf) -> Self {
        CopyDataSource::File(value)
    }
}
impl From<Vec<u8>> for CopyDataSource {
    fn from(value: Vec<u8>) -> Self {
        CopyDataSource::Data(value)
    }
}

impl CopyDataSource {
    fn append_tar<W: std::io::Write>(
        &self,
        ar: &mut tar::Builder<W>,
        target: &CopyTargetOptions,
    ) -> Result<(), CopyToContainerError> {
        let target_path = target.target();

        match self {
            CopyDataSource::File(source_file_path) => {
                if let Err(e) = append_tar_file(ar, source_file_path, target) {
                    log::error!(
                        "Could not append file/dir to tar: {source_file_path:?}:{target_path}"
                    );
                    return Err(e);
                }
            }
            CopyDataSource::Data(data) => {
                if let Err(e) = append_tar_bytes(ar, data, target) {
                    log::error!("Could not append data to tar: {target_path}");
                    return Err(e);
                }
            }
        };

        Ok(())
    }
}

fn append_tar_file<W: std::io::Write>(
    ar: &mut tar::Builder<W>,
    source_file_path: &Path,
    target: &CopyTargetOptions,
) -> Result<(), CopyToContainerError> {
    let target_path = make_path_relative(target.target());
    let meta = std::fs::metadata(source_file_path).map_err(CopyToContainerError::IoError)?;

    if meta.is_dir() {
        ar.append_dir_all(target_path, source_file_path)
            .map_err(CopyToContainerError::IoError)?;
    } else {
        let f =
            &mut std::fs::File::open(source_file_path).map_err(CopyToContainerError::IoError)?;

        let mut header = tar::Header::new_gnu();
        header.set_size(meta.len());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = target.mode().unwrap_or_else(|| meta.permissions().mode());
            header.set_mode(mode);
        }

        #[cfg(not(unix))]
        {
            let mode = target.mode().unwrap_or(0o644);
            header.set_mode(mode);
        }

        header.set_cksum();

        ar.append_data(&mut header, target_path, f)
            .map_err(CopyToContainerError::IoError)?;
    };

    Ok(())
}

fn append_tar_bytes<W: std::io::Write>(
    ar: &mut tar::Builder<W>,
    data: &Vec<u8>,
    target: &CopyTargetOptions,
) -> Result<(), CopyToContainerError> {
    let relative_target_path = make_path_relative(target.target());

    let mut header = tar::Header::new_gnu();
    header.set_size(data.len() as u64);
    header.set_mode(target.mode().unwrap_or(0o0644));
    header.set_cksum();

    ar.append_data(&mut header, relative_target_path, data.as_slice())
        .map_err(CopyToContainerError::IoError)?;

    Ok(())
}

fn make_path_relative(path: &str) -> String {
    // TODO support also absolute windows paths like "C:\temp\foo.txt"
    if path.starts_with("/") {
        path.trim_start_matches("/").to_string()
    } else {
        path.to_string()
    }
}

// Copy-to already returns one fully materialized archive. Keep its synchronous
// filesystem/codec work on a blocking worker without an unnecessary async bridge.
// Dropping the caller closes the cancellation receiver, stopping further writes.
async fn encode(entries: Vec<CopyToContainer>) -> Result<bytes::Bytes, CopyToContainerError> {
    let (cancel, receiver) = tokio::sync::oneshot::channel();
    let worker = tokio::task::spawn_blocking(move || {
        let mut archive = tar::Builder::new(ArchiveOutput {
            bytes: Vec::new(),
            cancel,
        });
        for entry in entries {
            entry.append_tar(&mut archive)?;
        }
        let output = archive
            .into_inner()
            .map_err(CopyToContainerError::IoError)?;
        Ok(bytes::Bytes::from(output.bytes))
    });
    let result = worker
        .await
        .map_err(|error| CopyToContainerError::IoError(std::io::Error::other(error)))?;
    drop(receiver);
    result
}
struct ArchiveOutput {
    bytes: Vec<u8>,
    cancel: tokio::sync::oneshot::Sender<()>,
}
impl std::io::Write for ArchiveOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.cancel.is_closed() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "archive consumer cancelled",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) async fn extract_file_entry<R, T>(
    mut reader: R,
    target: T,
) -> Result<T::Output, CopyFromContainerError>
where
    R: AsyncRead + Unpin,
    T: CopyFileFromContainer,
{
    let (archive_input, mut archive_writer) = tokio::io::duplex(64 * 1024);
    let (payload_reader, payload_output) = tokio::io::duplex(64 * 1024);
    let archive_input = tokio_util::io::SyncIoBridge::new(archive_input);
    let mut payload_output = tokio_util::io::SyncIoBridge::new(payload_output);
    let (metadata, received) = tokio::sync::oneshot::channel();
    let source_error = std::sync::Arc::new(std::sync::Mutex::new(None));
    let supplied_error = source_error.clone();
    let worker = tokio::task::spawn_blocking(move || {
        let mut metadata = Some(metadata);
        let result = (|| {
            let mut archive = tar::Archive::new(archive_input);
            for entry in archive.entries().map_err(CopyFromContainerError::Io)? {
                let mut entry = entry.map_err(CopyFromContainerError::Io)?;
                match entry.header().entry_type() {
                    EntryType::GNULongName
                    | EntryType::GNULongLink
                    | EntryType::XGlobalHeader
                    | EntryType::XHeader
                    | EntryType::GNUSparse => continue,
                    EntryType::Directory => return Err(CopyFromContainerError::IsDirectory),
                    EntryType::Regular | EntryType::Continuous => {
                        if metadata
                            .take()
                            .expect("one selected file")
                            .send(Ok(()))
                            .is_err()
                        {
                            return Ok(());
                        }
                        let expected = entry.size();
                        let copied = std::io::copy(&mut entry, &mut payload_output)
                            .map_err(CopyFromContainerError::Io)?;
                        if copied != expected {
                            return Err(CopyFromContainerError::Io(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "archive entry is truncated",
                            )));
                        }
                        return Ok(());
                    }
                    kind => return Err(CopyFromContainerError::UnsupportedEntry(kind)),
                }
            }
            Err(CopyFromContainerError::EmptyArchive)
        })();
        if let Some(sender) = metadata {
            let _ = sender.send(result);
            return Ok(());
        }
        result
    });
    // Keep the source and target on this async task: custom targets/readers need
    // not be Send or 'static. Only bounded pipe endpoints cross to the worker.
    let supply = async move {
        let mut buffer = [0_u8; 8192];
        loop {
            let length = match reader.read(&mut buffer).await {
                Ok(0) => break,
                Ok(length) => length,
                Err(error) => {
                    *supplied_error.lock().expect("archive source result") = Some(error);
                    break;
                }
            };
            // The parser may close its input after selecting or rejecting the
            // first entry. Only upstream reads are source failures, not that
            // intentional downstream close of the bounded archive bridge.
            if archive_writer.write_all(&buffer[..length]).await.is_err() {
                break;
            }
        }
        drop(archive_writer);
        // EOF or source failure closes input. A truncated selected entry must
        // still fail in the parser, never become successful partial file bytes.
    };
    let extract = async move {
        let metadata = received.await.map_err(|_| {
            CopyFromContainerError::Io(std::io::Error::other("archive worker stopped"))
        })?;
        if let Err(error) = metadata {
            worker
                .await
                .map_err(|error| CopyFromContainerError::Io(std::io::Error::other(error)))??;
            return Err(source_error
                .lock()
                .expect("archive source result")
                .take()
                .map(CopyFromContainerError::Io)
                .unwrap_or(error));
        }
        let result = target.copy_from_reader(payload_reader).await;
        let parsed = worker
            .await
            .map_err(|error| CopyFromContainerError::Io(std::io::Error::other(error)))?;
        let value = result?;
        if let Some(error) = source_error.lock().expect("archive source result").take() {
            return Err(CopyFromContainerError::Io(error));
        }
        parsed?;
        Ok(value)
    };
    tokio::pin!(supply, extract);
    tokio::select! { result = &mut extract => result, _ = &mut supply => extract.await }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Write};

    use tar::Archive;
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn copytocontainer_tar_file_success() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("file.txt");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "TEST").unwrap();

        let copy_to_container = CopyToContainer::new(file_path, "file.txt");
        let result = copy_to_container.tar().await;

        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty());
    }

    #[tokio::test]
    async fn copytocontainer_tar_data_success() {
        let data = vec![1, 2, 3, 4, 5];
        let copy_to_container = CopyToContainer::new(data, "data.bin");
        let result = copy_to_container.tar().await;

        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty());
    }

    #[tokio::test]
    async fn copytocontainer_tar_file_not_found() {
        let temp_dir = tempdir().unwrap();
        let non_existent_file_path = temp_dir.path().join("non_existent_file.txt");

        let copy_to_container = CopyToContainer::new(non_existent_file_path, "file.txt");
        let result = copy_to_container.tar().await;

        assert!(result.is_err());
        if let Err(CopyToContainerError::IoError(err)) = result {
            assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
        } else {
            panic!("Expected IoError");
        }
    }

    #[tokio::test]
    async fn copytocontainercollection_tar_file_and_data() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("file.txt");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "TEST").unwrap();

        let copy_to_container_collection = CopyToContainerCollection::new(vec![
            CopyToContainer::new(file_path, "file.txt"),
            CopyToContainer::new(vec![1, 2, 3, 4, 5], "data.bin"),
        ]);

        let result = copy_to_container_collection.tar().await;

        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty());
    }

    #[tokio::test]
    async fn tar_bytes_respects_custom_mode() {
        let data = vec![1, 2, 3];
        let target = CopyTargetOptions::new("data.bin").with_mode(0o600);
        let copy_to_container = CopyToContainer::new(data, target);

        let tar_bytes = copy_to_container.tar().await.unwrap();
        let mut archive = Archive::new(std::io::Cursor::new(tar_bytes));
        let mut entries = archive.entries().unwrap();
        let entry = entries.next().unwrap().unwrap();

        assert_eq!(entry.header().mode().unwrap(), 0o600);
    }

    #[tokio::test]
    async fn tar_file_respects_custom_mode() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("file.txt");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "TEST").unwrap();

        let target = CopyTargetOptions::new("file.txt").with_mode(0o640);
        let copy_to_container = CopyToContainer::new(file_path, target);

        let tar_bytes = copy_to_container.tar().await.unwrap();
        let mut archive = Archive::new(std::io::Cursor::new(tar_bytes));
        let mut entries = archive.entries().unwrap();
        let entry = entries.next().unwrap().unwrap();

        assert_eq!(entry.header().mode().unwrap(), 0o640);
    }

    fn archive(entries: &[(&str, &[u8], EntryType)]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (path, bytes, kind) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(*kind);
            header.set_mode(0o600);
            header.set_size(bytes.len() as u64);
            if kind.is_symlink() {
                header.set_link_name("unfollowed-target").unwrap();
            }
            header.set_cksum();
            builder.append_data(&mut header, path, *bytes).unwrap();
        }
        builder.into_inner().unwrap()
    }

    #[test]
    fn cancelling_copy_to_stops_further_buffer_writes_without_retryable_interruption() {
        let (cancel, receiver) = tokio::sync::oneshot::channel();
        let mut output = ArchiveOutput {
            bytes: Vec::new(),
            cancel,
        };
        output.write_all(b"accepted").unwrap();
        drop(receiver);
        assert_eq!(
            output.write_all(b"not accepted").unwrap_err().kind(),
            std::io::ErrorKind::BrokenPipe
        );
        assert_eq!(output.bytes, b"accepted");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn maintained_build_context_round_trips_through_native_docker_without_registry_or_execution(
    ) {
        use bollard::{
            models::ContainerCreateBody,
            query_parameters::{CreateContainerOptions, DownloadFromContainerOptionsBuilder},
            Docker,
        };
        use futures::TryStreamExt;
        use std::{collections::HashMap, io::Read, process::Stdio, time::Duration};
        let bytes: Vec<u8> = (0..100_000).map(|n| (n % 251) as u8).collect();
        let context = crate::core::BuildContextBuilder::default()
            .with_dockerfile_string("FROM scratch\nCOPY payload.bin /fixture/payload.bin\n")
            .with_data(bytes.clone(), "payload.bin")
            .collect()
            .tar()
            .await
            .unwrap();
        let home = tempdir().unwrap();
        let input = home.path().join("context.tar");
        std::fs::write(&input, context).unwrap();
        let descriptor = format!(
            "uc-maintained-tar-{}-{}:qualification",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        // Exercise the exact maintained build-context archive through Docker's
        // native client. No remote base image, RUN command or source mount.
        let output = tokio::time::timeout(
            Duration::from_secs(60),
            tokio::process::Command::new("docker")
                .args(["build", "--network=none", "--tag", &descriptor, "-"])
                .stdin(Stdio::from(std::fs::File::open(input).unwrap()))
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("bounded native archive build")
        .unwrap();
        assert!(
            output.status.success(),
            "native archive build failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let docker = Docker::connect_with_local_defaults().unwrap();
        let container = docker
            .create_container(
                None::<CreateContainerOptions>,
                ContainerCreateBody {
                    image: Some(descriptor),
                    cmd: Some(vec!["never-executed".into()]),
                    labels: Some(HashMap::from([(
                        "ai.ultracareer.fixture".into(),
                        "testcontainers-maintained-tar-build".into(),
                    )])),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        // Never start this container. Remove only this invocation's returned ID.
        let observed = tokio::time::timeout(Duration::from_secs(15), async {
            let chunks: Vec<bytes::Bytes> = docker
                .download_from_container(
                    &container.id,
                    Some(
                        DownloadFromContainerOptionsBuilder::new()
                            .path("/fixture/payload.bin")
                            .build(),
                    ),
                )
                .try_collect()
                .await
                .unwrap();
            let encoded: Vec<u8> = chunks.into_iter().flatten().collect();
            let mut archive = tar::Archive::new(encoded.as_slice());
            let mut result = Vec::new();
            archive
                .entries()
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .read_to_end(&mut result)
                .unwrap();
            result
        })
        .await;
        docker.remove_container(&container.id, None).await.unwrap();
        assert_eq!(observed.expect("bounded built-image verification"), bytes);
        // The uniquely named qualification image is retained, never pruned or
        // promoted. This test grants no authority over shared image/cache data.
    }

    #[tokio::test(flavor = "current_thread")]
    async fn maintained_codec_round_trips_exact_bytes_long_unicode_paths_and_directories() {
        let home = tempdir().unwrap();
        let path = home.path().join("résumé.bin");
        let original: Vec<u8> = (0..300_000).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &original).unwrap();
        let target = format!("/root/{}/résumé.bin", "long-name".repeat(20));
        let encoded = CopyToContainer::new(path, CopyTargetOptions::new(&target).with_mode(0o640))
            .tar()
            .await
            .unwrap();
        let mut inspected = tar::Archive::new(encoded.as_ref());
        let mut entries = inspected.entries().unwrap();
        let entry = entries.next().unwrap().unwrap();
        assert_eq!(
            entry.path().unwrap().to_string_lossy(),
            target.trim_start_matches('/')
        );
        assert_eq!(entry.header().mode().unwrap(), 0o640);
        let decoded = extract_file_entry(encoded.as_ref(), Vec::new())
            .await
            .unwrap();
        assert_eq!(decoded, original);
        let encoded = CopyToContainer::new(home.path(), "fixture-dir")
            .tar()
            .await
            .unwrap();
        assert!(
            tar::Archive::new(encoded.as_ref())
                .entries()
                .unwrap()
                .count()
                >= 2
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn copy_out_rejects_empty_directory_links_bad_checksum_and_truncated_payloads() {
        assert!(matches!(
            extract_file_entry(archive(&[]).as_slice(), Vec::new()).await,
            Err(CopyFromContainerError::EmptyArchive)
        ));
        assert!(matches!(
            extract_file_entry(
                archive(&[("directory", b"", EntryType::Directory)]).as_slice(),
                Vec::new()
            )
            .await,
            Err(CopyFromContainerError::IsDirectory)
        ));
        let later = vec![7; 300_000];
        assert!(
            matches!(
                extract_file_entry(
                    archive(&[
                        ("directory", b"", EntryType::Directory),
                        ("directory/file", &later, EntryType::Regular)
                    ])
                    .as_slice(),
                    Vec::new()
                )
                .await,
                Err(CopyFromContainerError::IsDirectory)
            ),
            "early directory rejection retains its category after closing a large input stream"
        );
        assert!(matches!(
            extract_file_entry(
                archive(&[("link", b"", EntryType::Symlink)]).as_slice(),
                Vec::new()
            )
            .await,
            Err(CopyFromContainerError::UnsupportedEntry(EntryType::Symlink))
        ));
        let mut corrupt = archive(&[("file", b"bytes", EntryType::Regular)]);
        corrupt[0] ^= 1;
        assert!(matches!(
            extract_file_entry(corrupt.as_slice(), Vec::new()).await,
            Err(CopyFromContainerError::Io(_))
        ));
        let bytes = vec![7; 300_000];
        let mut truncated = archive(&[("file", &bytes, EntryType::Regular)]);
        truncated.truncate(512 + 1234);
        assert!(
            matches!(
                extract_file_entry(truncated.as_slice(), Vec::new()).await,
                Err(CopyFromContainerError::Io(_))
            ),
            "partial payload cannot become successful copy-out"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn copy_out_preserves_non_send_targets_and_does_not_consume_later_files() {
        struct Local(std::rc::Rc<std::cell::Cell<usize>>);
        #[async_trait(?Send)]
        impl CopyFileFromContainer for Local {
            type Output = Vec<u8>;
            async fn copy_from_reader<R>(
                self,
                mut reader: R,
            ) -> Result<Self::Output, CopyFromContainerError>
            where
                R: AsyncRead + Unpin,
            {
                let mut bytes = Vec::new();
                reader.read_to_end(&mut bytes).await?;
                self.0.set(bytes.len());
                Ok(bytes)
            }
        }
        let later = vec![8; 300_000];
        let encoded = archive(&[
            ("first", b"selected", EntryType::Regular),
            ("later", &later, EntryType::Regular),
        ]);
        let count = std::rc::Rc::new(std::cell::Cell::new(0));
        assert_eq!(
            extract_file_entry(encoded.as_slice(), Local(count.clone()))
                .await
                .unwrap(),
            b"selected"
        );
        assert_eq!(count.get(), 8);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_stalled_copy_closes_both_bounded_pipe_directions() {
        let (mut writer, reader) = tokio::io::duplex(2048);
        {
            let copying = extract_file_entry(reader, Vec::new());
            tokio::pin!(copying);
            std::future::poll_fn(|context| {
                use std::future::Future;
                assert!(copying.as_mut().poll(context).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
        }
        assert!(writer.write_all(b"source must be closed").await.is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn source_and_target_io_failures_are_not_success_or_parser_eof() {
        struct BrokenSource;
        impl AsyncRead for BrokenSource {
            fn poll_read(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
                _: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "generated source failure",
                )))
            }
        }
        assert!(
            matches!(extract_file_entry(BrokenSource,Vec::new()).await,Err(CopyFromContainerError::Io(error)) if error.kind()==std::io::ErrorKind::ConnectionReset)
        );
        struct BrokenTarget;
        #[async_trait(?Send)]
        impl CopyFileFromContainer for BrokenTarget {
            type Output = ();
            async fn copy_from_reader<R>(self, _: R) -> Result<(), CopyFromContainerError>
            where
                R: AsyncRead + Unpin,
            {
                Err(CopyFromContainerError::Io(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "generated target failure",
                )))
            }
        }
        let bytes = vec![7; 300_000];
        let encoded = archive(&[("file", &bytes, EntryType::Regular)]);
        assert!(
            matches!(extract_file_entry(encoded.as_slice(),BrokenTarget).await,Err(CopyFromContainerError::Io(error)) if error.kind()==std::io::ErrorKind::PermissionDenied)
        );
    }
}
