//! Isolated native copy/build consumers for the maintained archive adapter.
use testcontainers::{
    core::{
        client::ClientError, error::TestcontainersError, CopyFromContainerError, ExecCommand,
        WaitFor,
    },
    runners::AsyncRunner,
    CopyTargetOptions, GenericImage, ImageExt,
};

#[tokio::test(flavor = "current_thread")]
async fn maintained_archive_async_builder_completes_without_registry_or_execution() {
    use testcontainers::{runners::AsyncBuilder, GenericBuildableImage};
    let name = format!(
        "uc-maintained-tar-builder-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let descriptor = format!("{name}:qualification");
    let _image = tokio::time::timeout(
        std::time::Duration::from_secs(45),
        GenericBuildableImage::new(name, "qualification")
            .with_dockerfile_string("FROM scratch\nCOPY payload.bin /fixture/payload.bin\n")
            .with_data(vec![7; 100_000], "payload.bin")
            .build_image(),
    )
    .await
    .expect("bounded native AsyncBuilder")
    .unwrap();
    bollard::Docker::connect_with_local_defaults()
        .unwrap()
        .inspect_image(&descriptor)
        .await
        .expect("completed actual image build");
    // Retain this uniquely named image, with no runtime container or prune.
}

#[tokio::test(flavor = "current_thread")]
async fn maintained_archive_round_trips_bytes_files_directories_modes_and_copy_out_errors() {
    let home = tempfile::tempdir().unwrap();
    let source = home.path().join("résumé.txt");
    let payload: Vec<u8> = (0..300_000).map(|n| (n % 251) as u8).collect();
    std::fs::write(&source, &payload).unwrap();
    let image = GenericImage::new(
        "debian@sha256",
        "99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3",
    )
    .with_entrypoint("/bin/sh")
    .with_wait_for(WaitFor::message_on_stdout("fixture ready"))
    .with_network("none")
    .with_copy_to(
        CopyTargetOptions::new("/fixture/raw.bin").with_mode(0o600),
        payload.clone(),
    )
    .with_copy_to("/fixture/file.bin", source)
    .with_copy_to("/fixture/directory", home.path())
    .with_cmd(["-c", "printf 'fixture ready\n'; exec sleep 120"])
    .with_label("ai.ultracareer.fixture", "testcontainers-maintained-tar")
    .with_security_opt("no-new-privileges")
    .with_host_config_modifier(|config| {
        config.memory = Some(128 * 1024 * 1024);
        config.nano_cpus = Some(1_000_000_000);
        config.pids_limit = Some(32);
        config.cap_drop = Some(vec!["ALL".into()]);
    });
    let container = image.start().await.unwrap();
    for path in [
        "/fixture/raw.bin",
        "/fixture/file.bin",
        "/fixture/directory/résumé.txt",
    ] {
        let copied = container.copy_file_from(path, Vec::new()).await.unwrap();
        assert_eq!(copied, payload);
    }
    let mut buffer = vec![9];
    container
        .copy_file_from("/fixture/raw.bin", &mut buffer)
        .await
        .unwrap();
    assert_eq!(buffer, payload);
    let target = home.path().join("copied.bin");
    container
        .copy_file_from("/fixture/file.bin", target.as_path())
        .await
        .unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), payload);
    let mut mode = container
        .exec(ExecCommand::new(["stat", "-c", "%a", "/fixture/raw.bin"]))
        .await
        .unwrap();
    assert_eq!(mode.stdout_to_vec().await.unwrap(), b"600\n");
    assert!(matches!(
        container
            .copy_file_from("/fixture/directory", Vec::new())
            .await,
        Err(TestcontainersError::Client(
            ClientError::CopyFromContainerError(CopyFromContainerError::IsDirectory)
        ))
    ));
    container.rm().await.unwrap();
}
