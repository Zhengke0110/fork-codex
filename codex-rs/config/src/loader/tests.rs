use super::*;
use codex_file_system::CopyOptions;
use codex_file_system::CreateDirectoryOptions;
use codex_file_system::ExecutorFileSystemFuture;
use codex_file_system::FileMetadata;
use codex_file_system::FileSystemReadStream;
use codex_file_system::FileSystemSandboxContext;
use codex_file_system::ReadDirectoryEntry;
use codex_file_system::RemoveOptions;
use codex_utils_path_uri::PathUri;
use pretty_assertions::assert_eq;
use tempfile::tempdir;

struct TestFileSystem;

impl ExecutorFileSystem for TestFileSystem {
    fn canonicalize<'a>(
        &'a self,
        path: &'a PathUri,
        _sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, PathUri> {
        Box::pin(async move {
            let path = path.to_abs_path()?;
            let canonicalized = path.canonicalize()?;
            Ok(PathUri::from_abs_path(&canonicalized))
        })
    }

    fn read_file<'a>(
        &'a self,
        path: &'a PathUri,
        _sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, Vec<u8>> {
        Box::pin(async move {
            let path = path.to_abs_path()?;
            tokio::fs::read(path.as_path()).await
        })
    }

    fn read_file_stream<'a>(
        &'a self,
        _path: &'a PathUri,
        _sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, FileSystemReadStream> {
        Box::pin(async {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "test filesystem does not support streaming reads",
            ))
        })
    }

    fn write_file<'a>(
        &'a self,
        _path: &'a PathUri,
        _contents: Vec<u8>,
        _sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, ()> {
        Box::pin(async move { unimplemented!("test filesystem only supports reads") })
    }

    fn create_directory<'a>(
        &'a self,
        _path: &'a PathUri,
        _create_directory_options: CreateDirectoryOptions,
        _sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, ()> {
        Box::pin(async move { unimplemented!("test filesystem only supports reads") })
    }

    fn get_metadata<'a>(
        &'a self,
        _path: &'a PathUri,
        _sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, FileMetadata> {
        Box::pin(async move { unimplemented!("test filesystem only supports reads") })
    }

    fn read_directory<'a>(
        &'a self,
        _path: &'a PathUri,
        _sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, Vec<ReadDirectoryEntry>> {
        Box::pin(async move { unimplemented!("test filesystem only supports reads") })
    }

    fn remove<'a>(
        &'a self,
        _path: &'a PathUri,
        _remove_options: RemoveOptions,
        _sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, ()> {
        Box::pin(async move { unimplemented!("test filesystem only supports reads") })
    }

    fn copy<'a>(
        &'a self,
        _source_path: &'a PathUri,
        _destination_path: &'a PathUri,
        _copy_options: CopyOptions,
        _sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, ()> {
        Box::pin(async move { unimplemented!("test filesystem only supports reads") })
    }
}

#[tokio::test]
async fn profile_v2_rejects_matching_legacy_profile_in_base_user_config() {
    let tmp = tempdir().expect("tempdir");
    let selected_config = tmp.path().join("work.config.toml");

    std::fs::write(
        tmp.path().join(CONFIG_TOML_FILE),
        r#"
model = "gpt-main"

[profiles.work]
model = "gpt-work"
"#,
    )
    .expect("write default user config");
    std::fs::write(&selected_config, r#"model = "gpt-work-v2""#)
        .expect("write selected user config");

    let mut overrides = LoaderOverrides::without_managed_config_for_tests();
    overrides.user_config_path = Some(AbsolutePathBuf::resolve_path_against_base(
        "work.config.toml",
        tmp.path(),
    ));
    overrides.user_config_profile = Some("work".parse().expect("profile-v2 name"));

    let err = load_config_layers_state(
        &TestFileSystem,
        tmp.path(),
        /*cwd*/ None,
        &[],
        overrides,
        &crate::NoopThreadConfigLoader,
    )
    .await
    .expect_err("profile-v2 should reject a matching legacy profile in base user config");

    assert_eq!(
        err.kind(),
        io::ErrorKind::InvalidData,
        "a matching legacy profile should be a hard config error"
    );
    let message = err.to_string();
    assert!(
        message.contains("--profile `work` cannot be used"),
        "unexpected error message: {message}"
    );
    assert!(
        message.contains("config.toml"),
        "unexpected error message: {message}"
    );
    assert!(
        message.contains("[profiles.work]"),
        "unexpected error message: {message}"
    );
    assert!(
        message.contains("https://developers.openai.com/codex/config-advanced#profiles"),
        "unexpected error message: {message}"
    );
}

#[tokio::test]
async fn profile_v2_rejects_matching_legacy_profile_selector_in_base_user_config() {
    let tmp = tempdir().expect("tempdir");
    let selected_config = tmp.path().join("work.config.toml");

    std::fs::write(
        tmp.path().join(CONFIG_TOML_FILE),
        r#"
profile = "work"
model = "gpt-main"
"#,
    )
    .expect("write default user config");
    std::fs::write(&selected_config, r#"model = "gpt-work-v2""#)
        .expect("write selected user config");

    let mut overrides = LoaderOverrides::without_managed_config_for_tests();
    overrides.user_config_path = Some(AbsolutePathBuf::resolve_path_against_base(
        "work.config.toml",
        tmp.path(),
    ));
    overrides.user_config_profile = Some("work".parse().expect("profile-v2 name"));

    let err = load_config_layers_state(
        &TestFileSystem,
        tmp.path(),
        /*cwd*/ None,
        &[],
        overrides,
        &crate::NoopThreadConfigLoader,
    )
    .await
    .expect_err("profile-v2 should reject a matching legacy profile selector");

    assert_eq!(
        err.kind(),
        io::ErrorKind::InvalidData,
        "a matching legacy profile selector should be a hard config error"
    );
    let message = err.to_string();
    assert!(
        message.contains("--profile `work` cannot be used"),
        "unexpected error message: {message}"
    );
    assert!(
        message.contains("profile = \"work\""),
        "unexpected error message: {message}"
    );
    assert!(
        message.contains("work.config.toml"),
        "unexpected error message: {message}"
    );
}

#[tokio::test]
async fn profile_v2_allows_unrelated_legacy_profiles_in_base_user_config() {
    let tmp = tempdir().expect("tempdir");
    let selected_config = tmp.path().join("work.config.toml");

    std::fs::write(
        tmp.path().join(CONFIG_TOML_FILE),
        r#"
model = "gpt-main"

[profiles.dev]
model = "gpt-dev"
"#,
    )
    .expect("write default user config");
    std::fs::write(&selected_config, r#"model = "gpt-work-v2""#)
        .expect("write selected user config");

    let mut overrides = LoaderOverrides::without_managed_config_for_tests();
    overrides.user_config_path = Some(AbsolutePathBuf::resolve_path_against_base(
        "work.config.toml",
        tmp.path(),
    ));
    overrides.user_config_profile = Some("work".parse().expect("profile-v2 name"));

    load_config_layers_state(
        &TestFileSystem,
        tmp.path(),
        /*cwd*/ None,
        &[],
        overrides,
        &crate::NoopThreadConfigLoader,
    )
    .await
    .expect("profile-v2 should allow unrelated legacy profiles in base user config");
}

#[test]
fn build_codex_llm_env_layer_is_opt_in_and_builds_provider_toml() {
    // nextest isolates each test in its own process, so mutating the
    // environment here is safe and cannot leak to other tests.

    // Not configured => no layer (strict opt-in).
    unsafe {
        std::env::remove_var("CODEX_LLM_API_KEY");
        std::env::remove_var("CODEX_LLM_BASE_URL");
        std::env::remove_var("CODEX_LLM_MODEL");
        std::env::remove_var("CODEX_LLM_PROVIDER");
    }
    assert!(build_codex_llm_env_layer(None).is_none());

    // Base URL only (no key) => still no layer.
    unsafe {
        std::env::set_var("CODEX_LLM_BASE_URL", "https://api.example.com");
    }
    assert!(build_codex_llm_env_layer(None).is_none());

    // Fully configured => layer with the expected provider TOML.
    unsafe {
        std::env::set_var("CODEX_LLM_API_KEY", "test-key");
        std::env::set_var("CODEX_LLM_MODEL", "deepseek-v4-pro");
        std::env::set_var("CODEX_LLM_PROVIDER", "custom");
    }
    let layer =
        build_codex_llm_env_layer(None).expect("layer built when key and base url are present");
    assert_eq!(layer.name, ConfigLayerSource::EnvLlm { file: None });
    let table = layer.config.as_table().expect("layer config is a table");
    assert_eq!(
        table.get("model").and_then(|v| v.as_str()),
        Some("deepseek-v4-pro")
    );
    assert_eq!(
        table.get("model_provider").and_then(|v| v.as_str()),
        Some("custom")
    );
    let provider = table
        .get("model_providers")
        .and_then(|v| v.as_table())
        .and_then(|t| t.get("custom"))
        .and_then(|v| v.as_table())
        .expect("model_providers.custom table");
    assert_eq!(
        provider.get("base_url").and_then(|v| v.as_str()),
        Some("https://api.example.com")
    );
    assert_eq!(
        provider.get("env_key").and_then(|v| v.as_str()),
        Some("CODEX_LLM_API_KEY")
    );
    assert_eq!(
        provider
            .get("requires_openai_auth")
            .and_then(toml::Value::as_bool),
        Some(false)
    );
    // The API key value must never be embedded in the layer; it is read
    // lazily at request time via `env_key`.
    assert!(provider.get("experimental_bearer_token").is_none());

    // Defaults: provider id is `codex-llm` and `model` omitted when unset.
    unsafe {
        std::env::remove_var("CODEX_LLM_PROVIDER");
        std::env::remove_var("CODEX_LLM_MODEL");
    }
    let layer = build_codex_llm_env_layer(None).expect("layer still built with defaults");
    let table = layer.config.as_table().expect("layer config is a table");
    assert!(table.get("model").is_none());
    assert_eq!(
        table.get("model_provider").and_then(|v| v.as_str()),
        Some("codex-llm")
    );
}

#[test]
fn build_codex_llm_env_layer_maps_api_style_and_timeout() {
    // nextest isolates each test in its own process.
    unsafe {
        std::env::set_var("CODEX_LLM_API_KEY", "test-key");
        std::env::set_var("CODEX_LLM_BASE_URL", "https://api.example.com");
        std::env::remove_var("CODEX_LLM_MODEL");
        std::env::remove_var("CODEX_LLM_PROVIDER");
        std::env::remove_var("CODEX_LLM_API_STYLE");
        std::env::remove_var("CODEX_LLM_TIMEOUT_SECONDS");
    }

    let provider_table = |layer: ConfigLayerEntry| -> toml::value::Table {
        layer
            .config
            .as_table()
            .and_then(|t| t.get("model_providers"))
            .and_then(|v| v.as_table())
            .and_then(|t| t.get("codex-llm"))
            .and_then(|v| v.as_table())
            .expect("model_providers.codex-llm table")
            .clone()
    };

    // Default API style is chat_completions; no timeout => no field.
    let provider = provider_table(build_codex_llm_env_layer(None).expect("layer built"));
    assert_eq!(
        provider.get("wire_api").and_then(|v| v.as_str()),
        Some("chat_completions")
    );
    assert!(provider.get("stream_idle_timeout_ms").is_none());

    // CODEX_LLM_API_STYLE=responses maps to wire_api=responses.
    unsafe {
        std::env::set_var("CODEX_LLM_API_STYLE", "responses");
    }
    let provider = provider_table(build_codex_llm_env_layer(None).expect("layer built"));
    assert_eq!(
        provider.get("wire_api").and_then(|v| v.as_str()),
        Some("responses")
    );

    // CODEX_LLM_TIMEOUT_SECONDS=45 maps to stream_idle_timeout_ms=45000.
    unsafe {
        std::env::set_var("CODEX_LLM_TIMEOUT_SECONDS", "45");
    }
    let provider = provider_table(build_codex_llm_env_layer(None).expect("layer built"));
    assert_eq!(
        provider
            .get("stream_idle_timeout_ms")
            .and_then(toml::Value::as_integer),
        Some(45000)
    );
}
