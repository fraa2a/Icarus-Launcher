//! Pteron profile management interface

use crate::event::LoadingBarType;
use crate::event::emit::{
    emit_loading, init_loading, loading_try_for_each_concurrent,
};
use crate::pack::install_from::{
    EnvType, PackDependency, PackFile, PackFileHash, PackFormat,
};
use crate::state::{
    CacheBehaviour, CachedEntry, ContentItem, Credentials, Dependency,
    JavaVersion, LinkedModpackInfo, ProcessMetadata, ProfileFile,
    ProfileInstallStage, ProjectType, SideType,
};

use crate::event::{ProfilePayloadType, emit::emit_profile};
use crate::util::fetch;
use crate::util::io::{self, IOError};
pub use crate::{State, state::Profile};
use async_zip::tokio::write::ZipFileWriter;
use async_zip::{Compression, ZipEntryBuilder};
use path_util::SafeRelativeUtf8UnixPathBuf;

use std::collections::{HashMap, HashSet};

use crate::data::Settings;
use crate::server_address::ServerAddress;
use dashmap::DashMap;
use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{fs::File, process::Command, sync::RwLock};
use tokio_util::compat::FuturesAsyncWriteCompatExt;

const NEVER_EXPORTABLE_PATH_PREFIXES: &[&str] =
    &["profile.json", "Icarus_logs", ".fabric", "__MACOSX"];
const NEVER_EXPORTABLE_PATH_SUFFIXES: &[&str] = &[".DS_Store"];

#[derive(Default)]
struct ExportSelectionNode {
    selected: bool,
    has_included_rule: bool,
    children: HashMap<String, ExportSelectionNode>,
}

#[derive(Default)]
struct ExportSelection {
    root: ExportSelectionNode,
}

impl ExportSelection {
    fn new(included_paths: Vec<String>) -> Self {
        let mut selection = Self::default();

        for path in included_paths {
            let Ok(path) = SafeRelativeUtf8UnixPathBuf::try_from(path) else {
                continue;
            };
            if path.as_str().is_empty() || !is_path_exportable(&path) {
                continue;
            }
            selection.root.insert(path.as_str());
        }

        selection
    }

    fn is_included(&self, path: &SafeRelativeUtf8UnixPathBuf) -> bool {
        self.resolve(path).0
    }

    fn should_visit_directory(
        &self,
        path: &SafeRelativeUtf8UnixPathBuf,
    ) -> bool {
        let (selected, node) = self.resolve(path);
        selected || node.is_some_and(|node| node.has_included_rule)
    }

    fn resolve(
        &self,
        path: &SafeRelativeUtf8UnixPathBuf,
    ) -> (bool, Option<&ExportSelectionNode>) {
        let mut node = &self.root;
        let mut selected = node.selected;

        for segment in path.as_str().split('/') {
            let Some(child) = node.children.get(segment) else {
                return (selected, None);
            };
            node = child;
            selected |= node.selected;
        }

        (selected, Some(node))
    }
}

impl ExportSelectionNode {
    fn insert(&mut self, path: &str) {
        self.has_included_rule = true;

        let mut node = self;
        for segment in path.split('/') {
            node = node.children.entry(segment.to_string()).or_default();
            node.has_included_rule = true;
        }
        node.selected = true;
    }
}

pub mod create;
pub mod sync;
pub mod update;

#[derive(Debug, Clone)]
pub enum QuickPlayType {
    None,
    Singleplayer(String),
    Server(ServerAddress),
}

/// Remove a profile
#[tracing::instrument]
pub async fn remove(path: &str) -> crate::Result<()> {
    let state = State::get().await?;
    Profile::remove(path, &state.pool).await?;

    emit_profile(path, ProfilePayloadType::Removed).await?;

    Ok(())
}

/// Get a profile by relative path (or, name)
#[tracing::instrument]
pub async fn get(path: &str) -> crate::Result<Option<Profile>> {
    let state = State::get().await?;
    let profile = Profile::get(path, &state.pool).await?;

    Ok(profile)
}

#[tracing::instrument]
pub async fn get_many(paths: &[&str]) -> crate::Result<Vec<Profile>> {
    let state = State::get().await?;
    let profiles = Profile::get_many(paths, &state.pool).await?;
    Ok(profiles)
}

#[tracing::instrument]
pub async fn get_projects(
    path: &str,
    cache_behaviour: Option<CacheBehaviour>,
) -> crate::Result<DashMap<String, ProfileFile>> {
    let state = State::get().await?;

    if let Some(profile) = get(path).await? {
        let files = profile
            .get_projects(cache_behaviour, &state.pool, &state.api_semaphore)
            .await?;

        Ok(files)
    } else {
        Err(crate::ErrorKind::UnmanagedProfileError(path.to_string())
            .as_error())
    }
}

#[tracing::instrument]
pub async fn get_installed_project_ids(
    path: &str,
) -> crate::Result<Vec<String>> {
    let state = State::get().await?;

    if let Some(profile) = get(path).await? {
        let ids = profile
            .get_installed_project_ids(&state.pool, &state.api_semaphore)
            .await?;
        Ok(ids)
    } else {
        Err(crate::ErrorKind::UnmanagedProfileError(path.to_string())
            .as_error())
    }
}

/// Get content items with rich metadata for a profile
///
/// Returns content items filtered to exclude modpack files (if linked),
/// sorted alphabetically by project name.
#[tracing::instrument]
pub async fn get_content_items(
    path: &str,
    cache_behaviour: Option<CacheBehaviour>,
) -> crate::Result<Vec<ContentItem>> {
    let state = State::get().await?;

    if let Some(profile) = get(path).await? {
        let items = crate::state::get_content_items(
            &profile,
            cache_behaviour,
            &state.pool,
            &state.api_semaphore,
        )
        .await?;
        Ok(items)
    } else {
        Err(crate::ErrorKind::UnmanagedProfileError(path.to_string())
            .as_error())
    }
}

/// Get content items that are part of the linked modpack
///
/// Returns the modpack's dependencies as ContentItem list.
/// Returns empty vec if the profile is not linked to a modpack.
#[tracing::instrument]
pub async fn get_linked_modpack_content(
    path: &str,
    cache_behaviour: Option<CacheBehaviour>,
) -> crate::Result<Vec<ContentItem>> {
    let state = State::get().await?;

    if let Some(profile) = get(path).await? {
        let items = crate::state::get_linked_modpack_content(
            &profile,
            cache_behaviour,
            &state.pool,
            &state.api_semaphore,
        )
        .await?;
        Ok(items)
    } else {
        Err(crate::ErrorKind::UnmanagedProfileError(path.to_string())
            .as_error())
    }
}

/// Convert a list of dependencies into ContentItems with rich metadata
#[tracing::instrument]
pub async fn get_dependencies_as_content_items(
    dependencies: Vec<Dependency>,
    cache_behaviour: Option<CacheBehaviour>,
) -> crate::Result<Vec<ContentItem>> {
    let state = State::get().await?;

    let items = crate::state::dependencies_to_content_items(
        &dependencies,
        cache_behaviour,
        &state.pool,
        &state.api_semaphore,
    )
    .await?;
    Ok(items)
}

/// Get linked modpack info for a profile
///
/// Returns project, version, and owner information for the linked modpack,
/// or None if the profile is not linked to a modpack.
#[tracing::instrument]
pub async fn get_linked_modpack_info(
    path: &str,
    cache_behaviour: Option<CacheBehaviour>,
) -> crate::Result<Option<LinkedModpackInfo>> {
    let state = State::get().await?;

    if let Some(profile) = get(path).await? {
        let info = crate::state::get_linked_modpack_info(
            &profile,
            cache_behaviour,
            &state.pool,
            &state.api_semaphore,
        )
        .await?;
        Ok(info)
    } else {
        Err(crate::ErrorKind::UnmanagedProfileError(path.to_string())
            .as_error())
    }
}

/// Get profile's full path in the filesystem
#[tracing::instrument]
pub async fn get_full_path(path: &str) -> crate::Result<PathBuf> {
    let state = State::get().await?;
    let profiles_dir = state.directories.profiles_dir();

    let full_path = io::canonicalize(profiles_dir.join(path))?;
    Ok(full_path)
}

/// Get mod's full path in the filesystem
#[tracing::instrument]
pub async fn get_mod_full_path(
    profile_path: &str,
    project_path: &str,
) -> crate::Result<PathBuf> {
    let path = get_full_path(profile_path).await?;

    Ok(path.join(project_path))
}

/// Edit a profile using a given asynchronous closure
pub async fn edit<Fut>(
    path: &str,
    action: impl Fn(&mut Profile) -> Fut,
) -> crate::Result<()>
where
    Fut: Future<Output = crate::Result<()>>,
{
    let state = State::get().await?;

    if let Some(mut profile) = get(path).await? {
        action(&mut profile).await?;
        profile.upsert(&state.pool).await?;

        emit_profile(path, ProfilePayloadType::Edited).await?;

        Ok(())
    } else {
        Err(crate::ErrorKind::UnmanagedProfileError(path.to_string())
            .as_error())
    }
}

/// Edits a profile's icon
pub async fn edit_icon(
    path: &str,
    icon_path: Option<&Path>,
) -> crate::Result<()> {
    let state = State::get().await?;

    if let Some(mut profile) = get(path).await? {
        if let Some(icon) = icon_path {
            let bytes = io::read(icon).await?;

            profile
                .set_icon(
                    &state.directories.caches_dir(),
                    &state.io_semaphore,
                    bytes::Bytes::from(bytes),
                    &icon.to_string_lossy(),
                )
                .await?;
        } else {
            profile.icon_path = None;
        }

        profile.upsert(&state.pool).await?;

        emit_profile(path, ProfilePayloadType::Edited).await?;

        Ok(())
    } else {
        Err(crate::ErrorKind::UnmanagedProfileError(path.to_string())
            .as_error())
    }
}

// Gets the optimal JRE key for the given profile, using Daedalus
// Generally this would be used for profile_create, to get the optimal JRE key
// this can be overwritten by the user a profile-by-profile basis
pub async fn get_optimal_jre_key(
    path: &str,
) -> crate::Result<Option<JavaVersion>> {
    let state = State::get().await?;

    if let Some(profile) = get(path).await? {
        let (minecraft, version_index) =
            crate::launcher::resolve_minecraft_manifest(
                &profile.game_version,
                &state,
            )
            .await?;
        let version = &minecraft.versions[version_index];

        let loader_version = crate::launcher::get_loader_version_from_profile(
            &profile.game_version,
            profile.loader,
            profile.loader_version.as_deref(),
        )
        .await?;

        // Get detailed manifest info from Daedalus
        let version_info = crate::launcher::download::download_version_info(
            &state,
            version,
            loader_version.as_ref(),
            None,
            None,
        )
        .await?;

        let version = crate::launcher::get_java_version_from_profile(
            &profile,
            &version_info,
        )
        .await?;

        Ok(version)
    } else {
        Err(crate::ErrorKind::UnmanagedProfileError(path.to_string())
            .as_error())
    }
}

/// Get a copy of the profile set
#[tracing::instrument]
pub async fn list() -> crate::Result<Vec<Profile>> {
    let state = State::get().await?;
    let profiles = Profile::get_all(&state.pool).await?;
    Ok(profiles)
}

/// Installs/Repairs a profile
#[tracing::instrument]
pub async fn install(path: &str, force: bool) -> crate::Result<()> {
    if let Some(profile) = get(path).await? {
        let result =
            crate::launcher::install_minecraft(&profile, None, force).await;
        if result.is_err() {
            // Re-read the profile to get the current install_stage, as
            // install_minecraft may have changed it (e.g. to MinecraftInstalling)
            let current_stage = get(path)
                .await
                .ok()
                .flatten()
                .map(|p| p.install_stage)
                .unwrap_or(ProfileInstallStage::NotInstalled);
            if current_stage != ProfileInstallStage::Installed {
                edit(path, |prof| {
                    prof.install_stage = ProfileInstallStage::NotInstalled;
                    async { Ok(()) }
                })
                .await?;
            }
        }
        result?;
    } else {
        return Err(crate::ErrorKind::UnmanagedProfileError(path.to_string())
            .as_error());
    }
    Ok(())
}

#[tracing::instrument]
pub async fn update_all_projects(
    profile_path: &str,
) -> crate::Result<HashMap<String, String>> {
    if let Some(profile) = get(profile_path).await? {
        let loading_bar = init_loading(
            LoadingBarType::ProfileUpdate {
                profile_path: profile.path.clone(),
                profile_name: profile.name.clone(),
            },
            100.0,
            "Updating profile",
        )
        .await?;

        let state = State::get().await?;
        let keys = profile
            .get_projects(
                Some(CacheBehaviour::MustRevalidate),
                &state.pool,
                &state.api_semaphore,
            )
            .await?
            .into_iter()
            .filter(|(_, project)| project.update_version_id.is_some())
            .map(|x| x.0)
            .collect::<Vec<_>>();
        let len = keys.len();

        let map = Arc::new(RwLock::new(HashMap::new()));

        use futures::StreamExt;
        loading_try_for_each_concurrent(
            futures::stream::iter(keys).map(Ok::<String, crate::Error>),
            None,
            Some(&loading_bar),
            100.0,
            len,
            None,
            |project| async {
                let map = map.clone();

                async move {
                    let new_path =
                        update_project(profile_path, &project, Some(true))
                            .await?;

                    map.write().await.insert(project, new_path);

                    Ok(())
                }
                .await
            },
        )
        .await?;

        emit_profile(profile_path, ProfilePayloadType::Edited).await?;

        Ok(Arc::try_unwrap(map).unwrap().into_inner())
    } else {
        Err(
            crate::ErrorKind::UnmanagedProfileError(profile_path.to_string())
                .as_error(),
        )
    }
}

/// Updates a project to the latest version
/// Uses and returns the relative path to the project
#[tracing::instrument]
pub async fn update_project(
    profile_path: &str,
    project_path: &str,
    skip_send_event: Option<bool>,
) -> crate::Result<String> {
    if let Some(profile) = get(profile_path).await? {
        let state = State::get().await?;
        if let Some((_, file)) = profile
            .get_projects(
                Some(CacheBehaviour::MustRevalidate),
                &state.pool,
                &state.api_semaphore,
            )
            .await?
            .remove(project_path)
            && let Some(update_version) = &file.update_version_id
        {
            let mut path = Profile::add_project_version(
                profile_path,
                update_version,
                fetch::DownloadReason::Standalone,
                &state.pool,
                &state.fetch_semaphore,
                &state.io_semaphore,
            )
            .await?;

            if project_path.ends_with(".disabled") {
                path = Profile::toggle_disable_project(profile_path, &path)
                    .await?;
            }

            if path != project_path {
                Profile::remove_project(profile_path, project_path).await?;
            }

            if !skip_send_event.unwrap_or(false) {
                emit_profile(profile_path, ProfilePayloadType::Edited).await?;
            }

            return Ok(path);
        }

        Err(crate::ErrorKind::InputError(
            "This project cannot be updated!".to_string(),
        )
        .as_error())
    } else {
        Err(
            crate::ErrorKind::UnmanagedProfileError(profile_path.to_string())
                .as_error(),
        )
    }
}

/// Add a project from a version
/// Returns the relative path to the project as a ProjectPathId
#[tracing::instrument]
pub async fn add_project_from_version(
    profile_path: &str,
    version_id: &str,
    reason: fetch::DownloadReason,
) -> crate::Result<String> {
    let state = State::get().await?;

    let project_path = Profile::add_project_version(
        profile_path,
        version_id,
        reason,
        &state.pool,
        &state.fetch_semaphore,
        &state.io_semaphore,
    )
    .await?;

    emit_profile(profile_path, ProfilePayloadType::Edited).await?;

    Ok(project_path)
}

/// Add a project from an FS path
/// Uses and returns the relative path to the project as a ProjectPathId
#[tracing::instrument]
pub async fn add_project_from_path(
    profile_path: &str,
    path: &Path,
    project_type: Option<ProjectType>,
) -> crate::Result<String> {
    let state = State::get().await?;

    let file = io::read(path).await?;
    let file_name = path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    let path = Profile::add_project_bytes(
        profile_path,
        &file_name,
        bytes::Bytes::from(file),
        None,
        project_type,
        &state.io_semaphore,
        &state.pool,
    )
    .await?;

    Ok(path)
}

/// Toggle whether a project is disabled or not
/// Project path should be relative to the profile
/// returns the new state, relative to the profile
#[tracing::instrument]
pub async fn toggle_disable_project(
    profile_path: &str,
    project: &str,
) -> crate::Result<String> {
    let res = Profile::toggle_disable_project(profile_path, project).await?;

    emit_profile(profile_path, ProfilePayloadType::Edited).await?;

    Ok(res)
}

/// Remove a project from a profile
/// Uses and returns the relative path to the project
#[tracing::instrument]
pub async fn remove_project(
    profile_path: &str,
    project: &str,
) -> crate::Result<()> {
    Profile::remove_project(profile_path, project).await?;

    emit_profile(profile_path, ProfilePayloadType::Edited).await?;

    Ok(())
}

/// Exports the profile to a Modrinth-formatted .mrpack file
// Version ID of uploaded version (ie 1.1.5), not the unique identifying ID of the version (nvrqJg44)
#[tracing::instrument(skip_all)]
pub async fn export_mrpack(
    profile_path: &str,
    export_path: PathBuf,
    included_export_candidates: Vec<String>, // which folders/files to include in the export
    version_id: Option<String>,
    description: Option<String>,
    _name: Option<String>,
) -> crate::Result<()> {
    let state = State::get().await?;
    let _permit: tokio::sync::SemaphorePermit =
        state.io_semaphore.0.acquire().await?;
    let profile = get(profile_path).await?.ok_or_else(|| {
        crate::ErrorKind::OtherError(format!(
            "Tried to export a nonexistent or unloaded profile at path {profile_path}!"
        ))
    })?;

    let export_selection = ExportSelection::new(included_export_candidates);
    let profile_base_path = get_full_path(profile_path).await?;

    let mut file = File::create(&export_path)
        .await
        .map_err(|e| IOError::with_path(e, &export_path))?;
    let mut writer = ZipFileWriter::with_tokio(&mut file).force_no_zip64();

    // Create mrpack json configuration file
    let version_id = version_id.unwrap_or("1.0.0".to_string());
    let mut packfile =
        create_mrpack_json(&profile, version_id, description).await?;
    packfile.files.retain(|file| {
        is_path_exportable(&file.path)
            && export_selection.is_included(&file.path)
    });
    let packfile_paths = packfile
        .files
        .iter()
        .map(|file| file.path.as_str().to_string())
        .collect::<HashSet<_>>();

    let loading_bar = init_loading(
        LoadingBarType::ZipExtract {
            profile_path: profile.path.clone(),
            profile_name: profile.name.clone(),
        },
        1.0,
        "Exporting profile to .mrpack",
    )
    .await?;

    let mut directories = vec![profile_base_path.clone()];
    while let Some(directory) = directories.pop() {
        let mut read_dir = io::read_dir(&directory).await?;
        while let Some(entry) = read_dir
            .next_entry()
            .await
            .map_err(|e| IOError::with_path(e, &directory))?
        {
            let path = entry.path();
            let relative_path =
                pack_get_relative_path(&profile_base_path, &path)?;
            if !is_path_exportable(&relative_path) {
                continue;
            }

            let file_type = entry
                .file_type()
                .await
                .map_err(|e| IOError::with_path(e, &path))?;
            if file_type.is_dir() {
                if export_selection.should_visit_directory(&relative_path) {
                    directories.push(path);
                }
                continue;
            }
            if !file_type.is_file()
                || !export_selection.is_included(&relative_path)
                || packfile_paths.contains(relative_path.as_str())
            {
                continue;
            }

            let mut stream = writer
                .write_entry_stream(
                    ZipEntryBuilder::new(
                        format!("overrides/{relative_path}").into(),
                        Compression::Deflate,
                    )
                    .build(),
                )
                .await?
                .compat_write();
            let mut source = File::open(&path)
                .await
                .map_err(|e| IOError::with_path(e, &path))?;
            tokio::io::copy(&mut source, &mut stream)
                .await
                .map_err(IOError::from)?;
            stream.into_inner().close().await?;
        }
    }

    // Add modrinth json to the zip
    let data = serde_json::to_vec_pretty(&packfile)?;
    let builder = ZipEntryBuilder::new(
        "modrinth.index.json".to_string().into(),
        Compression::Deflate,
    );
    writer.write_entry_whole(builder, &data).await?;

    writer.close().await?;
    emit_loading(&loading_bar, 1.0, None)?;

    Ok(())
}

fn is_path_exportable(relative_path: &SafeRelativeUtf8UnixPathBuf) -> bool {
    let path = relative_path.as_str();

    !NEVER_EXPORTABLE_PATH_PREFIXES.iter().any(|prefix| {
        path == *prefix
            || path
                .strip_prefix(prefix)
                .is_some_and(|suffix| suffix.starts_with('/'))
    }) && !NEVER_EXPORTABLE_PATH_SUFFIXES
        .iter()
        .any(|suffix| path.ends_with(suffix))
}

// Given a folder path, populate a Vec of all the subfolders and files, at most 2 layers deep
// profile
// -- folder1
// -- folder2
//    -- innerfolder
//       -- innerfile
//    -- folder2file
// -- file1
// => [folder1, folder2/innerfolder, folder2/folder2file, file1]
#[tracing::instrument]
pub async fn get_pack_export_candidates(
    profile_path: &str,
) -> crate::Result<Vec<SafeRelativeUtf8UnixPathBuf>> {
    let mut path_list = Vec::new();

    let profile_base_dir = get_full_path(profile_path).await?;
    let mut read_dir = io::read_dir(&profile_base_dir).await?;
    while let Some(entry) = read_dir
        .next_entry()
        .await
        .map_err(|e| IOError::with_path(e, &profile_base_dir))?
    {
        let path = entry.path();
        let relative_path = pack_get_relative_path(&profile_base_dir, &path)?;
        if !is_path_exportable(&relative_path) {
            continue;
        }

        let file_type = entry
            .file_type()
            .await
            .map_err(|e| IOError::with_path(e, &path))?;
        if file_type.is_dir() {
            // Two layers of files/folders if its a folder
            let mut read_dir = io::read_dir(&path).await?;
            while let Some(entry) = read_dir
                .next_entry()
                .await
                .map_err(|e| IOError::with_path(e, &profile_base_dir))?
            {
                let path = entry.path();
                let file_type = entry
                    .file_type()
                    .await
                    .map_err(|e| IOError::with_path(e, &path))?;
                if !file_type.is_dir() && !file_type.is_file() {
                    continue;
                }

                let relative_path =
                    pack_get_relative_path(&profile_base_dir, &path)?;
                if is_path_exportable(&relative_path) {
                    path_list.push(relative_path);
                }
            }
        } else if file_type.is_file() {
            // One layer of files/folders if its a file
            path_list.push(relative_path);
        }
    }
    Ok(path_list)
}

fn pack_get_relative_path(
    profile_path: &PathBuf,
    path: &PathBuf,
) -> crate::Result<SafeRelativeUtf8UnixPathBuf> {
    Ok(SafeRelativeUtf8UnixPathBuf::try_from(
        path.strip_prefix(profile_path)
            .map_err(|_| {
                crate::ErrorKind::FSError(format!(
                    "Path {path:?} does not correspond to a profile"
                ))
            })?
            .components()
            .map(|c| c.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/"),
    )?)
}

/// Run Minecraft using a profile and the default credentials, logged in credentials,
/// failing with an error if no credentials are available
#[tracing::instrument]
pub async fn run(
    path: &str,
    quick_play_type: QuickPlayType,
) -> crate::Result<ProcessMetadata> {
    let state = State::get().await?;

    let default_account = Credentials::get_default_credential(&state.pool)
        .await?
        .ok_or_else(|| crate::ErrorKind::NoCredentialsError.as_error())?;

    run_credentials(path, &default_account, quick_play_type).await
}

/// Run Minecraft using a profile, and credentials for authentication
#[tracing::instrument(skip(credentials))]
async fn run_credentials(
    path: &str,
    credentials: &Credentials,
    quick_play_type: QuickPlayType,
) -> crate::Result<ProcessMetadata> {
    let state = State::get().await?;
    let settings = Settings::get(&state.pool).await?;
    let profile = get(path).await?.ok_or_else(|| {
        crate::ErrorKind::OtherError(format!(
            "Tried to run a nonexistent or unloaded profile at path {path}!"
        ))
    })?;

    crate::sync::apply_sync_to_instance(
        &settings.sync,
        &crate::profile::get_full_path(&profile.path).await?,
        &state.directories.synced_dir(),
        profile.sync_enabled,
        &profile.sync_overrides,
    )?;

    let pre_launch_hooks = profile
        .hooks
        .pre_launch
        .as_ref()
        .or(settings.hooks.pre_launch.as_ref())
        .filter(|hook_command| !hook_command.is_empty());
    if let Some(hook) = pre_launch_hooks {
        // TODO: hook parameters
        let mut cmd = shlex::split(hook)
            .ok_or_else(|| {
                crate::ErrorKind::LauncherError(format!(
                    "Invalid pre-launch command: {hook}",
                ))
            })?
            .into_iter();

        if let Some(command) = cmd.next() {
            let full_path = get_full_path(&profile.path).await?;
            let result = Command::new(command)
                .args(cmd)
                .current_dir(&full_path)
                .spawn()
                .map_err(|e| IOError::with_path(e, &full_path))?
                .wait()
                .await
                .map_err(IOError::from)?;

            if !result.success() {
                return Err(crate::ErrorKind::LauncherError(format!(
                    "Non-zero exit code for pre-launch hook: {}",
                    result.code().unwrap_or(-1)
                ))
                .as_error());
            }
        }
    }

    let java_args = profile
        .extra_launch_args
        .clone()
        .unwrap_or(settings.extra_launch_args);

    let wrapper = profile
        .hooks
        .wrapper
        .clone()
        .or(settings.hooks.wrapper)
        .filter(|hook_command| !hook_command.is_empty());

    let memory = profile.memory.unwrap_or(settings.memory);
    let resolution =
        profile.game_resolution.unwrap_or(settings.game_resolution);

    let env_args = profile
        .custom_env_vars
        .clone()
        .unwrap_or(settings.custom_env_vars);

    // Post post exit hooks
    let post_exit_hook = profile
        .hooks
        .post_exit
        .clone()
        .or(settings.hooks.post_exit)
        .filter(|hook_command| !hook_command.is_empty());

    // Any options.txt settings that we want set, add here
    let mut mc_set_options: Vec<(String, String)> = vec![];
    if let Some(fullscreen) = profile.force_fullscreen {
        // Profile fullscreen setting takes priority
        mc_set_options.push(("fullscreen".to_string(), fullscreen.to_string()));
    } else if settings.force_fullscreen {
        // If global settings wants to force a fullscreen, do it
        mc_set_options.push(("fullscreen".to_string(), "true".to_string()));
    }

    crate::launcher::launch_minecraft(
        &java_args,
        &env_args,
        &mc_set_options,
        &wrapper,
        &memory,
        &resolution,
        credentials,
        post_exit_hook,
        &profile,
        quick_play_type,
    )
    .await
}

pub async fn kill(path: &str) -> crate::Result<()> {
    let state = State::get().await?;
    let processes = crate::api::process::get_by_profile_path(path).await?;

    for process in processes {
        state.process_manager.kill(process.uuid).await?;
    }

    Ok(())
}

/// Consolidates recently recorded playtime into the local total.
#[tracing::instrument]
pub async fn try_update_playtime(path: &str) -> crate::Result<()> {
    let profile = get(path).await?.ok_or_else(|| {
        crate::ErrorKind::OtherError(format!(
            "Tried to update playtime for a nonexistent or unloaded profile at path {path}!"
        ))
    })?;
    let updated_recent_playtime = profile.recent_time_played;

    if updated_recent_playtime > 0 {
        edit(&profile.path, |prof| {
            prof.submitted_time_played += updated_recent_playtime;
            prof.recent_time_played = 0;

            async { Ok(()) }
        })
        .await?;
    }

    Ok(())
}

/// Creates a json configuration for a .mrpack zipped file
// Version ID of uploaded version (ie 1.1.5), not the unique identifying ID of the version (nvrqJg44)
#[tracing::instrument(skip_all)]
pub async fn create_mrpack_json(
    profile: &Profile,
    version_id: String,
    description: Option<String>,
) -> crate::Result<PackFormat> {
    // Add loader version to dependencies
    let mut dependencies = HashMap::new();
    match (profile.loader, profile.loader_version.clone()) {
        (crate::prelude::ModLoader::Forge, Some(v)) => {
            dependencies.insert(PackDependency::Forge, v)
        }
        (crate::prelude::ModLoader::NeoForge, Some(v)) => {
            dependencies.insert(PackDependency::NeoForge, v)
        }
        (crate::prelude::ModLoader::Fabric, Some(v)) => {
            dependencies.insert(PackDependency::FabricLoader, v)
        }
        (crate::prelude::ModLoader::Quilt, Some(v)) => {
            dependencies.insert(PackDependency::QuiltLoader, v)
        }
        (crate::prelude::ModLoader::Vanilla, _) => None,
        _ => {
            return Err(crate::ErrorKind::OtherError(
                "Loader version mismatch".to_string(),
            )
            .into());
        }
    };
    dependencies
        .insert(PackDependency::Minecraft, profile.game_version.clone());

    let state = State::get().await?;
    let projects = profile
        .get_projects(
            Some(CacheBehaviour::MustRevalidate),
            &state.pool,
            &state.api_semaphore,
        )
        .await?
        .into_iter()
        .filter_map(|(path, file)| {
            file.metadata
                .map(|metadata| (path, file.hash, metadata.version_id))
        })
        .collect::<Vec<_>>();
    let versions = CachedEntry::get_version_many(
        &projects.iter().map(|x| &*x.2).collect::<Vec<_>>(),
        None,
        &state.pool,
        &state.api_semaphore,
    )
    .await?;

    let files = projects
        .into_iter()
        .filter_map(|(path, hash, version_id)| {
            let version = versions.iter().find(|x| x.id == version_id)?;
            let file = version.files.iter().find(|file| {
                file.hashes
                    .get("sha1")
                    .is_some_and(|file_hash| file_hash == &hash)
            })?;

            let mut env = HashMap::new();
            // TODO: envtype should be a controllable option (in general or at least .mrpack exporting)
            // For now, assume required.
            // env.insert(EnvType::Client, project.client_side.clone());
            // env.insert(EnvType::Server, project.server_side.clone());
            env.insert(EnvType::Client, SideType::Required);
            env.insert(EnvType::Server, SideType::Required);

            let file_size = file.size;
            let downloads = vec![file.url.clone()];
            let hashes = file
                .hashes
                .clone()
                .into_iter()
                .map(|(h1, h2)| (PackFileHash::from(h1), h2))
                .collect();

            Some(Ok(PackFile {
                path: match path.try_into() {
                    Ok(path) => path,
                    Err(_) => {
                        return Some(Err(crate::ErrorKind::OtherError(
                            "Invalid file path in project".into(),
                        )
                        .as_error()));
                    }
                },
                hashes,
                env: Some(env),
                downloads,
                file_size,
            }))
        })
        .collect::<crate::Result<Vec<PackFile>>>()?;

    Ok(PackFormat {
        game: "minecraft".to_string(),
        format_version: 1,
        version_id,
        name: profile.name.clone(),
        summary: description,
        files,
        dependencies,
    })
}

// Given a folder path, populate a Vec of all the files in the folder, recursively
#[async_recursion::async_recursion]
pub async fn add_all_recursive_folder_paths(
    path: &Path,
    path_list: &mut Vec<PathBuf>,
) -> crate::Result<()> {
    let mut read_dir = io::read_dir(path).await?;
    while let Some(entry) = read_dir
        .next_entry()
        .await
        .map_err(|e| IOError::with_path(e, path))?
    {
        let path = entry.path();
        if path.is_dir() {
            add_all_recursive_folder_paths(&path, path_list).await?;
        } else {
            path_list.push(path);
        }
    }
    Ok(())
}

pub fn sanitize_profile_name(input: &str) -> String {
    input.replace(
        ['/', '\\', '?', '*', ':', '\'', '\"', '|', '<', '>', '!'],
        "_",
    )
}

#[cfg(test)]
mod export_tests {
    use super::*;

    fn relative_path(path: &str) -> SafeRelativeUtf8UnixPathBuf {
        SafeRelativeUtf8UnixPathBuf::try_from(path.to_string()).unwrap()
    }

    #[test]
    fn export_selection_matches_path_segments() {
        let selection = ExportSelection::new(vec!["config".to_string()]);

        assert!(selection.is_included(&relative_path("config")));
        assert!(selection.is_included(&relative_path("config/example.toml")));
        assert!(
            !selection
                .is_included(&relative_path("config-backup/example.toml"))
        );
    }

    #[test]
    fn export_selection_visits_only_selected_branches() {
        let selection = ExportSelection::new(vec![
            "resourcepacks/example/assets".to_string(),
        ]);

        assert!(
            selection.should_visit_directory(&relative_path("resourcepacks"))
        );
        assert!(
            selection.should_visit_directory(&relative_path(
                "resourcepacks/example"
            ))
        );
        assert!(
            !selection.should_visit_directory(&relative_path("shaderpacks"))
        );
    }

    #[test]
    fn export_denylist_matches_whole_path_segments_and_suffixes() {
        assert!(!is_path_exportable(&relative_path("profile.json")));
        assert!(!is_path_exportable(&relative_path(
            "Icarus_logs/latest.log"
        )));
        assert!(!is_path_exportable(&relative_path("config/.DS_Store")));
        assert!(is_path_exportable(&relative_path(
            "Icarus_logs-backup/latest.log"
        )));
        assert!(is_path_exportable(&relative_path("config/profile.json")));
    }
}
