use std::{
	env,
	ffi::OsStr,
	io,
	os::unix::process::CommandExt,
	process::Command,
};

const NVIDIA_DISABLE_EXPLICIT_SYNC: &str = "__NV_DISABLE_EXPLICIT_SYNC";
const WEBKIT_DISABLE_DMABUF_RENDERER: &str = "WEBKIT_DISABLE_DMABUF_RENDERER";
const WAYLAND_SESSION: &str = "wayland";

pub(crate) fn configure_webkit() -> io::Result<()> {
	let session_type = env::var_os("XDG_SESSION_TYPE");
	let explicit_sync = env::var_os(NVIDIA_DISABLE_EXPLICIT_SYNC);
	let dmabuf_renderer = env::var_os(WEBKIT_DISABLE_DMABUF_RENDERER);
	if !should_restart_with_wayland_defaults(
		session_type.as_deref(),
		explicit_sync.as_deref(),
		dmabuf_renderer.as_deref(),
	) {
		return Ok(());
	}

	// NVIDIA's EGL loader reads its configuration before main, so the process must restart.
	let executable = env::current_exe()?;
	let mut command = Command::new(executable);
	command.args(env::args_os().skip(1));
	if explicit_sync.is_none() {
		command.env(NVIDIA_DISABLE_EXPLICIT_SYNC, "1");
	}
	if dmabuf_renderer.is_none() {
		command.env(WEBKIT_DISABLE_DMABUF_RENDERER, "1");
	}

	Err(command.exec())
}

fn should_restart_with_wayland_defaults(
	session_type: Option<&OsStr>,
	explicit_sync: Option<&OsStr>,
	dmabuf_renderer: Option<&OsStr>,
) -> bool {
	session_type == Some(OsStr::new(WAYLAND_SESSION))
		&& (explicit_sync.is_none() || dmabuf_renderer.is_none())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn process_should_restart_for_unconfigured_wayland_session() {
		assert!(should_restart_with_wayland_defaults(
			Some(OsStr::new(WAYLAND_SESSION)),
			None,
			None,
		));
	}

	#[test]
	fn process_should_restart_when_explicit_sync_is_unconfigured() {
		assert!(should_restart_with_wayland_defaults(
			Some(OsStr::new(WAYLAND_SESSION)),
			Some(OsStr::new("0")),
			None,
		));
	}

	#[test]
	fn process_should_restart_when_dmabuf_renderer_is_unconfigured() {
		assert!(should_restart_with_wayland_defaults(
			Some(OsStr::new(WAYLAND_SESSION)),
			None,
			Some(OsStr::new("1")),
		));
	}

	#[test]
	fn process_should_preserve_configured_wayland_overrides() {
		assert!(!should_restart_with_wayland_defaults(
			Some(OsStr::new(WAYLAND_SESSION)),
			Some(OsStr::new("0")),
			Some(OsStr::new("1")),
		));
	}

	#[test]
	fn process_should_not_restart_outside_wayland() {
		assert!(!should_restart_with_wayland_defaults(
			Some(OsStr::new("x11")),
			None,
			None,
		));
	}
}
