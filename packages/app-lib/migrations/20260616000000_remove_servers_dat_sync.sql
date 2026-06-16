UPDATE settings
SET sync_files = (
	SELECT json_group_array(value)
	FROM json_each(settings.sync_files)
	WHERE value <> 'servers.dat'
)
WHERE EXISTS (
	SELECT 1 FROM json_each(settings.sync_files) WHERE value = 'servers.dat'
);
