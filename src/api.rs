use pgrx::prelude::*;

extension_sql_file!(
    "../sql/functions/api_track_capture.sql",
    name = "flashback_api_track_capture",
    requires = ["flashback_storage_schema_bootstrap"],
);
