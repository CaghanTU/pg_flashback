use pgrx::pg_sys;
use pgrx::prelude::*;

mod api;
mod capture;
mod restore;
mod runtime_guard;
mod storage;

// Re-export so that the symbol is visible in the shared library.
// PostgreSQL calls _PG_output_plugin_init when loading our .so as a
// logical decoding output plugin (via pg_create_logical_replication_slot).
pub use capture::wal_decoder::_PG_output_plugin_init;

::pgrx::pg_module_magic!(name, version);

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    capture::ddl_hook::install_process_utility_hook();
    storage::worker::register_worker_and_guc();
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_flashback_delta_worker_main(arg: pg_sys::Datum) {
    storage::worker::pg_flashback_delta_worker_main(arg);
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    const COMMON_SETUP: &str = include_str!("../tests/sql/integration/_common_setup.sql");

    fn run_integration_sql(sql: &str) {
        Spi::run(COMMON_SETUP).expect("common integration setup failed");
        Spi::run(sql).expect("integration SQL scenario failed");
    }

    macro_rules! sql_test {
        ($name:ident, $path:literal) => {
            #[pg_test]
            fn $name() {
                run_integration_sql(include_str!($path));
            }
        };
    }

    sql_test!(
        it_dml_insert_restore,
        "../tests/sql/integration/dml_insert_restore.sql"
    );
    sql_test!(
        it_dml_update_restore,
        "../tests/sql/integration/dml_update_restore.sql"
    );
    sql_test!(
        it_dml_delete_restore,
        "../tests/sql/integration/dml_delete_restore.sql"
    );
    sql_test!(
        it_dml_ten_updates_restore,
        "../tests/sql/integration/dml_ten_updates_restore.sql"
    );
    sql_test!(
        it_dml_batch_insert_1000_restore,
        "../tests/sql/integration/dml_batch_insert_1000_restore.sql"
    );
    sql_test!(
        it_dml_update_all_rows_restore,
        "../tests/sql/integration/dml_update_all_rows_restore.sql"
    );

    sql_test!(
        it_ddl_truncate_restore,
        "../tests/sql/integration/ddl_truncate_restore.sql"
    );
    sql_test!(
        it_ddl_drop_restore,
        "../tests/sql/integration/ddl_drop_restore.sql"
    );
    sql_test!(
        it_ddl_truncate_then_insert_restore,
        "../tests/sql/integration/ddl_truncate_then_insert_restore.sql"
    );
    sql_test!(
        it_ddl_drop_recreate_same_name_restore,
        "../tests/sql/integration/ddl_drop_recreate_same_name_restore.sql"
    );

    sql_test!(
        it_schema_add_column_restore_old_time,
        "../tests/sql/integration/schema_add_column_restore_old_time.sql"
    );
    sql_test!(
        it_schema_drop_column_restore_old_time,
        "../tests/sql/integration/schema_drop_column_restore_old_time.sql"
    );
    sql_test!(
        it_schema_alter_type_restore,
        "../tests/sql/integration/schema_alter_type_restore.sql"
    );
    sql_test!(
        it_schema_multiple_alters_restore_oldest,
        "../tests/sql/integration/schema_multiple_alters_restore_oldest.sql"
    );

    sql_test!(
        it_multi_two_tables_restore,
        "../tests/sql/integration/multi_two_tables_restore.sql"
    );
    sql_test!(
        it_multi_fk_two_tables_restore,
        "../tests/sql/integration/multi_fk_two_tables_restore.sql"
    );
    sql_test!(
        it_multi_three_tables_restore,
        "../tests/sql/integration/multi_three_tables_restore.sql"
    );
    sql_test!(
        it_multi_drop_one_update_other_restore,
        "../tests/sql/integration/multi_drop_one_update_other_restore.sql"
    );

    sql_test!(
        it_edge_empty_table_restore,
        "../tests/sql/integration/edge_empty_table_restore.sql"
    );
    sql_test!(
        it_edge_null_values_restore,
        "../tests/sql/integration/edge_null_values_restore.sql"
    );
    sql_test!(
        it_edge_toast_long_text_restore,
        "../tests/sql/integration/edge_toast_long_text_restore.sql"
    );
    sql_test!(
        it_edge_same_tx_insert_update_delete_restore,
        "../tests/sql/integration/edge_same_tx_insert_update_delete_restore.sql"
    );
    sql_test!(
        it_edge_restore_without_tracking_error,
        "../tests/sql/integration/edge_restore_without_tracking_error.sql"
    );
    sql_test!(
        it_edge_partial_coverage_drop_restore,
        "../tests/sql/integration/edge_partial_coverage_drop_restore.sql"
    );

    sql_test!(
        it_checkpoint_after_restore,
        "../tests/sql/integration/checkpoint_after_restore.sql"
    );
    sql_test!(
        it_checkpoint_between_two_points_restore,
        "../tests/sql/integration/checkpoint_between_two_points_restore.sql"
    );
    sql_test!(
        it_checkpoint_no_checkpoint_long_chain_restore,
        "../tests/sql/integration/checkpoint_no_checkpoint_long_chain_restore.sql"
    );

    sql_test!(
        it_flashback_query_basic,
        "../tests/sql/integration/flashback_query_basic.sql"
    );

    sql_test!(
        it_partitioned_table_restore,
        "../tests/sql/integration/partitioned_table_restore.sql"
    );
    sql_test!(
        it_post_restore_checkpoint,
        "../tests/sql/integration/post_restore_checkpoint.sql"
    );
    sql_test!(
        it_acl_preservation_restore,
        "../tests/sql/integration/acl_preservation_restore.sql"
    );
    sql_test!(
        it_subtxn_rollback_restore,
        "../tests/sql/integration/subtxn_rollback_restore.sql"
    );
    sql_test!(
        it_pgdump_compat,
        "../tests/sql/integration/pgdump_compat.sql"
    );
    sql_test!(
        it_rbac_enforcement,
        "../tests/sql/integration/rbac_enforcement.sql"
    );
    sql_test!(
        it_pitr_time_filtering,
        "../tests/sql/integration/pitr_time_filtering.sql"
    );
    sql_test!(
        it_concurrent_restore_stress,
        "../tests/sql/integration/concurrent_restore_stress.sql"
    );

    sql_test!(
        it_diff_only_update_restore,
        "../tests/sql/integration/diff_only_update_restore.sql"
    );
    sql_test!(
        it_batch_replay_mixed_ops,
        "../tests/sql/integration/batch_replay_mixed_ops.sql"
    );
    sql_test!(
        it_noop_update_skip,
        "../tests/sql/integration/noop_update_skip.sql"
    );
    sql_test!(
        it_native_partition_restore,
        "../tests/sql/integration/native_partition_restore.sql"
    );
    sql_test!(
        it_restore_parallel_basic,
        "../tests/sql/integration/restore_parallel_basic.sql"
    );
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![
            "wal_level=logical",
            "max_replication_slots=10",
            "shared_preload_libraries='pg_flashback'",
        ]
    }
}
