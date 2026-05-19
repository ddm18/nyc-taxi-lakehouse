create table if not exists pipeline_run_audit (
    audit_id bigserial primary key,
    environment_name text not null,
    service_name text not null,
    dataset_month text not null,
    artifact_digest text not null,
    transformation_version text not null,
    dag_id text not null,
    run_id text not null,
    verification_status text not null,
    report_s3_uri text not null,
    metadata_json jsonb not null default '{}'::jsonb,
    recorded_at timestamptz not null default now()
);

create index if not exists pipeline_run_audit_env_month_idx
    on pipeline_run_audit (environment_name, dataset_month);

