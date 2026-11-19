create table media_asset (
 id uuid primary key, owner_id uuid not null, filename varchar(200) not null,
 declared_mime varchar(32) not null, raw_bytes integer not null check(raw_bytes between 1 and 10485760),
 raw_sha256 char(64) not null, original_key varchar(200) not null unique,
 status varchar(16) not null check(status in ('QUARANTINED','PROCESSING','CLEAN','REJECTED')),
 revision bigint not null default 0 check(revision >= 0), clean_key varchar(200) unique,
 width integer, height integer, rejection_code varchar(64), correlation_id varchar(64) not null,
 claim_id uuid, lease_until timestamptz, next_attempt_at timestamptz not null default now(),
 attempts integer not null default 0, created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
 check ((status = 'CLEAN') = (clean_key is not null)),
 check (status <> 'CLEAN' or (width > 0 and height > 0))
);
create index media_asset_jobs on media_asset(next_attempt_at, id) where status in ('QUARANTINED','PROCESSING');
create index media_asset_owner on media_asset(owner_id, id);
create table upload_idempotency (
 owner_id uuid not null, key varchar(128) not null, fingerprint char(64) not null,
 asset_id uuid not null references media_asset(id), created_at timestamptz not null default now(),
 primary key(owner_id,key)
);
create table media_audit (
 id uuid primary key, asset_id uuid not null references media_asset(id), actor_id uuid,
 from_status varchar(16), to_status varchar(16) not null, revision bigint not null,
 reason varchar(64), occurred_at timestamptz not null default now(), correlation_id varchar(64) not null,
 unique(asset_id, revision)
);
create function forbid_media_audit_mutation() returns trigger language plpgsql as $$
begin raise exception 'media audit is immutable'; end $$;
create trigger immutable_media_audit before update or delete on media_audit
 for each row execute function forbid_media_audit_mutation();
create table media_outbox (
 event_id uuid primary key, asset_id uuid not null references media_asset(id), revision bigint not null,
 payload jsonb not null, created_at timestamptz not null default now(), published_at timestamptz,
 unique(asset_id,revision)
);
create index media_outbox_pending on media_outbox(created_at,event_id) where published_at is null;
create table media_rate_window (
 actor_id uuid not null, window_start timestamptz not null, kind varchar(16) not null,
 count integer not null, primary key(actor_id,window_start,kind)
);
