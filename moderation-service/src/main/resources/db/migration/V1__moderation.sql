create table reviews (
 submission_id uuid primary key, author_id uuid, version_id uuid, version_no integer,
 submission_status varchar(32) not null default 'DRAFT', submission_row_version bigint not null default 0,
 last_sequence bigint not null default 0, local_version bigint not null default 0,
 reviewer_id uuid, escalated boolean not null default false, pending_command_id uuid,
 created_at timestamptz not null default now(), updated_at timestamptz not null default now()
);
create table snapshots (
 version_id uuid primary key, submission_id uuid not null references reviews(submission_id),
 version_no integer not null check(version_no>0), payload jsonb not null,
 payload_schema_version integer not null check(payload_schema_version=1), content_sha256 char(64) not null,
 base_offer_row_version integer, author_id uuid not null, occurred_at timestamptz not null,
 unique(submission_id,version_no)
);
create table commands (
 id uuid primary key, submission_id uuid not null references reviews(submission_id),
 version_id uuid not null references snapshots(version_id), version_no integer not null,
 expected_submission_version bigint not null, actor_id uuid not null, action varchar(32) not null,
 target_status varchar(32) not null, state varchar(32) not null check(state in ('PENDING','CONFIRMED','REJECTED','SUPERSEDED')),
 reason varchar(2000), failure varchar(2000), correlation_id varchar(64) not null,
 created_at timestamptz not null default now(), resolved_at timestamptz
);
create unique index one_pending_command on commands(submission_id) where state='PENDING';
create table decisions (
 id uuid primary key, command_id uuid unique references commands(id), submission_id uuid not null references reviews(submission_id),
 version_id uuid not null references snapshots(version_id), version_no integer not null,
 actor_id uuid not null, action varchar(32) not null, reason varchar(2000), occurred_at timestamptz not null default now()
);
create table audit (
 id uuid primary key, submission_id uuid, actor_id uuid, action varchar(64) not null,
 detail jsonb not null, correlation_id varchar(64) not null, occurred_at timestamptz not null default now()
);
create table inbox (
 event_id uuid primary key, source varchar(128) not null, aggregate_id uuid not null,
 sequence bigint, fingerprint char(64) not null, received_at timestamptz not null default now()
);
create unique index inbox_sequence on inbox(aggregate_id,sequence) where source='submission.events.v1';
create table outbox (
 id uuid primary key, aggregate_id uuid not null, aggregate_sequence bigint not null,
 topic varchar(128) not null, payload jsonb not null, created_at timestamptz not null default now(), published_at timestamptz,
 unique(aggregate_id,aggregate_sequence)
);
create index outbox_unpublished on outbox(created_at) where published_at is null;
create index review_queue on reviews(submission_status,escalated,submission_id);
create table idempotency (
 actor_id uuid not null, scope varchar(256) not null, key varchar(128) not null,
 fingerprint char(64) not null, response jsonb not null, primary key(actor_id,scope,key)
);
create table reports (
 id uuid primary key, reporter_id uuid not null, submission_id uuid not null,
 category varchar(32) not null check(category in ('OUTDATED','CLOSED','DISCOUNT_STOPPED','MISLEADING','ABUSE','OTHER')),
 description varchar(2000) not null, state varchar(32) not null default 'OPEN' check(state in ('OPEN','TRIAGED','ESCALATED','RESOLVED')),
 row_version bigint not null default 0, assigned_to uuid, resolution varchar(2000),
 created_at timestamptz not null default now(), updated_at timestamptz not null default now()
);
create index reports_owner on reports(reporter_id,id);
create index reports_queue on reports(state,id);
create table request_windows (
 actor_id uuid not null, minute timestamptz not null, count integer not null, primary key(actor_id,minute)
);
create function reject_mutation() returns trigger language plpgsql as $$
begin raise exception 'append-only table % cannot be mutated', TG_TABLE_NAME using errcode='55000'; end $$;
create trigger immutable_snapshots before update or delete on snapshots for each row execute function reject_mutation();
create trigger immutable_decisions before update or delete on decisions for each row execute function reject_mutation();
create trigger immutable_audit before update or delete on audit for each row execute function reject_mutation();
create trigger immutable_inbox before update or delete on inbox for each row execute function reject_mutation();
create table consumer_failures (
 source_topic varchar(128) not null, source_partition integer not null, source_offset bigint not null,
 event_id uuid, aggregate_id uuid, error_code varchar(128) not null, occurred_at timestamptz not null default now(),
 primary key(source_topic,source_partition,source_offset)
);
alter table outbox add column next_attempt_at timestamptz not null default now();
alter table outbox add column failure_count integer not null default 0;
alter table outbox add column last_error varchar(128);
create trigger immutable_audit_truncate before truncate on audit execute function reject_mutation();
create trigger immutable_snapshots_truncate before truncate on snapshots execute function reject_mutation();
create trigger immutable_decisions_truncate before truncate on decisions execute function reject_mutation();
