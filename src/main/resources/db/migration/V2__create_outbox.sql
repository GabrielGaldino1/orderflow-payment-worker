create table outbox_messages (
    message_id uuid primary key,
    aggregate_id uuid not null,
    event_type varchar(128) not null,
    event_version integer not null,
    destination varchar(160) not null,
    message_key varchar(255) not null,
    payload text not null,
    created_at timestamp with time zone not null,
    attempt_count integer not null default 0,
    claimed_by varchar(128) null,
    claimed_at timestamp with time zone null,
    last_attempt_at timestamp with time zone null,
    last_error varchar(255) null,
    published_at timestamp with time zone null
);

create index idx_outbox_unpublished on outbox_messages (published_at, created_at);
