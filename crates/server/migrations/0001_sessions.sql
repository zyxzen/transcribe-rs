create table sessions (
  id           uuid primary key,
  title        text,
  language     text not null,
  duration_ms  bigint not null default 0,
  created_at   timestamptz not null default now(),
  updated_at   timestamptz not null default now()
);

create table recordings (
  id           uuid primary key,
  session_id   uuid not null references sessions(id) on delete cascade,
  ordinal      int not null,
  offset_ms    bigint not null,
  sample_rate  int not null,
  duration_ms  bigint,
  started_at   timestamptz not null default now(),
  ended_at     timestamptz,
  unique (session_id, ordinal)
);

create table transcript_chunks (
  id           bigserial primary key,
  session_id   uuid not null references sessions(id) on delete cascade,
  recording_id uuid not null references recordings(id) on delete cascade,
  paragraph    int not null,
  text         text not null,
  start_ms     bigint not null,
  end_ms       bigint not null
);
create index transcript_chunks_session_idx on transcript_chunks (session_id, id);
