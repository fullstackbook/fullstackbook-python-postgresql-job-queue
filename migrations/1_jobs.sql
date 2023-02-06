create table jobs (
  job_id bigserial primary key,
  job_data text,
  job_status text,
  attempts int default 0,
  last_error text,
  run_at timestamp default now(),
  created_at timestamp default now(),
  updated_at timestamp default now()
)