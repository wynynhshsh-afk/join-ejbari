-- یک‌بار در Supabase (SQL Editor) اجرا کنید

create table if not exists join_attributions (
    item_id bigint not null,
    user_id bigint not null,
    warned_at timestamptz not null default now(),
    joined_at timestamptz,
    primary key (item_id, user_id)
);
create index if not exists join_attr_item_joined on join_attributions (item_id, joined_at);

create table if not exists join_notices (
    id bigint generated always as identity primary key,
    chat_id text not null,
    message_id bigint not null,
    delete_at timestamptz not null
);

alter table channels add column if not exists count_from timestamptz;
alter table channels add column if not exists report_sent boolean not null default false;

-- آیتم‌های قدیمیِ تمام‌شده گزارش تصویری نگیرند:
update channels set report_sent = true where quota_done or (expires_at is not null and expires_at <= now());
