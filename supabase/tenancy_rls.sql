-- Run this in Supabase SQL Editor (after events.sql).
-- Adds per-farm tenancy and tight RLS policies.

create extension if not exists "pgcrypto";

create table if not exists public.farms (
  id uuid primary key default gen_random_uuid(),
  name text not null default 'Patela Farm',
  created_by uuid not null references auth.users(id),
  created_at timestamptz not null default now()
);

-- Ensures INSERT ... RETURNING and WITH CHECK see the same created_by as auth.uid()
-- even if the client omits created_by (see ensureFarm in app).
alter table public.farms alter column created_by set default auth.uid();

create table if not exists public.farm_members (
  farm_id uuid not null references public.farms(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  role text not null default 'owner',
  created_at timestamptz not null default now(),
  primary key (farm_id, user_id)
);

-- Add farm_id to events
alter table public.events
  add column if not exists farm_id uuid references public.farms(id) on delete cascade;

create index if not exists events_farm_created_idx on public.events(farm_id, created_at);

-- Helper: membership check
create or replace function public.is_farm_member(farm uuid)
returns boolean
language sql
stable
as $$
  select exists (
    select 1 from public.farm_members m
    where m.farm_id = farm and m.user_id = auth.uid()
  );
$$;

-- RLS
alter table public.farms enable row level security;
alter table public.farm_members enable row level security;
alter table public.events enable row level security;

-- Farms: owners/members can read; creator can insert
-- Members see their farm; creator can read a farm they created before the first farm_members row exists.
drop policy if exists farms_select on public.farms;
create policy farms_select on public.farms
as permissive
for select to authenticated
using (
  public.is_farm_member(id)
  or created_by = auth.uid()
);

drop policy if exists farms_insert on public.farms;
create policy farms_insert on public.farms
as permissive
for insert to authenticated
with check (auth.uid() is not null and created_by = auth.uid());

-- Members: members can read; owner can insert (simplified)
drop policy if exists farm_members_select on public.farm_members;
create policy farm_members_select on public.farm_members
as permissive
for select to authenticated
using (public.is_farm_member(farm_id));

-- Creator must be able to insert the first row before is_farm_member() is true.
drop policy if exists farm_members_insert on public.farm_members;
create policy farm_members_insert on public.farm_members
as permissive
for insert to authenticated
with check (
  user_id = auth.uid()
  and (
    public.is_farm_member(farm_id)
    or exists (
      select 1 from public.farms f
      where f.id = farm_id and f.created_by = auth.uid()
    )
  )
);

-- Events: only members can read/write within their farm
drop policy if exists events_select_farm on public.events;
create policy events_select_farm on public.events
as permissive
for select to authenticated
using (public.is_farm_member(farm_id));

drop policy if exists events_insert_farm on public.events;
create policy events_insert_farm on public.events
as permissive
for insert to authenticated
with check (public.is_farm_member(farm_id));

