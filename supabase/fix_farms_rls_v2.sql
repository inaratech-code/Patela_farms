-- Fix: "Sync failed: new row violates row-level security policy for table \"farms\""
--
-- Run this in the Supabase SQL Editor (Dashboard → SQL → New query) once per project.
--
-- Prerequisites:
--   1) Auth → Providers → Anonymous: enable (the app uses signInAnonymously for sync).
--   2) Tables public.farms and public.farm_members already exist (from tenancy_rls.sql).
--
-- What this does:
--   - Sets created_by default to auth.uid() so INSERT ... RETURNING stays consistent with RLS.
--   - Recreates farms SELECT/INSERT policies as permissive with an explicit auth.uid() check on insert.

alter table public.farms alter column created_by set default auth.uid();

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

-- First farm_members row: creator may not be a "member" yet; allow insert if they own the farm.
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

grant select, insert, update, delete on table public.farms to authenticated;
grant select, insert, update, delete on table public.farm_members to authenticated;
