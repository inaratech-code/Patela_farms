-- Legacy patch (partial). Prefer running fix_farms_rls_v2.sql for the full farms + members + grants fix.
--
-- Fix "Sync failed: new row violates row-level security policy for table farms"
-- Typical causes:
-- 1) INSERT...RETURNING fails SELECT RLS (creator not a member yet).
-- 2) farm_members insert required membership before the first row.

-- 1) Creator can read their own farm rows
drop policy if exists farms_select on public.farms;
create policy farms_select on public.farms
for select to authenticated
using (
  public.is_farm_member(id)
  or created_by = auth.uid()
);

-- 2) First membership row for a new farm
drop policy if exists farm_members_insert on public.farm_members;
create policy farm_members_insert on public.farm_members
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
