# Patela Farms

Patela Farm is a modern farm inventory management system built for Dhangadhi, Nepal. It features stock tracking, sales, purchases, ledger, day book, user roles, alerts, and offline-first support. Designed with a clean SaaS dashboard UI, smooth UX, responsive layouts, and automation to simplify daily farm business operations.

## Getting started

Install dependencies and run the dev server:

```bash
npm install
npm run dev
```

Open `http://localhost:3000`.

## Supabase (optional, for multi-device sync)

1. Create a Supabase project and enable **Anonymous sign-ins** in Auth.
2. Run the SQL scripts in Supabase SQL editor:
   - `supabase/events.sql`
   - `supabase/tenancy_rls.sql`
3. Create `.env.local` (not committed) with:

```bash
NEXT_PUBLIC_SUPABASE_URL=...
NEXT_PUBLIC_SUPABASE_ANON_KEY=...
```

Then use **Settings → Sync** in the app.
