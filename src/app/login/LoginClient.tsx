"use client";

import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "next/navigation";
import { useLiveQuery } from "dexie-react-hooks";
import { db } from "@/lib/db";
import { loginWithPassword, getSession } from "@/lib/auth";
import { Eye, EyeOff, Lock, User } from "lucide-react";
import Link from "next/link";

export function LoginClient() {
  const search = useSearchParams();
  const users = useLiveQuery(() => db.users.toArray());

  const [form, setForm] = useState({ username: "", password: "" });
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [isWorking, setIsWorking] = useState(false);

  const hasUsers = useMemo(() => (users ? users.length > 0 : false), [users]);

  useEffect(() => {
    const s = getSession();
    if (s?.userId) window.location.replace("/");
  }, []);

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (isWorking) return;
    setError(null);
    try {
      setIsWorking(true);
      await loginWithPassword({ username: form.username, password: form.password });
      const next = search.get("next") ?? "/";
      window.location.replace(next);
    } catch (err: any) {
      setError(err?.message ?? "Login failed");
    } finally {
      setIsWorking(false);
    }
  };

  return (
    <div className="min-h-screen bg-background flex items-center justify-center p-4">
      <div className="w-full max-w-md rounded-3xl bg-white border border-slate-200 shadow-sm overflow-hidden">
        <div className="px-8 pt-8 pb-6 border-b border-slate-200">
          <div className="text-2xl font-semibold text-slate-900">Login</div>
          <div className="mt-1 text-sm text-slate-500">
            Sign in with your Patela Farm username and password. Accounts are stored on this device (IndexedDB); cloud
            sync is optional and not required to sign in.
          </div>
        </div>

        <form onSubmit={onSubmit} className="p-8 space-y-4">
          {!hasUsers ? (
            <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900 space-y-2">
              <p className="font-semibold">First-time setup</p>
              <p>No users exist yet on this device. Do this once:</p>
              <ol className="list-decimal list-inside space-y-1 pl-0.5">
                <li>
                  Open{" "}
                  <Link href="/users" className="font-semibold underline">
                    People → Users
                  </Link>{" "}
                  (allowed before login when there are no users).
                </li>
                <li>Create a role if needed (e.g. admin), then create a user with username and password (4–20 characters).</li>
                <li>Return here and sign in with that username and password.</li>
              </ol>
            </div>
          ) : null}

          <div>
            <label className="block text-sm font-medium mb-1">Username</label>
            <div className="relative">
              <User className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
              <input
                value={form.username}
                onChange={(e) => setForm((v) => ({ ...v, username: e.target.value }))}
                className="w-full pl-9 pr-3 py-2 border rounded-md bg-white"
                autoComplete="username"
                required
              />
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium mb-1">Password</label>
            <div className="relative">
              <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400 pointer-events-none" />
              <input
                type={showPassword ? "text" : "password"}
                value={form.password}
                onChange={(e) => setForm((v) => ({ ...v, password: e.target.value }))}
                className="w-full pl-9 pr-11 py-2 border rounded-md bg-white"
                autoComplete="current-password"
                required
              />
              <button
                type="button"
                onClick={() => setShowPassword((v) => !v)}
                className="absolute right-2 top-1/2 -translate-y-1/2 p-1.5 rounded-md text-slate-500 hover:bg-slate-100 hover:text-slate-700"
                aria-label={showPassword ? "Hide password" : "Show password"}
              >
                {showPassword ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
              </button>
            </div>
          </div>

          {error ? (
            <div className="rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-800">{error}</div>
          ) : null}

          <button
            type="submit"
            disabled={isWorking || !hasUsers}
            className={`w-full mt-2 px-4 py-2 rounded-md text-white font-semibold ${
              isWorking || !hasUsers ? "bg-slate-400" : "bg-primary hover:bg-primary/90"
            }`}
          >
            {isWorking ? "Signing in..." : "Login"}
          </button>
        </form>

        <div className="px-8 pb-8 -mt-2">
          <details className="rounded-xl border border-slate-200 bg-slate-50/80 text-sm text-slate-700 group">
            <summary className="cursor-pointer list-none px-4 py-3 font-semibold text-slate-800 flex items-center justify-between [&::-webkit-details-marker]:hidden">
              <span>What you need for login</span>
              <span className="text-slate-400 text-xs font-normal group-open:hidden">Show</span>
              <span className="text-slate-400 text-xs font-normal hidden group-open:inline">Hide</span>
            </summary>
            <div className="px-4 pb-4 pt-0 space-y-3 border-t border-slate-200/80">
              <div>
                <p className="font-medium text-slate-900">Already have a user?</p>
                <p className="mt-1">
                  Enter your username and password, then submit. Use the eye icon to check your password. After login
                  you can change your password under Settings → Password.
                </p>
              </div>
              <div>
                <p className="font-medium text-slate-900">If sign-in fails</p>
                <ul className="mt-1 list-disc list-inside space-y-1">
                  <li>
                    <span className="font-medium text-slate-800">User not found</span> — check spelling; usernames are
                    case-sensitive.
                  </li>
                  <li>
                    <span className="font-medium text-slate-800">Invalid password</span> — try again; reset from
                    Settings only while already signed in.
                  </li>
                  <li>
                    <span className="font-medium text-slate-800">No password set</span> — recreate the user from Users
                    or ask an admin.
                  </li>
                  <li>
                    If you are fully locked out on this device, you cannot open Settings. Use another device where you
                    are still signed in to add a user or change a password, or clear this site&apos;s stored data in your
                    browser (which removes local users and data) and run first-time setup again.
                  </li>
                </ul>
              </div>
            </div>
          </details>
        </div>
      </div>
    </div>
  );
}

