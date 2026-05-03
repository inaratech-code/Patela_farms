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
          <div className="mt-1 text-sm text-slate-500">Sign in to continue.</div>
        </div>

        <form onSubmit={onSubmit} className="p-8 space-y-4">
          {!hasUsers ? (
            <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
              No users exist yet. Create the first user from{" "}
              <Link href="/users" className="font-semibold underline">
                Users
              </Link>
              .
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
      </div>
    </div>
  );
}

