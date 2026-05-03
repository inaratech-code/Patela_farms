"use client";

import { Bell, Search, User, Menu } from "lucide-react";
import { useEffect, useMemo, useState, useSyncExternalStore } from "react";
import { cn } from "@/lib/utils";
import Link from "next/link";
import { useSidebar } from "@/components/sidebar/Sidebar";
import { getSession } from "@/lib/auth";
import { usePathname } from "next/navigation";

function subscribeOnlineStatus(onStoreChange: () => void) {
  window.addEventListener("online", onStoreChange);
  window.addEventListener("offline", onStoreChange);
  return () => {
    window.removeEventListener("online", onStoreChange);
    window.removeEventListener("offline", onStoreChange);
  };
}

function getOnlineSnapshot() {
  return navigator.onLine;
}

function getOnlineServerSnapshot() {
  // Server doesn't know; choose a stable default to avoid hydration mismatch.
  return true;
}

export function TopHeader() {
  const isOnline = useSyncExternalStore(subscribeOnlineStatus, getOnlineSnapshot, getOnlineServerSnapshot);
  const sidebar = useSidebar();
  const pathname = usePathname();
  const [menuOpen, setMenuOpen] = useState(false);
  const [sessionTick, setSessionTick] = useState(0);

  const session = useMemo(() => getSession(), [pathname, sessionTick]);

  useEffect(() => {
    setMenuOpen(false);
  }, [pathname]);

  useEffect(() => {
    // Update header when session changes (login/logout in another component).
    const onStorage = (e: StorageEvent) => {
      if (e.key === "pf.session.v1") setSessionTick((v) => v + 1);
    };
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, []);

  return (
    <header className="sticky top-0 z-30 flex items-center justify-between min-h-14 h-14 sm:h-16 px-3 sm:px-5 lg:px-8 pt-[env(safe-area-inset-top,0px)] bg-white/90 backdrop-blur border-b border-[#e2e8f0]">
      <button
        type="button"
        className="lg:hidden shrink-0 p-2.5 rounded-md hover:bg-slate-50 text-slate-600 [touch-action:manipulation]"
        onClick={sidebar.openMobile}
        aria-label="Open menu"
      >
        <Menu className="w-5 h-5" />
      </button>

      <div className="flex flex-1">
        <form className="flex w-full min-w-0 lg:ml-0" action="#" method="GET">
          <label htmlFor="search-field" className="sr-only">Search</label>
          <div className="relative w-full text-slate-400 focus-within:text-slate-600">
            <div className="absolute inset-y-0 left-0 flex items-center pointer-events-none">
              <Search className="w-5 h-5" aria-hidden="true" />
            </div>
            <input
              id="search-field"
              className="block w-full min-h-10 py-2 pl-8 pr-2 text-sm text-slate-900 placeholder-slate-500 border-transparent focus:outline-none focus:ring-0 focus:border-transparent sm:text-sm bg-transparent"
              placeholder="Search…"
              type="search"
              name="search"
            />
          </div>
        </form>
      </div>

      <div className="flex items-center shrink-0 ml-2 sm:ml-4 space-x-2 sm:space-x-4 lg:ml-6">
        <div className={cn(
          "flex items-center px-2.5 py-1 text-xs font-medium rounded-full",
          isOnline ? "bg-alert-green/10 text-alert-green" : "bg-alert-yellow/10 text-alert-yellow"
        )}>
          <span className={cn(
            "w-2 h-2 mr-1.5 rounded-full",
            isOnline ? "bg-alert-green" : "bg-alert-yellow"
          )} />
          {isOnline ? "Synced" : "Offline"}
        </div>

        <Link href="/alerts" className="p-1 text-slate-400 bg-white rounded-full hover:text-slate-500 focus:outline-none">
          <span className="sr-only">View notifications</span>
          <Bell className="w-6 h-6" aria-hidden="true" />
        </Link>

        <div className="relative">
          <button
            type="button"
            onClick={() => setMenuOpen((v) => !v)}
            className="flex items-center gap-2 max-w-xs text-sm bg-white rounded-full focus:outline-none"
          >
            <span className="sr-only">Open user menu</span>
            <div className="flex items-center justify-center w-8 h-8 rounded-full bg-slate-100">
              <User className="w-5 h-5 text-slate-500" />
            </div>
            <span className="hidden sm:block text-sm font-semibold text-slate-700 max-w-36 truncate">
              {session?.username ?? "User"}
            </span>
          </button>

          {menuOpen ? (
            <>
              <button
                type="button"
                className="fixed inset-0 z-20 cursor-default"
                aria-label="Close menu"
                onClick={() => setMenuOpen(false)}
              />
              <div className="absolute right-0 mt-2 z-30 w-48 rounded-xl border border-slate-200 bg-white shadow-lg overflow-hidden">
                <div className="px-3 py-2 border-b border-slate-100">
                  <div className="text-xs text-slate-500">Signed in as</div>
                  <div className="text-sm font-semibold text-slate-900 truncate">{session?.username ?? "User"}</div>
                </div>
              </div>
            </>
          ) : null}
        </div>
      </div>
    </header>
  );
}
