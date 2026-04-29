"use client";

import { createContext, useContext, useEffect, useMemo, useState } from "react";
import { usePathname } from "next/navigation";
import { useLiveQuery } from "dexie-react-hooks";
import { motion, AnimatePresence } from "framer-motion";
import { cn } from "@/lib/utils";
import { db } from "@/lib/db";
import { SidebarGroup } from "@/components/sidebar/SidebarGroup";
import { SidebarItem } from "@/components/sidebar/SidebarItem";
import { sidebarGroups, sidebarTopLevel, type SidebarGroupId } from "@/components/sidebar/sidebarConfig";
import { LogOut, Menu, X } from "lucide-react";
import { clearSession, getSession } from "@/lib/auth";

type SidebarContextValue = {
  openMobile: () => void;
  closeMobile: () => void;
  toggleMobile: () => void;
};

const SidebarContext = createContext<SidebarContextValue | null>(null);

export function useSidebar() {
  const ctx = useContext(SidebarContext);
  if (!ctx) throw new Error("useSidebar must be used within <SidebarProvider>");
  return ctx;
}

function safeJsonParse<T>(value: string | null, fallback: T): T {
  if (!value) return fallback;
  try {
    return JSON.parse(value) as T;
  } catch {
    return fallback;
  }
}

function computeAlertBadge(params: {
  inventory: Array<{ quantity: number; minStockThreshold: number; expiryDate?: string }>;
  ledgerAccounts: Array<{ id?: number }>;
  ledgerEntries: Array<{ accountId: number; debit: number; credit: number }>;
}) {
  const lowStock = params.inventory.filter((i) => i.quantity <= i.minStockThreshold).length;

  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const expiryWindowDays = 7;
  const expiry = params.inventory
    .filter((i) => !!i.expiryDate)
    .map((i) => {
      const exp = new Date(`${i.expiryDate}T00:00:00`);
      const daysLeft = Math.floor((exp.getTime() - today.getTime()) / (1000 * 60 * 60 * 24));
      return daysLeft;
    })
    .filter((d) => d <= expiryWindowDays).length;

  const sums = new Map<number, { debit: number; credit: number }>();
  for (const e of params.ledgerEntries) {
    const cur = sums.get(e.accountId) ?? { debit: 0, credit: 0 };
    cur.debit += e.debit;
    cur.credit += e.credit;
    sums.set(e.accountId, cur);
  }
  const pending = params.ledgerAccounts.filter((a) => {
    if (typeof a.id !== "number") return false;
    const s = sums.get(a.id) ?? { debit: 0, credit: 0 };
    return s.debit - s.credit !== 0;
  }).length;

  return lowStock + expiry + pending;
}

export function SidebarProvider(props: { children: React.ReactNode }) {
  const [mobileOpen, setMobileOpen] = useState(false);

  const value = useMemo<SidebarContextValue>(
    () => ({
      openMobile: () => setMobileOpen(true),
      closeMobile: () => setMobileOpen(false),
      toggleMobile: () => setMobileOpen((v) => !v),
    }),
    []
  );

  return (
    <SidebarContext.Provider value={value}>
      {props.children}
      <MobileSidebarDrawer open={mobileOpen} onClose={() => setMobileOpen(false)} />
    </SidebarContext.Provider>
  );
}

function MobileSidebarDrawer(props: { open: boolean; onClose: () => void }) {
  return (
    <AnimatePresence>
      {props.open ? (
        <motion.div className="fixed inset-0 z-50 md:hidden" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
          <div className="absolute inset-0 bg-slate-900/50" onClick={props.onClose} />
          <motion.div
            initial={{ x: -320 }}
            animate={{ x: 0 }}
            exit={{ x: -320 }}
            transition={{ duration: 0.2, ease: "easeOut" }}
            className="absolute inset-y-0 left-0 w-64 bg-white border-r border-slate-200"
          >
            <SidebarContent variant="mobile" onNavigate={props.onClose} />
          </motion.div>
        </motion.div>
      ) : null}
    </AnimatePresence>
  );
}

export function SidebarDesktop() {
  return (
    <aside className="hidden md:flex md:flex-col fixed inset-y-0 left-0 z-40 w-64 bg-white border-r border-slate-200">
      <SidebarContent variant="desktop" />
    </aside>
  );
}

function SidebarContent(props: { variant: "desktop" | "mobile"; onNavigate?: () => void }) {
  const pathname = usePathname();

  const inventory = useLiveQuery(() => db.inventory.toArray()) || [];
  const ledgerAccounts = useLiveQuery(() => db.ledgerAccounts.toArray()) || [];
  const ledgerEntries = useLiveQuery(() => db.ledgerEntries.toArray()) || [];
  const session = useMemo(() => getSession(), [pathname]);

  const alertBadge = useMemo(
    () => computeAlertBadge({ inventory, ledgerAccounts, ledgerEntries }),
    [inventory, ledgerAccounts, ledgerEntries]
  );

  const storageKey = "pf.sidebar.v1";
  const defaultOpenGroups: Record<SidebarGroupId, boolean> = {
    inventory: false,
    transactions: false,
    accounts: false,
    people: false,
  };

  const [isCollapsed, setIsCollapsed] = useState(props.variant === "mobile");
  const [openGroups, setOpenGroups] = useState<Record<SidebarGroupId, boolean>>(defaultOpenGroups);

  useEffect(() => {
    const existing = safeJsonParse<{ collapsed?: boolean; groups?: Partial<Record<SidebarGroupId, boolean>> }>(
      localStorage.getItem(storageKey),
      {}
    );

    // Desktop default expanded; Mobile default expanded (labels visible).
    setIsCollapsed(props.variant === "mobile" ? false : Boolean(existing.collapsed));
    // Always start dropdown groups collapsed. Users explicitly open them via click.
    setOpenGroups(defaultOpenGroups);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [props.variant]);

  useEffect(() => {
    if (props.variant === "mobile") return; // don't persist temporary mobile drawer state
    // Persist only collapsed/expanded sidebar state, not dropdown expansions.
    localStorage.setItem(storageKey, JSON.stringify({ collapsed: isCollapsed }));
  }, [isCollapsed, openGroups, props.variant]);

  const activeHref = pathname;

  const toggleGroup = (id: SidebarGroupId) => {
    setOpenGroups((prev) => ({ ...prev, [id]: !prev[id] }));
  };

  const onLogout = () => {
    clearSession();
    props.onNavigate?.(); // close mobile drawer if open
    window.location.replace("/login");
  };

  return (
    <div className={cn("h-full flex flex-col", isCollapsed ? "w-20" : "w-64")}>
      <div className="h-20 border-b border-slate-200 flex items-center justify-between px-4">
        <div className={cn("flex items-center gap-3", isCollapsed ? "justify-center w-full" : "")}>
          <img src="/logo.png" alt="Patela Farm Logo" className="h-10 w-auto object-contain" />
          {!isCollapsed ? <span className="text-lg font-bold text-primary">Patela Farm</span> : null}
        </div>

        {props.variant === "desktop" ? (
          <button
            type="button"
            onClick={() => setIsCollapsed((v) => !v)}
            className="p-2 rounded-md hover:bg-slate-50 text-slate-600"
            aria-label={isCollapsed ? "Expand sidebar" : "Collapse sidebar"}
            title={isCollapsed ? "Expand" : "Collapse"}
          >
            <Menu className="w-5 h-5" />
          </button>
        ) : (
          <button type="button" onClick={props.onNavigate} className="p-2 rounded-md hover:bg-slate-50 text-slate-600" aria-label="Close">
            <X className="w-5 h-5" />
          </button>
        )}
      </div>

      <nav className={cn("p-3 space-y-2 flex-1 overflow-y-auto")}>
        {sidebarTopLevel.slice(0, 2).map((item) => (
          <SidebarItem
            key={item.id}
            href={item.href}
            label={item.label}
            icon={item.icon}
            isActive={activeHref === item.href}
            isCollapsed={isCollapsed}
            badgeCount={item.badge === "alerts" ? alertBadge : undefined}
            onNavigate={props.onNavigate}
          />
        ))}

        <div className="h-px bg-slate-100 my-2" />

        {sidebarGroups.map((g) => (
          <SidebarGroup
            key={g.id}
            group={g}
            isOpen={openGroups[g.id]}
            onToggle={() => toggleGroup(g.id)}
            activeHref={activeHref}
            isCollapsed={isCollapsed}
            onNavigate={props.onNavigate}
          />
        ))}

        <div className="h-px bg-slate-100 my-2" />

        {sidebarTopLevel.slice(2).map((item) => (
          <SidebarItem
            key={item.id}
            href={item.href}
            label={item.label}
            icon={item.icon}
            isActive={activeHref === item.href}
            isCollapsed={isCollapsed}
            badgeCount={item.badge === "alerts" ? alertBadge : undefined}
            onNavigate={props.onNavigate}
          />
        ))}
      </nav>

      <div className={cn("p-4 border-t border-slate-200 text-xs text-slate-500", isCollapsed ? "text-center" : "")}>
        <button
          type="button"
          onClick={onLogout}
          className={cn(
            "w-full mb-3 inline-flex items-center gap-2 rounded-lg border border-slate-200 hover:bg-slate-50 text-slate-700 font-semibold",
            isCollapsed ? "justify-center px-2 py-2" : "justify-start px-3 py-2"
          )}
          title="Logout"
        >
          <LogOut className="w-4 h-4 text-slate-500" />
          {!isCollapsed ? <span className="text-sm">{session?.username ? `Logout (${session.username})` : "Logout"}</span> : <span className="sr-only">Logout</span>}
        </button>

        {!isCollapsed ? (
          <>
            Built by <span className="font-semibold text-slate-700">Inara Tech</span>
          </>
        ) : (
          <span title="Built by Inara Tech">Inara</span>
        )}
      </div>
    </div>
  );
}

