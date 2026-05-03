"use client";

import { useLiveQuery } from "dexie-react-hooks";
import { db } from "@/lib/db";
import { useMemo } from "react";
import dynamic from "next/dynamic";
import { HeroSection, StatsCards, SalesChart, QuickActions, RecentActivity, InventorySnapshot, FinanceSnapshot } from "@/components/dashboard";
import { HandCoins, IndianRupee, TrendingUp } from "lucide-react";
import { useSyncExternalStore } from "react";
import {
  consumptionTrend7d,
  expenseTrend7d,
  feedExpenseToday,
  lossTrend7d,
  netProfitErp,
} from "@/lib/erp/metrics";

const Sparkline = dynamic(() => import("@/components/dashboard/_Sparkline").then((m) => m.Sparkline), { ssr: false });

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
  return true;
}

export default function Dashboard() {
  const inventory = useLiveQuery(() => db.inventory.toArray());
  const dayBook = useLiveQuery(() => db.dayBook.toArray());
  const purchases = useLiveQuery(() => db.purchases.toArray());
  const sales = useLiveQuery(() => db.sales.toArray());
  const consumption = useLiveQuery(() => db.consumptionLogs.toArray());
  const losses = useLiveQuery(() => db.inventoryLosses.toArray());
  const ledgerAccounts = useLiveQuery(() => db.ledgerAccounts.toArray());
  const ledgerEntries = useLiveQuery(() => db.ledgerEntries.toArray());

  const isOnline = useSyncExternalStore(subscribeOnlineStatus, getOnlineSnapshot, getOnlineServerSnapshot);

  const todayKey = new Date().toISOString().split("T")[0];
  const monthKey = `${new Date().getFullYear()}-${String(new Date().getMonth() + 1).padStart(2, "0")}`;

  const purchasesThisMonth = (purchases ?? [])
    .filter((p) => new Date(p.date).toISOString().slice(0, 7) === monthKey)
    .reduce((acc, p) => acc + p.totalCost, 0);

  const operatingExpensesThisMonth = (dayBook ?? [])
    .filter((e) => e.type === "Expense" && e.category !== "Purchase" && e.time.startsWith(monthKey))
    .reduce((acc, e) => acc + e.amount, 0);

  const salesRevenueThisMonth = (sales ?? [])
    .filter((s) => new Date(s.date).toISOString().slice(0, 7) === monthKey)
    .reduce((acc, s) => acc + s.totalPrice, 0);

  const netProfitErpMonth = useMemo(
    () =>
      netProfitErp({
        inventory: inventory ?? [],
        sales: sales ?? [],
        purchases: purchases ?? [],
        dayBook: dayBook ?? [],
        consumption: consumption ?? [],
        losses: losses ?? [],
        monthKey,
        todayKey,
      }),
    [inventory, sales, purchases, dayBook, consumption, losses, monthKey, todayKey]
  );

  const feedToday = useMemo(() => feedExpenseToday(consumption ?? [], todayKey), [consumption, todayKey]);

  const yesterdayKey = (() => {
    const d = new Date();
    d.setDate(d.getDate() - 1);
    return d.toISOString().split("T")[0];
  })();

  const todaySales = (sales ?? [])
    .filter((s) => new Date(s.date).toISOString().split("T")[0] === todayKey)
    .reduce((acc, s) => acc + s.totalPrice, 0);

  const yesterdaySales = (sales ?? [])
    .filter((s) => new Date(s.date).toISOString().split("T")[0] === yesterdayKey)
    .reduce((acc, s) => acc + s.totalPrice, 0);

  const salesDeltaPct =
    yesterdaySales === 0 ? (todaySales > 0 ? 100 : 0) : ((todaySales - yesterdaySales) / yesterdaySales) * 100;

  // placeholder until we track stock history snapshots

  const salesByDay = useMemo(() => {
    const now = new Date();
    const days = 90;
    const result: Array<{ dayKey: string; label: string; total: number }> = [];
    for (let i = days - 1; i >= 0; i--) {
      const d = new Date(now);
      d.setDate(now.getDate() - i);
      const dayKey = d.toISOString().split("T")[0];
      const label = d.toLocaleDateString(undefined, { month: "short", day: "2-digit" });
      result.push({ dayKey, label, total: 0 });
    }
    const index = new Map(result.map((r, idx) => [r.dayKey, idx]));
    for (const s of sales ?? []) {
      const key = new Date(s.date).toISOString().split("T")[0];
      const i = index.get(key);
      if (typeof i === "number") result[i] = { ...result[i], total: result[i].total + s.totalPrice };
    }
    return result;
  }, [sales]);

  const spark7 = salesByDay.slice(-7).map((d) => ({ x: d.dayKey, y: d.total }));
  const expenseSpark7 = useMemo(() => expenseTrend7d(dayBook ?? []), [dayBook]);
  const lossSpark7 = useMemo(() => lossTrend7d(losses ?? []), [losses]);
  const feedSpark7 = useMemo(() => consumptionTrend7d(consumption ?? []), [consumption]);

  const lowStockItemsTop5 = useMemo(() => {
    return (inventory ?? [])
      .filter((i) => {
        const th = i.reorderLevel ?? i.minStockThreshold ?? 0;
        return i.quantity <= th;
      })
      .slice()
      .sort((a, b) => a.quantity - b.quantity)
      .slice(0, 5);
  }, [inventory]);

  const activityItems = useMemo(() => {
    const tx = (dayBook ?? [])
      .slice()
      .sort((a, b) => new Date(b.time).getTime() - new Date(a.time).getTime())
      .slice(0, 25)
      .map((e) => {
        const isIncome = e.type === "Income";
        return {
          id: `tx-${e.id ?? e.time}`,
          title: e.description,
          subtitle: `${e.category} • Rs.${e.amount.toLocaleString()}`,
          time: new Date(e.time).toLocaleString(),
          chip: { label: isIncome ? "Receipt" : "Payment", tone: isIncome ? "success" : "danger" } as const,
          icon: (isIncome ? "income" : "expense") as "income" | "expense",
        };
      });

    const alerts = lowStockItemsTop5.slice(0, 2).map((i) => ({
      id: `alert-low-${i.id ?? i.name}`,
      title: `Low stock alert for ${i.name}`,
      subtitle: `${i.quantity} ${i.unit} remaining (reorder at ${i.reorderLevel ?? i.minStockThreshold ?? 0})`,
      time: "Now",
      chip: { label: "Low Stock", tone: "warn" } as const,
      icon: "alert" as const,
    }));

    return [...alerts, ...tx].slice(0, 30);
  }, [dayBook, lowStockItemsTop5]);

  const finance = useMemo(() => {
    const sums = new Map<number, { debit: number; credit: number }>();
    for (const e of ledgerEntries ?? []) {
      const cur = sums.get(e.accountId) ?? { debit: 0, credit: 0 };
      cur.debit += e.debit;
      cur.credit += e.credit;
      sums.set(e.accountId, cur);
    }
    let receivable = 0;
    let payable = 0;
    for (const a of ledgerAccounts ?? []) {
      if (typeof a.id !== "number") continue;
      const s = sums.get(a.id) ?? { debit: 0, credit: 0 };
      const bal = s.debit - s.credit;
      if (bal > 0) receivable += bal;
      if (bal < 0) payable += Math.abs(bal);
    }

    const expensesToday = (dayBook ?? [])
      .filter((e) => e.type === "Expense" && e.category !== "Purchase" && e.time.startsWith(todayKey))
      .reduce((acc, e) => acc + e.amount, 0);

    const cashInHand = (dayBook ?? []).reduce((acc, e) => (e.type === "Income" ? acc + e.amount : acc - e.amount), 0);

    return { receivable, payable, expensesToday, cashInHand };
  }, [dayBook, ledgerAccounts, ledgerEntries, todayKey]);

  const statCards = useMemo(() => {
    return [
      {
        id: "todaySales",
        title: "Today Sales",
        value: `Rs. ${todaySales.toLocaleString()}`,
        deltaPct: salesDeltaPct,
        icon: IndianRupee,
        iconBg: "bg-[#0871b3]/10",
        iconFg: "text-[#0871b3]",
        spark: spark7,
      },
      {
        id: "feedToday",
        title: "Feed expense (today)",
        value: `Rs. ${feedToday.toLocaleString()}`,
        deltaPct: 0,
        icon: TrendingUp,
        iconBg: "bg-amber-500/10",
        iconFg: "text-amber-800",
        spark: feedSpark7,
      },
      {
        id: "receivable",
        title: "Outstanding Receivable",
        value: `Rs. ${finance.receivable.toLocaleString()}`,
        deltaPct: 0,
        icon: HandCoins,
        iconBg: "bg-[#80a932]/12",
        iconFg: "text-[#80a932]",
      },
      {
        id: "payable",
        title: "Outstanding Payable",
        value: `Rs. ${finance.payable.toLocaleString()}`,
        deltaPct: 0,
        icon: HandCoins,
        iconBg: "bg-rose-500/10",
        iconFg: "text-rose-700",
      },
      {
        id: "netProfitErp",
        title: "Net Profit (month)",
        value: `Rs. ${netProfitErpMonth.toLocaleString()}`,
        deltaPct: 0,
        icon: TrendingUp,
        iconBg: "bg-[#80a932]/12",
        iconFg: "text-[#80a932]",
      },
    ];
  }, [
    todaySales,
    feedToday,
    finance.receivable,
    finance.payable,
    netProfitErpMonth,
    salesDeltaPct,
    spark7,
    feedSpark7,
  ]);

  return (
    <div className="space-y-4 sm:space-y-6">
      <HeroSection isOnline={isOnline} />
      <StatsCards cards={statCards} />

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 sm:gap-6">
        <div className="lg:col-span-2">
          <SalesChart salesByDay={salesByDay} />
        </div>
        <QuickActions />
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 sm:gap-6">
        <div className="rounded-2xl bg-white border border-[#e2e8f0] shadow-sm p-5">
          <div className="text-sm font-medium text-[#64748b]">Expense trend (7 days)</div>
          <div className="mt-3 h-10">
            <Sparkline data={expenseSpark7} />
          </div>
        </div>
        <div className="rounded-2xl bg-white border border-[#e2e8f0] shadow-sm p-5">
          <div className="text-sm font-medium text-[#64748b]">Feed consumption (7 days)</div>
          <div className="mt-3 h-10">
            <Sparkline data={feedSpark7} />
          </div>
        </div>
        <div className="rounded-2xl bg-white border border-[#e2e8f0] shadow-sm p-5">
          <div className="text-sm font-medium text-[#64748b]">Mortality / loss cost (7 days)</div>
          <div className="mt-3 h-10">
            <Sparkline data={lossSpark7} />
          </div>
        </div>
      </div>

      <FinanceSnapshot
        receivable={finance.receivable}
        payable={finance.payable}
        expensesToday={finance.expensesToday}
        cashInHand={finance.cashInHand}
      />

      <div className="rounded-2xl bg-white border border-[#e2e8f0] shadow-sm overflow-hidden">
        <div className="p-6 border-b border-[#e2e8f0]">
          <div className="text-sm font-medium text-[#64748b]">Monthly Report</div>
          <div className="mt-1 text-lg font-semibold text-[#0f172a]">{monthKey}</div>
        </div>
        <div className="p-6 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 text-sm">
          <div className="rounded-xl border border-[#e2e8f0] bg-[#f8fafc] p-4">
            <div className="text-xs font-semibold text-[#64748b]">Purchases</div>
            <div className="mt-2 text-lg font-semibold text-[#0f172a]">Rs. {purchasesThisMonth.toLocaleString()}</div>
          </div>
          <div className="rounded-xl border border-[#e2e8f0] bg-[#f8fafc] p-4">
            <div className="text-xs font-semibold text-[#64748b]">Expenses</div>
            <div className="mt-2 text-lg font-semibold text-[#0f172a]">Rs. {operatingExpensesThisMonth.toLocaleString()}</div>
          </div>
          <div className="rounded-xl border border-[#e2e8f0] bg-[#f8fafc] p-4">
            <div className="text-xs font-semibold text-[#64748b]">Sales</div>
            <div className="mt-2 text-lg font-semibold text-[#0f172a]">Rs. {salesRevenueThisMonth.toLocaleString()}</div>
          </div>
          <div className="rounded-xl border border-[#e2e8f0] bg-[#f8fafc] p-4">
            <div className="text-xs font-semibold text-[#64748b]">Net profit (ERP)</div>
            <div className="mt-2 text-lg font-semibold text-[#0f172a]">Rs. {netProfitErpMonth.toLocaleString()}</div>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-6">
        <RecentActivity items={activityItems} />
        <InventorySnapshot items={lowStockItemsTop5} />
      </div>
    </div>
  );
}
