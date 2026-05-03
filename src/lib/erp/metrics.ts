import type { ConsumptionLog, DayBookEntry, InventoryItem, InventoryLoss, Purchase, Sale } from "@/lib/db";

export type ErpMetricInputs = {
  inventory: InventoryItem[];
  sales: Sale[];
  purchases: Purchase[];
  dayBook: DayBookEntry[];
  consumption: ConsumptionLog[];
  losses: InventoryLoss[];
  monthKey: string; // YYYY-MM
  todayKey: string; // YYYY-MM-DD local via caller
};

function monthPrefix(iso: string, monthKey: string) {
  return new Date(iso).toISOString().slice(0, 7) === monthKey;
}

/** Calendar YYYY-MM-DD in the user's local timezone (not UTC). */
export function localDayKey(d: Date) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function dayKeyFromStoredInstant(iso: string) {
  return localDayKey(new Date(iso));
}

/** SUM(qty * avgCost) — falls back to costPrice when avgCost missing. */
export function inventoryStockValue(items: InventoryItem[]) {
  return items.reduce((acc, i) => {
    if (i.active === false) return acc;
    const unit = Number(i.avgCost ?? i.costPrice ?? 0);
    return acc + Number(i.quantity ?? 0) * unit;
  }, 0);
}

export function grossSalesMonth(sales: Sale[], monthKey: string) {
  return sales.filter((s) => monthPrefix(s.date, monthKey)).reduce((a, s) => a + Number(s.totalPrice ?? 0), 0);
}

export function purchasesMonth(purchases: Purchase[], monthKey: string) {
  return purchases.filter((p) => monthPrefix(p.date, monthKey)).reduce((a, p) => a + Number(p.totalCost ?? 0), 0);
}

export function feedExpenseMonth(consumption: ConsumptionLog[], monthKey: string) {
  return consumption.filter((c) => monthPrefix(c.date, monthKey)).reduce((a, c) => a + Number(c.cost ?? 0), 0);
}

export function feedExpenseToday(consumption: ConsumptionLog[], todayKey: string) {
  return consumption
    .filter((c) => dayKeyFromStoredInstant(c.date) === todayKey)
    .reduce((a, c) => a + Number(c.cost ?? 0), 0);
}

export function lossExpenseMonth(losses: InventoryLoss[], monthKey: string) {
  return losses.filter((l) => monthPrefix(l.date, monthKey)).reduce((a, l) => a + Number(l.estimatedCost ?? 0), 0);
}

export function operatingExpensesMonth(dayBook: DayBookEntry[], monthKey: string) {
  return dayBook
    .filter((e) => e.type === "Expense" && e.category !== "Purchase" && e.time.slice(0, 7) === monthKey)
    .reduce((a, e) => a + Number(e.amount ?? 0), 0);
}

export function netProfitErp(inp: ErpMetricInputs) {
  const gross = grossSalesMonth(inp.sales, inp.monthKey);
  const buy = purchasesMonth(inp.purchases, inp.monthKey);
  const feed = feedExpenseMonth(inp.consumption, inp.monthKey);
  const loss = lossExpenseMonth(inp.losses, inp.monthKey);
  const other = operatingExpensesMonth(inp.dayBook, inp.monthKey);
  return gross - buy - feed - loss - other;
}

export function expenseTrend7d(dayBook: DayBookEntry[]) {
  const now = new Date();
  const out: Array<{ x: string; y: number }> = [];
  for (let i = 6; i >= 0; i--) {
    const d = new Date(now);
    d.setDate(now.getDate() - i);
    out.push({ x: localDayKey(d), y: 0 });
  }
  const idx = new Map(out.map((r, i) => [r.x, i]));
  for (const e of dayBook) {
    if (e.type !== "Expense") continue;
    const k = dayKeyFromStoredInstant(e.time);
    const j = idx.get(k);
    if (typeof j === "number") out[j] = { ...out[j], y: out[j].y + Number(e.amount ?? 0) };
  }
  return out;
}

export function consumptionTrend7d(consumption: ConsumptionLog[]) {
  const now = new Date();
  const out: Array<{ x: string; y: number }> = [];
  for (let i = 6; i >= 0; i--) {
    const d = new Date(now);
    d.setDate(now.getDate() - i);
    out.push({ x: localDayKey(d), y: 0 });
  }
  const idx = new Map(out.map((r, i) => [r.x, i]));
  for (const c of consumption) {
    const k = dayKeyFromStoredInstant(c.date);
    const j = idx.get(k);
    if (typeof j === "number") out[j] = { ...out[j], y: out[j].y + Number(c.cost ?? 0) };
  }
  return out;
}

export function lossTrend7d(losses: InventoryLoss[]) {
  const now = new Date();
  const out: Array<{ x: string; y: number }> = [];
  for (let i = 6; i >= 0; i--) {
    const d = new Date(now);
    d.setDate(now.getDate() - i);
    out.push({ x: localDayKey(d), y: 0 });
  }
  const idx = new Map(out.map((r, i) => [r.x, i]));
  for (const l of losses) {
    const k = dayKeyFromStoredInstant(l.date);
    const j = idx.get(k);
    if (typeof j === "number") out[j] = { ...out[j], y: out[j].y + Number(l.estimatedCost ?? 0) };
  }
  return out;
}
