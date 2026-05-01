"use client";

import { useLiveQuery } from "dexie-react-hooks";
import { db } from "@/lib/db";
import { useMemo, useState } from "react";

function formatISODate(d: Date) {
  return d.toISOString().slice(0, 10);
}

function formatSheetDate(isoDate: string) {
  const [y, m, d] = isoDate.split("-");
  if (!y || !m || !d) return isoDate;
  return `${y} / ${m} / ${d}`;
}

function pad2(n: number) {
  return String(n).padStart(2, "0");
}

function localDayKeyFromIsoTime(isoTime: string) {
  const d = new Date(isoTime);
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
}

function isBeforeDay(isoTime: string, isoDate: string) {
  // Compare by local day key (prevents UTC vs local-day mismatches).
  const day = localDayKeyFromIsoTime(isoTime);
  return day < isoDate;
}

export default function DayBookPage() {
  const allEntries = useLiveQuery(() => db.dayBook.toArray()) || [];
  const [selectedDate, setSelectedDate] = useState(() => formatISODate(new Date()));

  const dayEntries = useMemo(() => {
    const list = allEntries
      .filter((e) => localDayKeyFromIsoTime(e.time) === selectedDate)
      .slice()
      .sort((a, b) => new Date(a.time).getTime() - new Date(b.time).getTime());
    return {
      receipts: list.filter((e) => e.type === "Income"),
      payments: list.filter((e) => e.type === "Expense"),
    };
  }, [allEntries, selectedDate]);

  const openingBalance = useMemo(() => {
    // Simple running cash balance: sum(income) - sum(expense) before selected day.
    // (We don't yet track per-account cashbook balance in DB.)
    return allEntries.reduce((acc, e) => {
      if (!isBeforeDay(e.time, selectedDate)) return acc;
      return acc + (e.type === "Income" ? e.amount : -e.amount);
    }, 0);
  }, [allEntries, selectedDate]);

  const totals = useMemo(() => {
    const receiptsTotal = dayEntries.receipts.reduce((acc, e) => acc + e.amount, 0);
    const paymentsTotal = dayEntries.payments.reduce((acc, e) => acc + e.amount, 0);
    return { receiptsTotal, paymentsTotal };
  }, [dayEntries.payments, dayEntries.receipts]);

  const closingBalance = openingBalance + totals.receiptsTotal - totals.paymentsTotal;

  const rowCount = Math.max(dayEntries.receipts.length + 1, dayEntries.payments.length + 2, 8);

  const receiptRef = (idx: number) => {
    const e = dayEntries.receipts[idx];
    if (!e) return "";
    const prefix = e.category === "Sale" ? "S" : "R";
    return `${prefix}-${String(idx + 1).padStart(2, "0")}`;
  };

  const paymentRef = (idx: number) => {
    const e = dayEntries.payments[idx];
    if (!e) return "";
    const prefix = e.category === "Purchase" ? "P" : "E";
    return `${prefix}-${String(idx + 1).padStart(2, "0")}`;
  };

  return (
    <div className="space-y-4">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold text-slate-900">Day Book</h1>
          <div className="text-sm text-slate-500">Daily Cash Summary sheet format</div>
        </div>
        <div className="flex items-center gap-3">
          <label className="text-sm font-medium text-slate-600">Date</label>
          <input
            type="date"
            value={selectedDate}
            onChange={(e) => setSelectedDate(e.target.value)}
            className="px-3 py-2 border rounded-md bg-white"
          />
        </div>
      </div>

      <div className="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
        <div className="px-4 sm:px-6 py-4 border-b border-slate-200">
          <div className="text-center">
            <div className="text-xl font-bold tracking-wide">Patela Farm</div>
            <div className="mt-1 text-sm font-semibold">
              Daily Cash Summary For&nbsp; {formatSheetDate(selectedDate)}
            </div>
          </div>
        </div>

        <div className="w-full overflow-x-auto">
          <table className="min-w-[980px] w-full border-collapse">
            <thead>
              <tr>
                <th colSpan={3} className="border border-slate-700 bg-emerald-50 px-3 py-2 text-center text-sm font-semibold">
                  Cash Debit/Inward
                </th>
                <th colSpan={3} className="border border-slate-700 bg-rose-50 px-3 py-2 text-center text-sm font-semibold">
                  Cash Credit/Outward
                </th>
              </tr>
              <tr>
                <th className="border border-slate-700 bg-emerald-50 px-3 py-2 text-left text-xs font-semibold uppercase">Reference</th>
                <th className="border border-slate-700 bg-emerald-50 px-3 py-2 text-left text-xs font-semibold uppercase">Particulars</th>
                <th className="border border-slate-700 bg-emerald-50 px-3 py-2 text-right text-xs font-semibold uppercase">Amount Rs.</th>
                <th className="border border-slate-700 bg-rose-50 px-3 py-2 text-left text-xs font-semibold uppercase">Reference</th>
                <th className="border border-slate-700 bg-rose-50 px-3 py-2 text-left text-xs font-semibold uppercase">Particulars</th>
                <th className="border border-slate-700 bg-rose-50 px-3 py-2 text-right text-xs font-semibold uppercase">Amount Rs.</th>
              </tr>
            </thead>
            <tbody>
              {Array.from({ length: rowCount }).map((_, idx) => {
                const leftIsOpening = idx === 0;
                const receiptIdx = idx - 1;
                const receipt = receiptIdx >= 0 ? dayEntries.receipts[receiptIdx] : undefined;

                const payment = dayEntries.payments[idx];
                const rightIsClosing = idx === rowCount - 1;

                return (
                  <tr key={idx} className="h-12">
                    {/* Left side */}
                    <td className="border border-slate-700 px-3 py-2 text-sm">
                      {leftIsOpening ? "" : receipt ? receiptRef(receiptIdx) : ""}
                    </td>
                    <td className="border border-slate-700 px-3 py-2 text-sm">
                      {leftIsOpening ? <span className="font-semibold">Opening Balance B/F</span> : receipt?.description ?? ""}
                    </td>
                    <td className="border border-slate-700 px-3 py-2 text-sm text-right font-semibold">
                      {leftIsOpening
                        ? openingBalance ? openingBalance.toLocaleString() : ""
                        : receipt
                          ? receipt.amount.toLocaleString()
                          : ""}
                    </td>

                    {/* Right side */}
                    <td className="border border-slate-700 px-3 py-2 text-sm">
                      {rightIsClosing ? "" : payment ? paymentRef(idx) : ""}
                    </td>
                    <td className="border border-slate-700 px-3 py-2 text-sm">
                      {rightIsClosing ? <span className="font-semibold">Closing Balance C/F</span> : payment?.description ?? ""}
                    </td>
                    <td className="border border-slate-700 px-3 py-2 text-sm text-right font-semibold">
                      {rightIsClosing
                        ? closingBalance ? closingBalance.toLocaleString() : ""
                        : payment
                          ? payment.amount.toLocaleString()
                          : ""}
                    </td>
                  </tr>
                );
              })}

              <tr className="bg-slate-50">
                <td className="border border-slate-700 px-3 py-2 text-sm font-semibold" colSpan={2}>
                  Total Inward
                </td>
                <td className="border border-slate-700 px-3 py-2 text-sm text-right font-semibold">
                  {totals.receiptsTotal ? totals.receiptsTotal.toLocaleString() : ""}
                </td>
                <td className="border border-slate-700 px-3 py-2 text-sm font-semibold" colSpan={2}>
                  Total Outward
                </td>
                <td className="border border-slate-700 px-3 py-2 text-sm text-right font-semibold">
                  {totals.paymentsTotal ? totals.paymentsTotal.toLocaleString() : ""}
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        {dayEntries.receipts.length === 0 && dayEntries.payments.length === 0 ? (
          <div className="p-4 text-sm text-slate-500 border-t border-slate-200">
            No transactions recorded for this date.
          </div>
        ) : null}
      </div>
    </div>
  );
}
