"use client";

import Link from "next/link";
import { useLiveQuery } from "dexie-react-hooks";
import { db } from "@/lib/db";
import { ArrowLeft } from "lucide-react";
import { useMemo } from "react";

/** Line-item debit/credit: always show the number (including 0) so both columns stay visible. */
function formatLedgerSide(n: number) {
  return n.toLocaleString();
}

function formatDate(iso: string) {
  const d = new Date(iso);
  return d.toLocaleDateString();
}

function asDrCr(amount: number) {
  if (amount === 0) return "-";
  return amount >= 0 ? "Dr" : "Cr";
}

export function LedgerDetailClient(props: { accountId: number }) {
  const accountId = props.accountId;

  const account = useLiveQuery(
    () => (Number.isFinite(accountId) ? db.ledgerAccounts.get(accountId) : undefined),
    [accountId]
  );

  const entries =
    useLiveQuery(
      () => (Number.isFinite(accountId) ? db.ledgerEntries.where("accountId").equals(accountId).toArray() : []),
      [accountId]
    ) || [];

  const rows = useMemo(() => {
    const sorted = entries.slice().sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
    return sorted.reduce<Array<(typeof sorted)[number] & { opening: number; closing: number }>>((acc, e) => {
      const opening = acc.length ? acc[acc.length - 1].closing : 0;
      const closing = opening + (e.debit - e.credit);
      acc.push({ ...e, opening, closing });
      return acc;
    }, []);
  }, [entries]);

  const totals = useMemo(() => {
    return rows.reduce(
      (acc, r) => {
        acc.debit += r.debit;
        acc.credit += r.credit;
        return acc;
      },
      { debit: 0, credit: 0 }
    );
  }, [rows]);

  const latestBalance = rows.length ? rows[rows.length - 1].closing : 0;
  const timePeriod = useMemo(() => {
    if (!rows.length) return "";
    const from = formatDate(rows[0].date);
    const to = formatDate(rows[rows.length - 1].date);
    return from === to ? from : `${from} to ${to}`;
  }, [rows]);

  if (!Number.isFinite(accountId)) {
    return (
      <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-8 text-slate-600">Invalid account.</div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between gap-4">
        <Link
          href="/ledger"
          className="inline-flex items-center gap-2 px-3 py-2 rounded-lg border border-slate-200 hover:bg-slate-50 text-sm font-medium text-slate-700"
        >
          <ArrowLeft className="w-4 h-4" />
          Back
        </Link>
        <div className="text-sm text-slate-500">Dr = balance receivable · Cr = balance payable</div>
      </div>

      <div className="rounded-xl shadow-sm border border-slate-300 overflow-hidden bg-white">
        <div className="px-6 py-4 border-b border-slate-300 bg-emerald-50">
          <div className="text-center text-lg font-bold text-slate-900">Ledger Reconciliation</div>
        </div>
        <div className="px-6 py-4 border-b border-slate-300 flex items-center justify-between gap-4">
          <div className="text-sm text-slate-700">
            <span className="font-semibold">{account?.name ?? "Account"}</span>
            {account?.type ? <span className="text-slate-500"> ({account.type})</span> : null}
            {timePeriod ? <span className="text-slate-500"> · {timePeriod}</span> : null}
          </div>
          <div className="text-right">
            <div className="text-xs text-slate-600">Closing Balance</div>
            <div className="text-lg font-bold text-slate-900">
              Rs. {Math.abs(latestBalance).toLocaleString()}{" "}
              <span className="text-sm font-semibold text-slate-700">{asDrCr(latestBalance)}</span>
            </div>
          </div>
        </div>

        {rows.length === 0 ? (
          <div className="p-10 text-center text-slate-500">No ledger entries yet.</div>
        ) : (
          <div className="w-full overflow-x-auto">
            <table className="min-w-[1100px] w-full border-collapse">
              <thead>
                <tr className="bg-emerald-100">
                  <th className="border border-slate-700 px-3 py-2 text-left text-xs font-semibold uppercase">Date</th>
                  <th className="border border-slate-700 px-3 py-2 text-left text-xs font-semibold uppercase">Description</th>
                  <th className="border border-slate-700 px-3 py-2 text-right text-xs font-semibold uppercase">Opening Balance</th>
                  <th className="border border-slate-700 px-3 py-2 text-right text-xs font-semibold uppercase">Debit</th>
                  <th className="border border-slate-700 px-3 py-2 text-right text-xs font-semibold uppercase">Credit</th>
                  <th className="border border-slate-700 px-3 py-2 text-center text-xs font-semibold uppercase">Dr or Cr</th>
                  <th className="border border-slate-700 px-3 py-2 text-right text-xs font-semibold uppercase">Closing Balance</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((e) => (
                  <tr key={e.id} className="h-10">
                    <td className="border border-slate-700 px-3 py-2 text-sm whitespace-nowrap">{formatDate(e.date)}</td>
                    <td className="border border-slate-700 px-3 py-2 text-sm">{e.description}</td>
                    <td className="border border-slate-700 px-3 py-2 text-sm text-right whitespace-nowrap">
                      {e.opening ? Math.abs(e.opening).toLocaleString() : "-"}{" "}
                      <span className="text-xs text-slate-600">{e.opening ? asDrCr(e.opening) : ""}</span>
                    </td>
                    <td className="border border-slate-700 px-3 py-2 text-sm text-right whitespace-nowrap font-semibold text-emerald-700">
                      {formatLedgerSide(e.debit)}
                    </td>
                    <td className="border border-slate-700 px-3 py-2 text-sm text-right whitespace-nowrap font-semibold text-rose-700">
                      {formatLedgerSide(e.credit)}
                    </td>
                    <td className="border border-slate-700 px-3 py-2 text-sm text-center font-semibold">{asDrCr(e.closing)}</td>
                    <td className="border border-slate-700 px-3 py-2 text-sm text-right whitespace-nowrap font-semibold">
                      {e.closing === 0 ? (
                        <span className="text-slate-500">0</span>
                      ) : (
                        <>
                          {Math.abs(e.closing).toLocaleString()}{" "}
                          <span className="text-xs font-semibold text-slate-600">{asDrCr(e.closing)}</span>
                        </>
                      )}
                    </td>
                  </tr>
                ))}

                <tr className="bg-emerald-50">
                  <td className="border border-slate-700 px-3 py-2 text-sm font-semibold text-center" colSpan={2}>
                    Total
                  </td>
                  <td className="border border-slate-700 px-3 py-2 text-sm" />
                  <td className="border border-slate-700 px-3 py-2 text-sm text-right font-semibold text-emerald-700">
                    {totals.debit.toLocaleString()}
                  </td>
                  <td className="border border-slate-700 px-3 py-2 text-sm text-right font-semibold text-rose-700">
                    {totals.credit.toLocaleString()}
                  </td>
                  <td className="border border-slate-700 px-3 py-2 text-sm" />
                    <td className="border border-slate-700 px-3 py-2 text-sm text-right font-semibold">
                    {Math.abs(latestBalance).toLocaleString()}{" "}
                    <span className="text-xs font-semibold text-slate-700">{asDrCr(latestBalance)}</span>
                  </td>
                </tr>

                <tr className="bg-emerald-100">
                  <td className="border border-slate-700 px-3 py-2 text-sm font-semibold text-center" colSpan={6}>
                    Closing Balance
                  </td>
                  <td className="border border-slate-700 px-3 py-2 text-sm text-right font-bold">
                    {Math.abs(latestBalance).toLocaleString()}{" "}
                    <span className="text-xs font-semibold text-slate-700">{asDrCr(latestBalance)}</span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

