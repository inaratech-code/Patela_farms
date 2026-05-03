"use client";

import { BookOpenText, Plus, Search, Trash2, ArrowRight } from "lucide-react";
import { useMemo, useState } from "react";
import { useLiveQuery } from "dexie-react-hooks";
import { db } from "@/lib/db";
import Link from "next/link";
import { makeSyncEvent } from "@/lib/syncEvents";
import { newUid } from "@/lib/uid";

function asDrCr(amount: number) {
  if (amount === 0) return { label: "0", side: "" as const };
  return amount >= 0 ? { label: `${Math.abs(amount).toLocaleString()} Dr`, side: "dr" as const } : { label: `${Math.abs(amount).toLocaleString()} Cr`, side: "cr" as const };
}

export default function LedgerPage() {
  const accounts = useLiveQuery(() => db.ledgerAccounts.toArray());
  const entries = useLiveQuery(() => db.ledgerEntries.toArray()) || [];
  const [showForm, setShowForm] = useState(false);
  const [formData, setFormData] = useState({ name: "", type: "Supplier" });

  const latestBalanceByAccountId = useMemo(() => {
    const m = new Map<number, { date: string; balance: number }>();
    for (const e of entries) {
      const cur = m.get(e.accountId);
      if (!cur || e.date > cur.date) m.set(e.accountId, { date: e.date, balance: e.balance });
    }
    return m;
  }, [entries]);

  const debitCreditTotalsByAccountId = useMemo(() => {
    const m = new Map<number, { debit: number; credit: number }>();
    for (const e of entries) {
      const cur = m.get(e.accountId) ?? { debit: 0, credit: 0 };
      cur.debit += e.debit ?? 0;
      cur.credit += e.credit ?? 0;
      m.set(e.accountId, cur);
    }
    return m;
  }, [entries]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    const account = {
      uid: newUid(),
      name: formData.name,
      type: formData.type as "Supplier" | "Customer" | "Worker",
    };

    await db.transaction("rw", db.tables, async () => {
      const id = await db.ledgerAccounts.add(account);
      await db.outbox.add(
        makeSyncEvent({
          entityType: "ledger.account",
          entityId: account.uid!,
          op: "create",
          payload: { id, account },
        })
      );
    });
    setShowForm(false);
    setFormData({ name: "", type: "Supplier" });
  };

  return (
    <div className="space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <h1 className="text-2xl font-semibold text-slate-900">Ledger Accounts</h1>
        <button 
          onClick={() => setShowForm(!showForm)}
          className="flex items-center justify-center gap-2 px-4 py-2 bg-primary text-white rounded-lg hover:bg-primary/90 transition-colors"
        >
          <Plus className="w-5 h-5" />
          <span>{showForm ? "Cancel" : "New Account"}</span>
        </button>
      </div>

      {showForm && (
        <form onSubmit={handleSubmit} className="bg-white p-6 rounded-xl shadow-sm border border-slate-100 grid grid-cols-1 sm:grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium mb-1">Entity Name</label>
            <input required type="text" value={formData.name} onChange={e => setFormData({...formData, name: e.target.value})} className="w-full px-3 py-2 border rounded-md" />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Account Type</label>
            <select value={formData.type} onChange={e => setFormData({...formData, type: e.target.value})} className="w-full px-3 py-2 border rounded-md bg-white">
              <option value="Supplier">Supplier</option>
              <option value="Customer">Customer</option>
              <option value="Worker">Worker</option>
            </select>
          </div>
          <div className="sm:col-span-2 flex justify-end mt-2">
            <button type="submit" className="px-6 py-2 bg-secondary text-white rounded-md hover:bg-secondary/90">Save Account</button>
          </div>
        </form>
      )}

      {!accounts || accounts.length === 0 ? (
        <div className="bg-white rounded-xl shadow-sm border border-slate-100 overflow-hidden p-8 text-center text-slate-500">
            <BookOpenText className="w-12 h-12 mx-auto mb-4 text-slate-300" />
            <p className="text-lg font-medium text-slate-900">No accounts found</p>
            <p className="mt-1">Create an account to start tracking ledgers.</p>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
          {accounts.map(account => (
            <Link
              key={account.id}
              href={`/ledger/${account.id}`}
              className="bg-white p-6 rounded-xl shadow-sm border border-slate-100 flex flex-col justify-between hover:border-primary/50 transition-colors cursor-pointer group"
            >
              <div>
                <div className="flex justify-between items-start">
                  <div className="min-w-0">
                    <h3 className="text-lg font-medium text-slate-900 truncate">{account.name}</h3>
                    <div className="mt-1 flex items-center gap-2">
                      <span className="px-2 py-1 bg-slate-100 text-slate-600 text-xs rounded">{account.type}</span>
                      {(() => {
                        const id = account.id;
                        const sums =
                          typeof id === "number" ? debitCreditTotalsByAccountId.get(id) ?? { debit: 0, credit: 0 } : { debit: 0, credit: 0 };
                        const bal = typeof id === "number" ? (latestBalanceByAccountId.get(id)?.balance ?? 0) : 0;
                        const { label, side } = asDrCr(bal);
                        return (
                          <div className="flex flex-col gap-1 items-start">
                            <div className="flex flex-wrap gap-x-2 gap-y-0.5 text-xs text-slate-600">
                              <span title="Total debits posted to this ledger">
                                Dr: <span className="font-semibold text-emerald-700">{sums.debit.toLocaleString()}</span>
                              </span>
                              <span className="text-slate-300" aria-hidden>
                                |
                              </span>
                              <span title="Total credits posted to this ledger">
                                Cr: <span className="font-semibold text-rose-700">{sums.credit.toLocaleString()}</span>
                              </span>
                            </div>
                            {bal === 0 ? (
                              <span className="text-xs text-slate-400">Net: settled</span>
                            ) : (
                              <span
                                className={`px-2 py-1 text-xs rounded font-semibold ${
                                  side === "dr" ? "bg-emerald-50 text-emerald-700" : "bg-rose-50 text-rose-700"
                                }`}
                                title={side === "dr" ? "Net receivable (Dr)" : "Net payable (Cr)"}
                              >
                                Net: Rs. {label}
                              </span>
                            )}
                          </div>
                        );
                      })()}
                    </div>
                  </div>
                </div>
              </div>
              <div className="mt-6 flex items-center justify-between text-primary font-medium text-sm opacity-0 group-hover:opacity-100 transition-opacity">
                <span>View Detailed Ledger</span>
                <ArrowRight className="w-4 h-4" />
              </div>
            </Link>
          ))}
        </div>
      )}
    </div>
  );
}
