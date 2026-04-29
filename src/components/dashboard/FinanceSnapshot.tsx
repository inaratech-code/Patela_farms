"use client";

import { motion } from "framer-motion";
import { ArrowDownRight, ArrowUpRight, Wallet, Receipt, HandCoins } from "lucide-react";

type Mini = {
  id: string;
  label: string;
  value: string;
  tone: "blue" | "green" | "red" | "amber";
  icon: React.ComponentType<{ className?: string }>;
};

function tileTone(t: Mini["tone"]) {
  if (t === "green") return "bg-[#80a932]/10 text-[#80a932]";
  if (t === "red") return "bg-rose-500/10 text-rose-700";
  if (t === "amber") return "bg-amber-500/10 text-amber-700";
  return "bg-[#0871b3]/10 text-[#0871b3]";
}

export function FinanceSnapshot(props: {
  receivable: number;
  payable: number;
  expensesToday: number;
  cashInHand: number;
}) {
  const tiles: Mini[] = [
    { id: "recv", label: "Receivable", value: `Rs. ${props.receivable.toLocaleString()}`, tone: "green", icon: ArrowUpRight },
    { id: "pay", label: "Payable", value: `Rs. ${props.payable.toLocaleString()}`, tone: "red", icon: ArrowDownRight },
    { id: "exp", label: "Expenses Today", value: `Rs. ${props.expensesToday.toLocaleString()}`, tone: "amber", icon: Receipt },
    { id: "cash", label: "Cash In Hand", value: `Rs. ${props.cashInHand.toLocaleString()}`, tone: "blue", icon: Wallet },
  ];

  return (
    <motion.section
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.35, ease: "easeOut" }}
      className="rounded-2xl bg-white border border-[#e2e8f0] shadow-sm overflow-hidden"
    >
      <div className="p-6 border-b border-[#e2e8f0] flex items-center justify-between">
        <div>
          <div className="text-sm font-medium text-[#64748b]">Financial Snapshot</div>
          <div className="mt-1 text-lg font-semibold text-[#0f172a]">At a glance</div>
        </div>
        <HandCoins className="w-5 h-5 text-[#64748b]" />
      </div>

      <div className="p-6 grid grid-cols-2 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {tiles.map((t) => {
          const Icon = t.icon;
          return (
            <div key={t.id} className="rounded-2xl border border-[#e2e8f0] bg-[#f8fafc] p-4 hover:bg-white transition-colors">
              <div className="flex items-center justify-between gap-4">
                <div>
                  <div className="text-xs font-semibold text-[#64748b]">{t.label}</div>
                  <div className="mt-2 text-lg font-semibold text-[#0f172a]">{t.value}</div>
                </div>
                <div className={`w-10 h-10 rounded-xl flex items-center justify-center ${tileTone(t.tone)}`}>
                  <Icon className="w-5 h-5" />
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </motion.section>
  );
}

