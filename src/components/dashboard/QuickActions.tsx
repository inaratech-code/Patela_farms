"use client";

import Link from "next/link";
import { motion } from "framer-motion";
import { PackagePlus, ShoppingCart, Truck, Receipt, BookOpenText, UserPlus, ChevronRight } from "lucide-react";

const actions = [
  { href: "/inventory", label: "Add Inventory", icon: PackagePlus, tint: "bg-[#0871b3]/10 text-[#0871b3]" },
  { href: "/orders", label: "Record Sale", icon: ShoppingCart, tint: "bg-[#80a932]/12 text-[#80a932]" },
  { href: "/purchases", label: "Add Supplier", icon: Truck, tint: "bg-amber-500/10 text-amber-700" },
  { href: "/expenses", label: "Add Expense", icon: Receipt, tint: "bg-rose-500/10 text-rose-700" },
  { href: "/ledger", label: "New Ledger Entry", icon: BookOpenText, tint: "bg-violet-500/10 text-violet-700" },
  { href: "/users", label: "Add User", icon: UserPlus, tint: "bg-slate-500/10 text-slate-700" },
] as const;

export function QuickActions() {
  return (
    <motion.section
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.35, ease: "easeOut" }}
      className="rounded-2xl bg-white border border-[#e2e8f0] shadow-sm overflow-hidden"
    >
      <div className="p-6 border-b border-[#e2e8f0]">
        <div className="text-sm font-medium text-[#64748b]">Quick Actions</div>
        <div className="mt-1 text-lg font-semibold text-[#0f172a]">Do more in fewer taps</div>
      </div>

      <div className="p-6">
        <div className="grid grid-cols-2 sm:grid-cols-2 gap-3">
          {actions.map((a) => {
            const Icon = a.icon;
            return (
              <motion.div key={a.href} whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
                <Link
                  href={a.href}
                  className="group flex items-center gap-4 rounded-xl border border-[#e2e8f0] bg-[#f8fafc] hover:bg-white px-4 py-4 transition-colors min-h-[76px]"
                >
                  <div className="flex items-center gap-3">
                    <div className={`w-10 h-10 rounded-xl flex items-center justify-center ${a.tint}`}>
                      <Icon className="w-5 h-5" />
                    </div>
                    <div className="text-sm font-semibold text-[#0f172a] leading-snug">
                      {a.label}
                    </div>
                  </div>
                  <div className="ml-auto flex items-center gap-1 text-xs font-semibold text-[#64748b] group-hover:text-[#0f172a]">
                    <span className="hidden xs:inline">Open</span>
                    <ChevronRight className="w-4 h-4" />
                  </div>
                </Link>
              </motion.div>
            );
          })}
        </div>
      </div>
    </motion.section>
  );
}

