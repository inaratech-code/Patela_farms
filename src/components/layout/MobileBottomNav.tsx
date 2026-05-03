"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState } from "react";
import { cn } from "@/lib/utils";
import { Home, Package, Plus, Wallet, Settings, X, ShoppingCart, Truck, ArrowUpDown, Receipt, Users, HandCoins, AlertTriangle, BarChart3, Soup } from "lucide-react";

const nav = [
  { href: "/", label: "Home", icon: Home, isAction: false as const },
  { href: "/inventory", label: "Inventory", icon: Package, isAction: false as const },
  { href: "/add", label: "Add", icon: Plus, isAction: true as const },
  { href: "/ledger", label: "Ledger", icon: Wallet, isAction: false as const },
  { href: "/settings", label: "Settings", icon: Settings, isAction: false as const },
] as const;

const addActions = [
  { href: "/inventory", label: "Add Inventory", icon: Package },
  { href: "/orders", label: "Record Sale", icon: ShoppingCart },
  { href: "/purchases", label: "Add Supplier / Purchase", icon: Truck },
  { href: "/consumption", label: "Feed usage", icon: Package },
  { href: "/stock-movement", label: "Stock Movement", icon: ArrowUpDown },
  { href: "/loss-wastage", label: "Loss / Wastage", icon: AlertTriangle },
  { href: "/reports", label: "Reports", icon: BarChart3 },
  { href: "/expenses", label: "Add Expense", icon: Receipt },
  { href: "/customers", label: "Add Customer", icon: Users },
  { href: "/suppliers", label: "Add Supplier", icon: Truck },
  { href: "/payments?direction=Receive&partyType=Customer", label: "Receive Payment", icon: HandCoins },
  { href: "/payments?direction=Pay&partyType=Supplier", label: "Pay Supplier", icon: HandCoins },
  { href: "/outstanding", label: "Outstanding", icon: HandCoins },
  { href: "/accounts", label: "Accounts", icon: Wallet },
] as const;

export function MobileBottomNav() {
  const pathname = usePathname();
  const [isOpen, setIsOpen] = useState(false);

  return (
    <>
      {isOpen && (
        <div className="fixed inset-0 z-40 lg:hidden">
          <div className="absolute inset-0 bg-slate-900/50" onClick={() => setIsOpen(false)} />
          <div className="absolute left-0 right-0 bottom-0 bg-white rounded-t-2xl shadow-xl border border-slate-200 p-4">
            <div className="flex items-center justify-between">
              <div className="text-sm font-semibold text-slate-900">Quick Add</div>
              <button
                type="button"
                className="p-2 rounded-md hover:bg-slate-50 text-slate-600"
                onClick={() => setIsOpen(false)}
                aria-label="Close"
              >
                <X className="w-5 h-5" />
              </button>
            </div>

            <div className="mt-3 grid grid-cols-1 gap-2">
              {addActions.map((a) => {
                const Icon = a.icon;
                return (
                  <Link
                    key={a.href}
                    href={a.href}
                    className="flex items-center gap-3 px-4 py-3 rounded-lg border border-slate-200 hover:bg-slate-50"
                    onClick={() => setIsOpen(false)}
                  >
                    <Icon className="w-5 h-5 text-primary" />
                    <span className="text-sm font-medium text-slate-900">{a.label}</span>
                  </Link>
                );
              })}
            </div>
          </div>
        </div>
      )}

      <nav
        className="fixed bottom-0 left-0 right-0 z-30 lg:hidden bg-white border-t border-slate-200 pb-[env(safe-area-inset-bottom,0px)]"
        aria-label="Primary"
      >
        <div className="grid grid-cols-5 max-w-lg mx-auto w-full">
          {nav.map((item) => {
            const Icon = item.icon;
            const isActive = !item.isAction && pathname === item.href;
            if (item.isAction) {
              return (
                <button
                  key={item.label}
                  type="button"
                  onClick={() => setIsOpen(true)}
                  className="py-3 flex flex-col items-center justify-center"
                  aria-label="Add"
                >
                  <span className="w-12 h-12 -mt-6 rounded-full bg-primary text-white flex items-center justify-center shadow-lg border-4 border-white">
                    <Plus className="w-6 h-6" />
                  </span>
                  <span className="mt-1 text-[11px] font-medium text-slate-600">Add</span>
                </button>
              );
            }

            return (
              <Link
                key={item.href}
                href={item.href}
                className={cn(
                  "py-3 flex flex-col items-center justify-center gap-1",
                  isActive ? "text-primary" : "text-slate-500"
                )}
              >
                <Icon className={cn("w-6 h-6", isActive ? "text-primary" : "text-slate-500")} />
                <span className={cn("text-[11px] font-medium", isActive ? "text-primary" : "text-slate-600")}>
                  {item.label}
                </span>
              </Link>
            );
          })}
        </div>
      </nav>
    </>
  );
}

