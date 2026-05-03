"use client";

import { motion } from "framer-motion";
import { ArrowDownRight, ArrowUpRight, AlertTriangle, Clock } from "lucide-react";

type ActivityItem = {
  id: string;
  title: string;
  subtitle: string;
  time: string;
  chip: { label: string; tone: "success" | "danger" | "warn" | "neutral" };
  icon: "income" | "expense" | "alert";
};

function chipClass(tone: ActivityItem["chip"]["tone"]) {
  if (tone === "success") return "bg-[#80a932]/10 text-[#80a932] border-[#80a932]/25";
  if (tone === "danger") return "bg-rose-500/10 text-rose-700 border-rose-500/25";
  if (tone === "warn") return "bg-amber-500/10 text-amber-700 border-amber-500/25";
  return "bg-slate-500/10 text-slate-700 border-slate-500/25";
}

export function RecentActivity(props: { items: ActivityItem[] }) {
  return (
    <motion.section
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.35, ease: "easeOut" }}
      className="rounded-2xl bg-white border border-[#e2e8f0] shadow-sm overflow-hidden"
    >
      <div className="p-6 border-b border-[#e2e8f0] flex items-center justify-between">
        <div>
          <div className="text-sm font-medium text-[#64748b]">Recent Activity</div>
          <div className="mt-1 text-lg font-semibold text-[#0f172a]">Latest updates</div>
        </div>
        <Clock className="w-5 h-5 text-[#64748b]" />
      </div>

      {props.items.length === 0 ? (
        <div className="p-8 text-center text-[#64748b] text-sm">No recent activity.</div>
      ) : (
        <div className="px-6 pb-6 pt-2 min-h-0">
          <div
            className="max-h-[28rem] overflow-y-auto overflow-x-hidden overscroll-contain pr-1 [scrollbar-gutter:stable]"
            style={{ WebkitOverflowScrolling: "touch" }}
          >
            <div className="space-y-4 pt-4">
              {props.items.map((a, idx) => (
                <div key={a.id} className="relative pl-10">
                  {idx !== props.items.length - 1 ? (
                    <div className="absolute left-4 top-9 bottom-0 w-px bg-[#e2e8f0]" />
                  ) : null}
                  <div className="absolute left-0 top-1 w-8 h-8 rounded-xl border border-[#e2e8f0] bg-[#f8fafc] flex items-center justify-center">
                    {a.icon === "income" ? (
                      <ArrowUpRight className="w-4 h-4 text-[#80a932]" />
                    ) : a.icon === "expense" ? (
                      <ArrowDownRight className="w-4 h-4 text-rose-700" />
                    ) : (
                      <AlertTriangle className="w-4 h-4 text-amber-700" />
                    )}
                  </div>

                  <div className="rounded-xl border border-[#e2e8f0] bg-white hover:bg-[#f8fafc] transition-colors p-4 flex items-start justify-between gap-4">
                    <div className="min-w-0">
                      <div className="text-sm font-semibold text-[#0f172a] truncate">{a.title}</div>
                      <div className="mt-1 text-sm text-[#64748b] truncate">{a.subtitle}</div>
                      <div className="mt-2 text-xs text-[#64748b] inline-flex items-center gap-2">
                        <span className="w-1.5 h-1.5 rounded-full bg-[#64748b]" />
                        {a.time}
                      </div>
                    </div>
                    <span className={`shrink-0 inline-flex items-center rounded-full border px-3 py-1 text-xs font-semibold ${chipClass(a.chip.tone)}`}>
                      {a.chip.label}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </motion.section>
  );
}

