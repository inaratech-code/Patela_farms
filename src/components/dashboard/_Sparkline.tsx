"use client";

import { Line, LineChart, ResponsiveContainer, Tooltip } from "recharts";

function formatRs(n: number) {
  return `Rs. ${n.toLocaleString()}`;
}

export function Sparkline(props: { data: Array<{ x: string; y: number }> }) {
  return (
    <ResponsiveContainer width="100%" height="100%" minWidth={0} minHeight={0}>
      <LineChart data={props.data} margin={{ top: 4, right: 10, bottom: 2, left: 2 }}>
        <Tooltip
          cursor={false}
          content={({ active, payload }) => {
            if (!active || !payload?.length) return null;
            const v = payload[0].value as number;
            return (
              <div className="rounded-lg border border-[#e2e8f0] bg-white px-3 py-2 text-xs shadow-sm">
                <div className="font-semibold text-[#0f172a]">{formatRs(v)}</div>
              </div>
            );
          }}
        />
        <Line type="monotone" dataKey="y" stroke="#0871b3" strokeWidth={2} dot={false} />
      </LineChart>
    </ResponsiveContainer>
  );
}

