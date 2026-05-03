import type { LedgerAccount, Purchase } from "@/lib/db";

/** Names for supplier pickers: ledger Supplier accounts + any name seen on purchases. */
export function buildSupplierNameOptions(ledgerSuppliers: LedgerAccount[], purchases: Purchase[]): string[] {
  const names = new Set<string>();
  for (const a of ledgerSuppliers) {
    if (a.type === "Supplier" && a.name?.trim()) names.add(a.name.trim());
  }
  for (const p of purchases) {
    const n = p.supplierName?.trim();
    if (n) names.add(n);
  }
  return Array.from(names).sort((a, b) => a.localeCompare(b));
}
