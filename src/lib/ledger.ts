import { db, type LedgerAccount, type LedgerEntry } from "@/lib/db";
import { newUid } from "@/lib/uid";

export type LedgerAccountType = LedgerAccount["type"];

/** Ledger party used for typical cash / counter sales (no named credit customer). */
export const WALK_IN_CUSTOMER_NAME = "Walk in customer";

export async function getOrCreateWalkInCustomerAccountId() {
  return getOrCreateLedgerAccountId({
    name: WALK_IN_CUSTOMER_NAME,
    type: "Customer",
  });
}

function computeNextBalance(prevBalance: number, debit: number, credit: number) {
  return prevBalance + (debit - credit);
}

export async function getOrCreateLedgerAccountId(params: {
  name: string;
  type: LedgerAccountType;
}) {
  const name = params.name.trim();
  if (!name) throw new Error("Ledger account name is required");

  const existing = await db.ledgerAccounts
    .where({ name, type: params.type })
    .first();

  if (typeof existing?.id === "number") {
    if (!existing.uid) await db.ledgerAccounts.update(existing.id, { uid: newUid() });
    return existing.id;
  }

  const id = await db.ledgerAccounts.add({ uid: newUid(), name, type: params.type });
  if (typeof id !== "number") throw new Error("Failed to create ledger account");
  return id;
}

export async function addLedgerEntry(params: {
  accountId: number;
  date: string; // ISO
  description: string;
  debit?: number;
  credit?: number;
}) {
  const debit = Number(params.debit ?? 0);
  const credit = Number(params.credit ?? 0);

  const last = await db.ledgerEntries
    .where("accountId")
    .equals(params.accountId)
    .sortBy("date");

  const prevBalance = last.length ? last[last.length - 1].balance : 0;
  const balance = computeNextBalance(prevBalance, debit, credit);

  const entry: Omit<LedgerEntry, "id"> = {
    uid: newUid(),
    accountId: params.accountId,
    date: params.date,
    description: params.description,
    debit,
    credit,
    balance,
  };

  return await db.ledgerEntries.add(entry);
}

