"use client";

import { Plus } from "lucide-react";
import { useMemo, useState } from "react";
import { useLiveQuery } from "dexie-react-hooks";
import { db } from "@/lib/db";
import { addLedgerEntry, getOrCreateLedgerAccountId } from "@/lib/ledger";
import { getOrCreateDefaultCashAccountId, sortAccountsForPicker, type PaymentMethod } from "@/lib/accounts";
import { makeSyncEvent } from "@/lib/syncEvents";
import { newUid } from "@/lib/uid";
import { useSearchParams } from "next/navigation";
import { buildSupplierNameOptions } from "@/lib/supplierOptions";

/** Same decimal rules as Sales (orders page). */
function normalizeQtyInput(raw: string): string {
  let s = raw.replace(/[^\d.]/g, "");
  const firstDot = s.indexOf(".");
  if (firstDot !== -1) {
    s = s.slice(0, firstDot + 1) + s.slice(firstDot + 1).replace(/\./g, "");
  }
  if (s === "") return "";

  const endsWithDot = s.endsWith(".") && (s.match(/\./g) ?? []).length === 1;
  const parts = s.split(".");
  let intPart = parts[0] ?? "";
  const fracPart = parts[1];

  if (intPart === "" && fracPart !== undefined) {
    intPart = "0";
  }
  if (intPart.length > 1) {
    intPart = intPart.replace(/^0+/, "") || "0";
  }

  if (fracPart !== undefined) {
    if (fracPart === "" && endsWithDot) return `${intPart}.`;
    return `${intPart}.${fracPart}`;
  }
  return intPart;
}

function parseQty(s: string): number | null {
  const t = s.trim();
  if (t === "" || t === ".") return null;
  const n = Number.parseFloat(t);
  if (!Number.isFinite(n)) return null;
  return n;
}

type PurchaseLine = { itemId: number; quantity: number; unitCost: number };

export default function PurchasesPage() {
  const inventory = useLiveQuery(() => db.inventory.toArray());
  const purchases = useLiveQuery(() => db.purchases.toArray());
  const ledgerSuppliers = useLiveQuery(() => db.ledgerAccounts.where("type").equals("Supplier").toArray()) || [];
  const financialAccounts = useLiveQuery(() => db.financialAccounts.toArray()) || [];
  const searchParams = useSearchParams();

  const [showForm, setShowForm] = useState(() => searchParams.get("new") === "1");
  const [purchaseQtyStr, setPurchaseQtyStr] = useState("1");
  const [purchaseUnitCostStr, setPurchaseUnitCostStr] = useState("");
  const [purchaseForm, setPurchaseForm] = useState({
    supplierName: "",
    date: new Date().toISOString().slice(0, 10), // YYYY-MM-DD
    paymentType: "Due" as "Paid" | "Due",
    method: "Cash" as PaymentMethod,
    financialAccountId: 0,
    lineItemDraft: { itemId: 0 },
    lineItems: [] as PurchaseLine[],
  });

  const purchaseTotal = useMemo(() => {
    return purchaseForm.lineItems.reduce((acc, li) => acc + li.unitCost * li.quantity, 0);
  }, [purchaseForm.lineItems]);

  const addPurchaseLineItem = () => {
    const itemId = Number(purchaseForm.lineItemDraft.itemId);
    const list = inventory ?? [];
    const item = list.find((i) => i.id === itemId);
    if (!itemId || !item) return;

    const qty = parseQty(purchaseQtyStr);
    if (qty == null || qty <= 0) {
      alert("Enter a quantity greater than 0.");
      return;
    }

    let unitCost = parseQty(purchaseUnitCostStr.trim());
    if (unitCost == null || unitCost <= 0) {
      unitCost = Number(item.costPrice ?? 0);
    }
    if (!Number.isFinite(unitCost) || unitCost <= 0) {
      alert("Enter a valid cost per unit (greater than 0), or set a list cost on the item.");
      return;
    }

    setPurchaseForm((prev) => {
      const existingIdx = prev.lineItems.findIndex((li) => li.itemId === itemId);
      const nextLineItems = [...prev.lineItems];
      if (existingIdx >= 0) {
        const ex = nextLineItems[existingIdx];
        const newQty = ex.quantity + qty;
        const blendedUnit = newQty > 0 ? (ex.unitCost * ex.quantity + unitCost * qty) / newQty : unitCost;
        nextLineItems[existingIdx] = { itemId, quantity: newQty, unitCost: blendedUnit };
      } else {
        nextLineItems.push({ itemId, quantity: qty, unitCost });
      }
      return { ...prev, lineItems: nextLineItems, lineItemDraft: { itemId: 0 } };
    });
    setPurchaseQtyStr("1");
    setPurchaseUnitCostStr("");
  };

  const removePurchaseLineItem = (itemId: number) => {
    setPurchaseForm((prev) => ({
      ...prev,
      lineItems: prev.lineItems.filter((li) => li.itemId !== itemId),
    }));
  };

  const handlePurchaseSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!purchaseForm.supplierName.trim()) return alert("Supplier name is required.");
    if (purchaseForm.lineItems.length === 0) return alert("Add at least one item to purchase.");

    const inv = inventory ?? [];
    const date = new Date(`${purchaseForm.date}T12:00:00`).toISOString();
    const supplierName = purchaseForm.supplierName.trim();

    const lineItemsResolved = purchaseForm.lineItems.map((li) => {
      const item = inv.find((i) => i.id === li.itemId);
      return { ...li, item };
    });

    if (lineItemsResolved.some((li) => !li.item)) return alert("One or more selected items were not found.");

    const totalCost = lineItemsResolved.reduce((acc, li) => acc + li.unitCost * li.quantity, 0);
    const description = `Purchase from ${supplierName} (${purchaseForm.lineItems.length} item(s))`;

    let supplierLedgerId: number | undefined;
    if (purchaseForm.paymentType === "Due") {
      supplierLedgerId = await getOrCreateLedgerAccountId({ name: supplierName, type: "Supplier" });
    }

    await db.transaction("rw", db.tables, async () => {
      for (const li of lineItemsResolved) {
        const item = li.item!;
        const lineCost = li.unitCost * li.quantity;
        const paid = purchaseForm.paymentType === "Paid" ? lineCost : 0;
        const due = purchaseForm.paymentType === "Paid" ? 0 : lineCost;
        const paymentStatus = purchaseForm.paymentType === "Paid" ? ("paid" as const) : ("due" as const);

        const purchase = {
          uid: newUid(),
          supplierName,
          supplierId: supplierLedgerId,
          itemId: item.id!,
          quantity: li.quantity,
          totalCost: lineCost,
          date,
          paidAmount: paid,
          dueAmount: due,
          paymentStatus,
        };
        await db.purchases.add(purchase);

        const prevQty = item.quantity;
        const prevAvg = Number(item.avgCost ?? item.costPrice ?? 0);
        const addQty = li.quantity;
        const newQty = prevQty + addQty;
        const newAvg = newQty > 0 ? (prevQty * prevAvg + lineCost) / newQty : prevAvg;
        await db.inventory.update(item.id!, { quantity: newQty, avgCost: newAvg });
        const movement = { uid: newUid(), itemId: item.id!, quantity: li.quantity, type: "IN" as const, reason: "Purchase" as const, date };
        await db.stockMovement.add(movement);

        await db.outbox.add(
          makeSyncEvent({
            entityType: "order.purchase",
            entityId: newUid(),
            op: "create",
            payload: {
              supplierName,
              date,
              paymentType: purchaseForm.paymentType === "Paid" ? "Cash" : "Credit",
              method: purchaseForm.method,
              totalCost: lineCost,
              purchases: [{ ...purchase, itemUid: item.uid }],
              movements: [{ ...movement, itemUid: item.uid }],
              inventoryDeltas: [{ itemUid: item.uid, delta: li.quantity }],
            },
          })
        );
      }

      // Accounting:
      // - Paid: affects Day Book (selected account), no payable.
      // - Due: creates payable in ledger, no Day Book cash impact.
      if (purchaseForm.paymentType === "Paid") {
        const accountId = Number(purchaseForm.financialAccountId) || (await getOrCreateDefaultCashAccountId());
        const acct = await db.financialAccounts.get(accountId);
        const day = {
          uid: newUid(),
          time: date,
          type: "Expense" as const,
          category: "Purchase" as const,
          amount: totalCost,
          description: `${description} (${purchaseForm.method})`,
          method: purchaseForm.method,
          accountId,
        };
        await db.dayBook.add(day);
        await db.outbox.add(
          makeSyncEvent({
            entityType: "daybook.entry",
            entityId: day.uid,
            op: "create",
            payload: { entry: { ...day, account: acct?.uid ? { uid: acct.uid, name: acct.name, type: acct.type } : null } },
          })
        );
      } else {
        const accountId = supplierLedgerId ?? (await getOrCreateLedgerAccountId({ name: supplierName, type: "Supplier" }));
        const ledgerEntryId = (await addLedgerEntry({ accountId, date, description, debit: 0, credit: totalCost })) as number;
        const acct = await db.ledgerAccounts.get(accountId);
        const entryRow = await db.ledgerEntries.get(ledgerEntryId);
        if (acct?.uid && entryRow?.uid) {
          await db.outbox.add(
            makeSyncEvent({
              entityType: "ledger.entry",
              entityId: entryRow.uid,
              op: "create",
              payload: {
                account: { uid: acct.uid, name: acct.name, type: acct.type },
                entry: { uid: entryRow.uid, date: entryRow.date, description: entryRow.description, debit: entryRow.debit, credit: entryRow.credit },
              },
            })
          );
        }
      }
    });

    setShowForm(false);
    setPurchaseQtyStr("1");
    setPurchaseUnitCostStr("");
    setPurchaseForm({
      supplierName: "",
      date: new Date().toISOString().slice(0, 10),
      paymentType: "Due",
      method: "Cash",
      financialAccountId: 0,
      lineItemDraft: { itemId: 0 },
      lineItems: [],
    });
  };

  const inventoryList = inventory ?? [];
  const purchaseList = purchases ?? [];

  const supplierOptions = useMemo(
    () => buildSupplierNameOptions(ledgerSuppliers, purchaseList),
    [ledgerSuppliers, purchaseList]
  );

  return (
    <div className="space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <h1 className="text-2xl font-semibold text-slate-900">Purchase / Supplier Management</h1>
        <button
          onClick={() => setShowForm(!showForm)}
          className="flex items-center justify-center gap-2 px-4 py-2 bg-primary text-white rounded-lg hover:bg-primary/90 transition-colors"
        >
          <Plus className="w-5 h-5" />
          <span>{showForm ? "Cancel" : "New Purchase"}</span>
        </button>
      </div>

      {showForm && (
        <form
          onSubmit={handlePurchaseSubmit}
          className="bg-white p-6 rounded-xl shadow-sm border border-slate-100 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4"
        >
          <div className="lg:col-span-2">
            <label className="block text-sm font-medium mb-1">Supplier</label>
            <input
              required
              type="text"
              list="purchases-supplier-names"
              autoComplete="off"
              value={purchaseForm.supplierName}
              onChange={(e) => setPurchaseForm({ ...purchaseForm, supplierName: e.target.value })}
              className="w-full px-3 py-2 border rounded-md bg-white"
              placeholder="Pick from list or type a new supplier"
            />
            <datalist id="purchases-supplier-names">
              {supplierOptions.map((name) => (
                <option key={name} value={name} />
              ))}
            </datalist>
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Date</label>
            <input
              required
              type="date"
              value={purchaseForm.date}
              onChange={(e) => setPurchaseForm({ ...purchaseForm, date: e.target.value })}
              className="w-full px-3 py-2 border rounded-md"
            />
          </div>
          <div className="lg:col-span-2">
            <label className="block text-sm font-medium mb-1">Payment Type</label>
            <select
              value={purchaseForm.paymentType}
              onChange={(e) => setPurchaseForm({ ...purchaseForm, paymentType: e.target.value as "Paid" | "Due" })}
              className="w-full px-3 py-2 border rounded-md bg-white"
            >
              <option value="Due">Credit (Payable in Ledger)</option>
              <option value="Paid">Paid Now (Affects Account)</option>
            </select>
          </div>

          {purchaseForm.paymentType === "Paid" ? (
            <>
              <div>
                <label className="block text-sm font-medium mb-1">Payment Mode</label>
                <select
                  value={purchaseForm.method}
                  onChange={(e) => setPurchaseForm({ ...purchaseForm, method: e.target.value as PaymentMethod })}
                  className="w-full px-3 py-2 border rounded-md bg-white"
                >
                  <option value="Cash">Cash</option>
                  <option value="QR">QR</option>
                  <option value="BankTransfer">Bank Transfer</option>
                </select>
              </div>
              <div className="sm:col-span-2 lg:col-span-4">
                <label className="block text-sm font-medium mb-1">Account</label>
                <select
                  value={purchaseForm.financialAccountId}
                  onChange={(e) => setPurchaseForm({ ...purchaseForm, financialAccountId: Number(e.target.value) })}
                  className="w-full px-3 py-2 border rounded-md bg-white"
                >
                  <option value={0}>Select account…</option>
                  {sortAccountsForPicker(financialAccounts).map((a) => (
                    <option key={a.id} value={a.id}>
                      {a.type} — {a.name}
                    </option>
                  ))}
                </select>
              </div>
            </>
          ) : null}

          <div>
            <label className="block text-sm font-medium mb-1">Select Item</label>
            <select
              value={purchaseForm.lineItemDraft.itemId}
              onChange={(e) => {
                const id = Number(e.target.value);
                const row = inventoryList.find((i) => i.id === id);
                setPurchaseForm({
                  ...purchaseForm,
                  lineItemDraft: { itemId: id },
                });
                setPurchaseUnitCostStr(row ? String(row.costPrice ?? "") : "");
              }}
              className="w-full px-3 py-2 border rounded-md bg-white"
            >
              <option value={0}>Select...</option>
              {inventoryList.map((i) => (
                <option key={i.id} value={i.id}>
                  {i.name} ({i.quantity} {i.unit} on hand)
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Quantity</label>
            <input
              type="text"
              inputMode="decimal"
              autoComplete="off"
              value={purchaseQtyStr}
              onChange={(e) => setPurchaseQtyStr(normalizeQtyInput(e.target.value))}
              className="w-full px-3 py-2 border rounded-md"
              placeholder="e.g. 29.8"
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Cost (per unit)</label>
            <input
              type="text"
              inputMode="decimal"
              autoComplete="off"
              value={purchaseUnitCostStr}
              onChange={(e) => setPurchaseUnitCostStr(normalizeQtyInput(e.target.value))}
              className="w-full px-3 py-2 border rounded-md"
              placeholder="Buy rate per unit"
              title="Leave blank to use the item list cost"
            />
          </div>
          <div className="lg:col-span-2 flex items-end">
            <button
              type="button"
              onClick={addPurchaseLineItem}
              className="w-full px-4 py-2 bg-secondary text-white rounded-md hover:bg-secondary/90"
            >
              Add Item
            </button>
          </div>

          <div className="lg:col-span-5 border rounded-lg overflow-hidden">
            <div className="bg-slate-50 px-4 py-2 text-sm font-medium text-slate-700">Items Purchased</div>
            {purchaseForm.lineItems.length === 0 ? (
              <div className="px-4 py-6 text-sm text-slate-500">No items added yet.</div>
            ) : (
              <table className="min-w-full divide-y divide-slate-200">
                <thead className="bg-white">
                  <tr>
                    <th className="px-4 py-2 text-left text-xs font-medium text-slate-500 uppercase">Item</th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-slate-500 uppercase">Qty</th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-slate-500 uppercase">Unit cost</th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-slate-500 uppercase">Line total</th>
                    <th className="px-4 py-2 text-right text-xs font-medium text-slate-500 uppercase">Remove</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-slate-200">
                  {purchaseForm.lineItems.map((li) => {
                    const item = inventoryList.find((i) => i.id === li.itemId);
                    const lineCost = li.unitCost * li.quantity;
                    return (
                      <tr key={li.itemId}>
                        <td className="px-4 py-2 text-sm text-slate-900">{item?.name ?? "Unknown"}</td>
                        <td className="px-4 py-2 text-sm text-slate-900">{li.quantity}</td>
                        <td className="px-4 py-2 text-sm text-slate-900">Rs. {li.unitCost.toLocaleString()}</td>
                        <td className="px-4 py-2 text-sm text-slate-900">Rs. {lineCost.toLocaleString()}</td>
                        <td className="px-4 py-2 text-right">
                          <button
                            type="button"
                            onClick={() => removePurchaseLineItem(li.itemId)}
                            className="text-alert-red hover:text-alert-red/80 text-sm font-medium"
                          >
                            Remove
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </div>

          <div className="sm:col-span-2 lg:col-span-5 flex justify-between items-center mt-2 border-t border-slate-200 pt-4">
            <div className="text-lg font-semibold text-slate-900">Total: Rs. {purchaseTotal.toLocaleString()}</div>
            <button type="submit" className="px-6 py-2 bg-alert-green text-white rounded-md hover:bg-alert-green/90">
              Complete Purchase
            </button>
          </div>
        </form>
      )}

      <div className="bg-white rounded-xl shadow-sm border border-slate-100 overflow-hidden">
        {purchaseList.length === 0 ? (
          <div className="p-8 text-center text-slate-500">
            <p className="text-lg font-medium text-slate-900">No purchases recorded</p>
          </div>
        ) : (
          <table className="min-w-full divide-y divide-slate-200">
            <thead className="bg-slate-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase">Date</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase">Supplier</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase">Item</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase">Qty</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase">Cost</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-slate-200">
              {purchaseList
                .slice()
                .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime())
                .map((purchase) => {
                  const item = inventoryList.find((i) => i.id === purchase.itemId);
                  return (
                    <tr key={purchase.id}>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-500">
                        {new Date(purchase.date).toLocaleDateString()}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-900">{purchase.supplierName}</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-900">{item?.name || "Unknown"}</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-900">{purchase.quantity}</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-slate-900">Rs. {purchase.totalCost}</td>
                    </tr>
                  );
                })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

