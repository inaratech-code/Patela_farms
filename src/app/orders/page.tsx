"use client";

import { ShoppingCart, Plus } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "next/navigation";
import { useLiveQuery } from "dexie-react-hooks";
import { db, type PaymentStatusErp } from "@/lib/db";
import {
  addLedgerEntry,
  getOrCreateLedgerAccountId,
  getOrCreateWalkInCustomerAccountId,
  WALK_IN_CUSTOMER_NAME,
} from "@/lib/ledger";
import { getOrCreateDefaultCashAccountId, sortAccountsForPicker, type PaymentMethod } from "@/lib/accounts";
import { makeSyncEvent } from "@/lib/syncEvents";
import { newUid } from "@/lib/uid";
import { buildSupplierNameOptions } from "@/lib/supplierOptions";

/** Digits + one dot; strips meaningless leading zeros on the whole part (keeps 0.5). */
function normalizeSaleQtyInput(raw: string): string {
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

function parseSaleQuantityInput(s: string): number | null {
  const t = s.trim();
  if (t === "" || t === ".") return null;
  const n = Number.parseFloat(t);
  if (!Number.isFinite(n)) return null;
  return n;
}

export default function OrdersPage() {
  const searchParams = useSearchParams();
  const inventory = useLiveQuery(() => db.inventory.toArray()) || [];
  const sales = useLiveQuery(() => db.sales.toArray()) || [];
  const purchases = useLiveQuery(() => db.purchases.toArray()) || [];
  const financialAccounts = useLiveQuery(() => db.financialAccounts.toArray()) || [];
  const ledgerCustomers = useLiveQuery(() => db.ledgerAccounts.where("type").equals("Customer").toArray()) || [];
  const ledgerSuppliers = useLiveQuery(() => db.ledgerAccounts.where("type").equals("Supplier").toArray()) || [];

  const customerOptions = useMemo(() => {
    const names = new Set<string>();
    for (const a of ledgerCustomers) {
      if (a.name?.trim()) names.add(a.name.trim());
    }
    for (const s of sales) {
      const n = s.customerName?.trim();
      if (n) names.add(n);
    }
    names.add(WALK_IN_CUSTOMER_NAME);
    const rest = Array.from(names)
      .filter((n) => n !== WALK_IN_CUSTOMER_NAME)
      .sort((a, b) => a.localeCompare(b));
    return [WALK_IN_CUSTOMER_NAME, ...rest];
  }, [ledgerCustomers, sales]);

  const supplierOptions = useMemo(
    () => buildSupplierNameOptions(ledgerSuppliers, purchases),
    [ledgerSuppliers, purchases]
  );
  
  const [activeTab, setActiveTab] = useState<"Sales" | "Purchases">("Sales");
  const [showForm, setShowForm] = useState(false);

  useEffect(() => {
    const tab = searchParams.get("tab");
    const itemId = Number(searchParams.get("itemId") ?? 0);
    if (tab === "Sales" || tab === "sales") setActiveTab("Sales");
    if (itemId > 0) {
      setSaleForm((s) => ({ ...s, itemId }));
      setShowForm(true);
      setSaleUnitPriceStr("");
    }
  }, [searchParams]);
  
  const [saleQuantityStr, setSaleQuantityStr] = useState("1");
  const [saleUnitPriceStr, setSaleUnitPriceStr] = useState("");
  const [saleForm, setSaleForm] = useState<{
    itemId: number;
    customerName: string;
    paymentType: "Cash" | "Credit";
    method: PaymentMethod;
    financialAccountId: number;
  }>({
    itemId: 0,
    customerName: WALK_IN_CUSTOMER_NAME,
    paymentType: "Cash",
    method: "Cash",
    financialAccountId: 0,
  });
  const [purchaseForm, setPurchaseForm] = useState({
    supplierName: "",
    date: new Date().toISOString().slice(0, 10), // YYYY-MM-DD
    paymentType: "Credit" as "Cash" | "Credit",
    method: "Cash" as PaymentMethod,
    financialAccountId: 0,
    lineItemDraft: { itemId: 0, quantity: 1 },
    lineItems: [] as Array<{ itemId: number; quantity: number }>,
  });

  const saleTotal = useMemo(() => {
    const item = inventory.find((i) => i.id === Number(saleForm.itemId));
    const qty = parseSaleQuantityInput(saleQuantityStr);
    const unit = parseSaleQuantityInput(saleUnitPriceStr.trim());
    if (!item || qty == null || qty <= 0 || unit == null || unit <= 0) return 0;
    return unit * qty;
  }, [inventory, saleForm.itemId, saleQuantityStr, saleUnitPriceStr]);

  const purchaseTotal = useMemo(() => {
    return purchaseForm.lineItems.reduce((acc, li) => {
      const item = inventory.find((i) => i.id === li.itemId);
      const unitCost = item?.costPrice ?? 0;
      return acc + unitCost * li.quantity;
    }, 0);
  }, [inventory, purchaseForm.lineItems]);

  const addPurchaseLineItem = () => {
    const itemId = Number(purchaseForm.lineItemDraft.itemId);
    const quantity = Number(purchaseForm.lineItemDraft.quantity);
    if (!itemId || quantity <= 0) return;

    setPurchaseForm((prev) => {
      const existingIdx = prev.lineItems.findIndex((li) => li.itemId === itemId);
      const nextLineItems = [...prev.lineItems];
      if (existingIdx >= 0) {
        nextLineItems[existingIdx] = {
          itemId,
          quantity: nextLineItems[existingIdx].quantity + quantity,
        };
      } else {
        nextLineItems.push({ itemId, quantity });
      }
      return { ...prev, lineItems: nextLineItems, lineItemDraft: { itemId: 0, quantity: 1 } };
    });
  };

  const removePurchaseLineItem = (itemId: number) => {
    setPurchaseForm((prev) => ({
      ...prev,
      lineItems: prev.lineItems.filter((li) => li.itemId !== itemId),
    }));
  };

  const handleSaleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    const qtyParsed = parseSaleQuantityInput(saleQuantityStr);
    if (qtyParsed == null) {
      return alert("Enter a quantity.");
    }
    if (qtyParsed <= 0) {
      return alert("Quantity must be greater than 0.");
    }
    const item = inventory.find(i => i.id === Number(saleForm.itemId));
    if (!item || item.quantity < qtyParsed) return alert("Not enough stock!");

    const unitPrice = parseSaleQuantityInput(saleUnitPriceStr.trim());
    if (unitPrice == null || unitPrice <= 0) {
      return alert("Enter a valid selling price per unit (greater than 0).");
    }
    const totalPrice = unitPrice * qtyParsed;
    const date = new Date().toISOString();

    const isCredit = saleForm.paymentType === "Credit";
    const customerNameResolved =
      saleForm.customerName.trim() || WALK_IN_CUSTOMER_NAME;
    if (isCredit && !customerNameResolved.trim()) {
      return alert("Customer name is required for Credit sales (for ledger).");
    }

    await db.transaction('rw', db.tables, async () => {
      if (!isCredit && customerNameResolved === WALK_IN_CUSTOMER_NAME) {
        await getOrCreateWalkInCustomerAccountId();
      }

      // 1. Record Sale
      const sale = {
        uid: newUid(),
        itemId: item.id!,
        quantity: qtyParsed,
        totalPrice,
        unitPrice,
        customerName: customerNameResolved,
        paymentType: saleForm.paymentType,
        date,
        paidAmount: isCredit ? 0 : totalPrice,
        dueAmount: isCredit ? totalPrice : 0,
        paymentStatus: (isCredit ? "due" : "paid") as PaymentStatusErp,
      };
      const saleId = await db.sales.add(sale);

      // 2. Reduce Stock
      await db.inventory.update(item.id!, { quantity: item.quantity - qtyParsed });
      // 3. Record Movement
      const movement = { uid: newUid(), itemId: item.id!, quantity: qtyParsed, type: 'OUT' as const, reason: 'Sale' as const, date };
      const movementId = await db.stockMovement.add(movement);
      // 4. DayBook Entry (cash only)
      let dayBookId: number | null = null;
      let dayBookUid: string | null = null;
      let dayBookEntry: any = null;
      if (!isCredit) {
        const accountId = Number(saleForm.financialAccountId) || (await getOrCreateDefaultCashAccountId());
        const acct = await db.financialAccounts.get(accountId);
        dayBookUid = newUid();
        dayBookEntry = {
          uid: dayBookUid,
          time: date,
          type: "Income",
          category: "Sale",
          amount: totalPrice,
          description: `Sold ${qtyParsed} ${item.unit} ${item.name} @ Rs.${unitPrice}/${item.unit} (${saleForm.method})`,
          method: saleForm.method,
          accountId,
        };
        dayBookId = (await db.dayBook.add(dayBookEntry)) as number;

        await db.outbox.add(
          makeSyncEvent({
            entityType: "daybook.entry",
            entityId: dayBookUid,
            op: "create",
            payload: {
              entry: {
                ...dayBookEntry,
                account: acct?.uid
                  ? { uid: acct.uid, name: acct.name, type: acct.type }
                  : null,
              },
            },
          })
        );
      }

      // 5. Ledger entry (credit only)
      let ledgerAccountId: number | null = null;
      let ledgerEntryId: number | null = null;
      let ledgerAccount: any = null;
      let ledgerEntry: any = null;
      if (isCredit) {
        ledgerAccountId = await getOrCreateLedgerAccountId({ name: customerNameResolved, type: "Customer" });
        const acct = await db.ledgerAccounts.get(ledgerAccountId);
        ledgerAccount = acct?.uid ? { uid: acct.uid, name: acct.name, type: acct.type } : null;
        ledgerEntryId = (await addLedgerEntry({
          accountId: ledgerAccountId,
          date,
          description: `Credit sale: ${qtyParsed} ${item.unit} ${item.name} @ Rs.${unitPrice}/${item.unit}`,
          debit: totalPrice,
          credit: 0,
        })) as number;
        const entryRow = await db.ledgerEntries.get(ledgerEntryId);
        ledgerEntry = entryRow?.uid
          ? {
              uid: entryRow.uid,
              date: entryRow.date,
              description: entryRow.description,
              debit: entryRow.debit,
              credit: entryRow.credit,
            }
          : null;

        if (ledgerAccount?.uid && ledgerEntry?.uid) {
          await db.outbox.add(
            makeSyncEvent({
              entityType: "ledger.entry",
              entityId: ledgerEntry.uid,
              op: "create",
              payload: { account: ledgerAccount, entry: ledgerEntry },
            })
          );
        }
      }

      await db.outbox.add(
        makeSyncEvent({
          entityType: "order.sale",
          entityId: sale.uid!,
          op: "create",
          payload: {
            sale: { ...sale, itemUid: item.uid },
            movement: { ...movement, itemUid: item.uid },
            inventoryDelta: { itemUid: item.uid, delta: -qtyParsed },
            dayBookUid,
            ledgerEntryUid: ledgerEntry?.uid ?? null,
            ledgerAccountUid: ledgerAccount?.uid ?? null,
          },
        })
      );
    });

    setShowForm(false);
    setSaleQuantityStr("1");
    setSaleUnitPriceStr("");
    setSaleForm({
      itemId: 0,
      customerName: WALK_IN_CUSTOMER_NAME,
      paymentType: "Cash",
      method: "Cash",
      financialAccountId: 0,
    });
  };

  const handlePurchaseSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!purchaseForm.supplierName.trim()) return alert("Supplier name is required.");
    if (purchaseForm.lineItems.length === 0) return alert("Add at least one item to purchase.");

    const date = new Date(`${purchaseForm.date}T12:00:00`).toISOString();
    const supplierName = purchaseForm.supplierName.trim();

    const lineItemsResolved = purchaseForm.lineItems.map((li) => {
      const item = inventory.find((i) => i.id === li.itemId);
      return { ...li, item };
    });

    if (lineItemsResolved.some((li) => !li.item)) return alert("One or more selected items were not found.");

    const totalCost = lineItemsResolved.reduce((acc, li) => acc + (li.item!.costPrice * li.quantity), 0);
    const description = `Purchase from ${supplierName} (${purchaseForm.lineItems.length} item(s))`;

    await db.transaction('rw', db.tables, async () => {
      const purchaseBatchUid = newUid();
      const purchaseRows: any[] = [];
      const movementRows: any[] = [];
      const inventoryDeltas: Array<{ itemUid: string; delta: number }> = [];

      for (const li of lineItemsResolved) {
        const item = li.item!;
        if (!item.uid) throw new Error("Inventory item missing uid (sync requires uid)");
        const lineCost = item.costPrice * li.quantity;

        const purchase = {
          uid: newUid(),
          supplierName,
          itemId: item.id!,
          quantity: li.quantity,
          totalCost: lineCost,
          date,
        };
        const pid = await db.purchases.add(purchase);

        await db.inventory.update(item.id!, { quantity: item.quantity + li.quantity });
        const movement = { uid: newUid(), itemId: item.id!, quantity: li.quantity, type: 'IN' as const, reason: 'Purchase' as const, date };
        const mid = await db.stockMovement.add(movement);

        purchaseRows.push({ ...purchase, itemUid: item.uid, localId: pid });
        movementRows.push({ ...movement, itemUid: item.uid, localId: mid });
        inventoryDeltas.push({ itemUid: item.uid, delta: li.quantity });
      }

      // Purchase accounting:
      // - Cash: affects Day Book (selected account) and does NOT create payable.
      // - Credit: creates payable in ledger and does NOT affect Day Book.
      let dayBookId: number | null = null;
      let dayBookUid: string | null = null;
      let dayBookEntry: any = null;
      let ledgerAccountId: number | null = null;
      let ledgerEntryId: number | null = null;
      let ledgerAccount: any = null;
      let ledgerEntry: any = null;
      if (purchaseForm.paymentType === "Cash") {
        const accountId = Number(purchaseForm.financialAccountId) || (await getOrCreateDefaultCashAccountId());
        const acct = await db.financialAccounts.get(accountId);
        dayBookUid = newUid();
        dayBookEntry = {
          uid: dayBookUid,
          time: date,
          type: "Expense",
          category: "Purchase",
          amount: totalCost,
          description: `${description} (${purchaseForm.method})`,
          method: purchaseForm.method,
          accountId,
        };
        dayBookId = (await db.dayBook.add(dayBookEntry)) as number;

        await db.outbox.add(
          makeSyncEvent({
            entityType: "daybook.entry",
            entityId: dayBookUid,
            op: "create",
            payload: {
              entry: {
                ...dayBookEntry,
                account: acct?.uid
                  ? { uid: acct.uid, name: acct.name, type: acct.type }
                  : null,
              },
            },
          })
        );
      } else {
        ledgerAccountId = await getOrCreateLedgerAccountId({ name: supplierName, type: "Supplier" });
        ledgerEntryId = (await addLedgerEntry({ accountId: ledgerAccountId, date, description, debit: 0, credit: totalCost })) as number;
        const acct = await db.ledgerAccounts.get(ledgerAccountId);
        ledgerAccount = acct?.uid ? { uid: acct.uid, name: acct.name, type: acct.type } : null;
        const entryRow = await db.ledgerEntries.get(ledgerEntryId);
        ledgerEntry = entryRow?.uid
          ? {
              uid: entryRow.uid,
              date: entryRow.date,
              description: entryRow.description,
              debit: entryRow.debit,
              credit: entryRow.credit,
            }
          : null;

        if (ledgerAccount?.uid && ledgerEntry?.uid) {
          await db.outbox.add(
            makeSyncEvent({
              entityType: "ledger.entry",
              entityId: ledgerEntry.uid,
              op: "create",
              payload: { account: ledgerAccount, entry: ledgerEntry },
            })
          );
        }
      }

      await db.outbox.add(
        makeSyncEvent({
          entityType: "order.purchase",
          entityId: purchaseBatchUid,
          op: "create",
          payload: {
            purchaseBatchUid,
            supplierName,
            date,
            paymentType: purchaseForm.paymentType,
            method: purchaseForm.method,
            totalCost,
            purchases: purchaseRows.map(({ localId, ...p }) => p),
            movements: movementRows.map(({ localId, ...m }) => m),
            inventoryDeltas,
            dayBookUid,
            ledgerEntryUid: ledgerEntry?.uid ?? null,
            ledgerAccountUid: ledgerAccount?.uid ?? null,
          },
        })
      );
    });

    setShowForm(false);
    setPurchaseForm({
      supplierName: "",
      date: new Date().toISOString().slice(0, 10),
      paymentType: "Credit",
      method: "Cash",
      financialAccountId: 0,
      lineItemDraft: { itemId: 0, quantity: 1 },
      lineItems: [],
    });
  };

  return (
    <div className="space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <h1 className="text-2xl font-semibold text-slate-900">Order Management</h1>
        <div className="flex bg-slate-100 p-1 rounded-lg">
          <button onClick={() => setActiveTab('Sales')} className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${activeTab === 'Sales' ? 'bg-white shadow-sm text-slate-900' : 'text-slate-500 hover:text-slate-700'}`}>Sales (POS)</button>
          <button onClick={() => setActiveTab('Purchases')} className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${activeTab === 'Purchases' ? 'bg-white shadow-sm text-slate-900' : 'text-slate-500 hover:text-slate-700'}`}>Purchases</button>
        </div>
        <button 
          onClick={() => setShowForm(!showForm)}
          className="flex items-center justify-center gap-2 px-4 py-2 bg-primary text-white rounded-lg hover:bg-primary/90 transition-colors"
        >
          <Plus className="w-5 h-5" />
          <span>{showForm ? "Cancel" : activeTab === 'Sales' ? "New Sale" : "New Purchase"}</span>
        </button>
      </div>

      {showForm && activeTab === 'Sales' && (
        <form onSubmit={handleSaleSubmit} className="bg-white p-6 rounded-xl shadow-sm border border-slate-100 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
          <div>
            <label className="block text-sm font-medium mb-1">Select Item</label>
            <select
              required
              value={saleForm.itemId}
              onChange={(e) => {
                const id = Number(e.target.value);
                setSaleForm({ ...saleForm, itemId: id });
                setSaleUnitPriceStr("");
              }}
              className="w-full px-3 py-2 border rounded-md bg-white"
            >
              <option value={0}>Select...</option>
              {inventory.map(i => <option key={i.id} value={i.id}>{i.name} ({i.quantity} {i.unit} left)</option>)}
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Quantity</label>
            <input
              type="text"
              inputMode="decimal"
              autoComplete="off"
              value={saleQuantityStr}
              onChange={(e) => setSaleQuantityStr(normalizeSaleQtyInput(e.target.value))}
              className="w-full px-3 py-2 border rounded-md"
              placeholder="e.g. 29.8"
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Selling price (per unit)</label>
            <input
              type="text"
              inputMode="decimal"
              autoComplete="off"
              value={saleUnitPriceStr}
              onChange={(e) => setSaleUnitPriceStr(normalizeSaleQtyInput(e.target.value))}
              className="w-full px-3 py-2 border rounded-md"
              placeholder="Enter price per unit"
              title="Retail rate for this sale (cash or credit)"
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Customer</label>
            <select
              value={saleForm.customerName}
              onChange={(e) => setSaleForm({ ...saleForm, customerName: e.target.value })}
              className="w-full px-3 py-2 border rounded-md bg-white"
            >
              {customerOptions.map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Payment Type</label>
            <select value={saleForm.paymentType} onChange={e => setSaleForm({...saleForm, paymentType: e.target.value as "Cash"|"Credit"})} className="w-full px-3 py-2 border rounded-md bg-white">
              <option value="Cash">Cash</option>
              <option value="Credit">Credit (Update Ledger)</option>
            </select>
          </div>
          {saleForm.paymentType === "Cash" ? (
            <>
              <div>
                <label className="block text-sm font-medium mb-1">Payment Mode</label>
                <select
                  value={saleForm.method}
                  onChange={(e) => setSaleForm({ ...saleForm, method: e.target.value as PaymentMethod })}
                  className="w-full px-3 py-2 border rounded-md bg-white"
                >
                  <option value="Cash">Cash</option>
                  <option value="QR">QR</option>
                  <option value="BankTransfer">Bank Transfer</option>
                </select>
              </div>
              <div>
                <label className="block text-sm font-medium mb-1">Account</label>
                <select
                  value={saleForm.financialAccountId}
                  onChange={(e) => setSaleForm({ ...saleForm, financialAccountId: Number(e.target.value) })}
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
          <div className="sm:col-span-2 lg:col-span-4 flex justify-between items-center mt-2 border-t pt-4">
            <div className="text-lg font-semibold text-slate-900">
              Total: Rs. {saleTotal}
            </div>
            <button type="submit" className="px-6 py-2 bg-alert-green text-white rounded-md hover:bg-alert-green/90">Complete Sale</button>
          </div>
        </form>
      )}

      {showForm && activeTab === 'Purchases' && (
        <form onSubmit={handlePurchaseSubmit} className="bg-white p-6 rounded-xl shadow-sm border border-slate-100 space-y-4">
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            <div>
              <label className="block text-sm font-medium mb-1">Supplier</label>
              <input
                required
                type="text"
                list="orders-purchase-supplier-names"
                autoComplete="off"
                value={purchaseForm.supplierName}
                onChange={(e) => setPurchaseForm({ ...purchaseForm, supplierName: e.target.value })}
                className="w-full px-3 py-2 border rounded-md bg-white"
                placeholder="Pick from list or type a new supplier"
              />
              <datalist id="orders-purchase-supplier-names">
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
            <div>
              <label className="block text-sm font-medium mb-1">Payment Type</label>
              <select
                value={purchaseForm.paymentType}
                onChange={(e) => setPurchaseForm({ ...purchaseForm, paymentType: e.target.value as "Cash" | "Credit" })}
                className="w-full px-3 py-2 border rounded-md bg-white"
              >
                <option value="Credit">Credit (Payable in Ledger)</option>
                <option value="Cash">Paid Now (Affects Account)</option>
              </select>
            </div>
          </div>

          {purchaseForm.paymentType === "Cash" ? (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
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
              <div className="sm:col-span-2">
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
            </div>
          ) : null}

          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 items-end">
            <div>
              <label className="block text-sm font-medium mb-1">Select Item</label>
              <select
                value={purchaseForm.lineItemDraft.itemId}
                onChange={(e) =>
                  setPurchaseForm({
                    ...purchaseForm,
                    lineItemDraft: { ...purchaseForm.lineItemDraft, itemId: Number(e.target.value) },
                  })
                }
                className="w-full px-3 py-2 border rounded-md bg-white"
              >
                <option value={0}>Select...</option>
                {inventory.map((i) => (
                  <option key={i.id} value={i.id}>
                    {i.name} (Current: {i.quantity} {i.unit})
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Quantity</label>
              <input
                type="number"
                min={0}
                step="any"
                value={purchaseForm.lineItemDraft.quantity}
                onChange={(e) =>
                  setPurchaseForm({
                    ...purchaseForm,
                    lineItemDraft: { ...purchaseForm.lineItemDraft, quantity: Number(e.target.value) },
                  })
                }
                className="w-full px-3 py-2 border rounded-md"
              />
            </div>
            <div className="flex gap-2">
              <button
                type="button"
                onClick={addPurchaseLineItem}
                className="w-full px-4 py-2 bg-secondary text-white rounded-md hover:bg-secondary/90"
              >
                Add Item
              </button>
            </div>
          </div>

          <div className="border rounded-lg overflow-hidden">
            <div className="bg-slate-50 px-4 py-2 text-sm font-medium text-slate-700">Items Purchased</div>
            {purchaseForm.lineItems.length === 0 ? (
              <div className="px-4 py-6 text-sm text-slate-500">No items added yet.</div>
            ) : (
              <table className="min-w-full divide-y divide-slate-200">
                <thead className="bg-white">
                  <tr>
                    <th className="px-4 py-2 text-left text-xs font-medium text-slate-500 uppercase">Item</th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-slate-500 uppercase">Qty</th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-slate-500 uppercase">Line Cost</th>
                    <th className="px-4 py-2 text-right text-xs font-medium text-slate-500 uppercase">Remove</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-slate-200">
                  {purchaseForm.lineItems.map((li) => {
                    const item = inventory.find((i) => i.id === li.itemId);
                    const lineCost = (item?.costPrice ?? 0) * li.quantity;
                    return (
                      <tr key={li.itemId}>
                        <td className="px-4 py-2 text-sm text-slate-900">{item?.name ?? "Unknown"}</td>
                        <td className="px-4 py-2 text-sm text-slate-900">{li.quantity}</td>
                        <td className="px-4 py-2 text-sm text-slate-900">Rs. {lineCost}</td>
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

          <div className="flex justify-between items-center border-t pt-4">
            <div className="text-lg font-semibold text-slate-900">Total Cost: Rs. {purchaseTotal}</div>
            <button type="submit" className="px-6 py-2 bg-primary text-white rounded-md hover:bg-primary/90">
              Complete Purchase
            </button>
          </div>
        </form>
      )}

      {/* History Tables */}
      <div className="bg-white rounded-xl shadow-sm border border-slate-100 overflow-hidden">
        {activeTab === 'Sales' ? (
          sales.length === 0 ? (
            <div className="p-8 text-center text-slate-500">
               <ShoppingCart className="w-12 h-12 mx-auto mb-4 text-slate-300" />
               <p className="text-lg font-medium text-slate-900">No sales recorded</p>
            </div>
          ) : (
            <table className="min-w-full divide-y divide-slate-200">
              <thead className="bg-slate-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase">Date</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase">Customer</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase">Item</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase">Amount</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-slate-200">
                {sales.sort((a,b) => new Date(b.date).getTime() - new Date(a.date).getTime()).map(sale => {
                  const item = inventory.find(i => i.id === sale.itemId);
                  return (
                    <tr key={sale.id}>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-500">{new Date(sale.date).toLocaleDateString()}</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-900">
                        {sale.customerName?.trim() || WALK_IN_CUSTOMER_NAME} ({sale.paymentType})
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-900">
                        {sale.quantity}x {item?.name || "Unknown"}
                        <span className="block text-xs text-slate-500 mt-0.5">
                          @ Rs.{" "}
                          {(sale.unitPrice ??
                            (sale.quantity ? sale.totalPrice / sale.quantity : 0)
                          ).toLocaleString(undefined, { maximumFractionDigits: 4 })}
                          {item?.unit ? `/${item.unit}` : ""}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-alert-green">+ Rs. {sale.totalPrice}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          )
        ) : (
          purchases.length === 0 ? (
            <div className="p-8 text-center text-slate-500">
               <ShoppingCart className="w-12 h-12 mx-auto mb-4 text-slate-300" />
               <p className="text-lg font-medium text-slate-900">No purchases recorded</p>
            </div>
          ) : (
            <table className="min-w-full divide-y divide-slate-200">
              <thead className="bg-slate-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase">Date</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase">Supplier</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase">Item</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase">Cost</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-slate-200">
                {purchases.sort((a,b) => new Date(b.date).getTime() - new Date(a.date).getTime()).map(purchase => {
                  const item = inventory.find(i => i.id === purchase.itemId);
                  return (
                    <tr key={purchase.id}>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-500">{new Date(purchase.date).toLocaleDateString()}</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-900">{purchase.supplierName}</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-900">{purchase.quantity}x {item?.name || 'Unknown'}</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-slate-900">- Rs. {purchase.totalCost}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          )
        )}
      </div>
    </div>
  );
}
