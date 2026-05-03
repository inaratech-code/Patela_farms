"use client";

import Link from "next/link";
import { PackagePlus, Trash2, ShoppingCart, AlertTriangle, Soup } from "lucide-react";
import { useState } from "react";
import { useLiveQuery } from "dexie-react-hooks";
import { db, type ItemTypeErp } from "@/lib/db";
import { commonUnits } from "@/lib/units";
import { makeSyncEvent } from "@/lib/syncEvents";
import { newUid } from "@/lib/uid";
import { isConsumable, isSellable, resolveItemType } from "@/lib/erp/items";

export default function InventoryPage() {
  const items = useLiveQuery(() => db.inventory.toArray());
  const [showForm, setShowForm] = useState(false);
  
  // New Item Form
  const [formData, setFormData] = useState({
    name: "",
    sku: "",
    itemType: "sellable" as ItemTypeErp,
    quantity: "",
    unit: "kg",
    costPrice: "",
    sellingPrice: "",
    minStockThreshold: "",
    expiryDate: "",
  });
  const [unitMode, setUnitMode] = useState<"preset" | "custom">("preset");
  const [customUnit, setCustomUnit] = useState("");

  const normalizeMoney = (raw: string) => {
    // digits only, remove leading zeros (keep single zero)
    const digits = raw.replace(/[^\d]/g, "");
    return digits.replace(/^0+(?=\d)/, "");
  };

  const normalizeInt = (raw: string) => {
    const digits = raw.replace(/[^\d]/g, "");
    return digits.replace(/^0+(?=\d)/, "");
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    const reorder = Number(formData.minStockThreshold || 0);
    const cost = Number(formData.costPrice || 0);
    const item = {
      uid: newUid(),
      name: formData.name,
      sku: formData.sku.trim() || undefined,
      itemType: formData.itemType,
      active: true as const,
      unit: unitMode === "custom" ? (customUnit.trim() || formData.unit) : formData.unit,
      quantity: Number(formData.quantity || 0),
      costPrice: cost,
      sellingPrice: Number(formData.sellingPrice || 0),
      minStockThreshold: reorder,
      reorderLevel: reorder,
      avgCost: cost,
      expiryDate: formData.expiryDate || undefined,
    };

    await db.transaction("rw", db.tables, async () => {
      const id = await db.inventory.add(item);
      await db.outbox.add(
        makeSyncEvent({
          entityType: "inventory.item",
          entityId: item.uid!,
          op: "create",
          payload: { id, item },
        })
      );
    });
    setShowForm(false);
    setFormData({
      name: "",
      sku: "",
      itemType: "sellable",
      quantity: "",
      unit: "kg",
      costPrice: "",
      sellingPrice: "",
      minStockThreshold: "",
      expiryDate: "",
    });
    setUnitMode("preset");
    setCustomUnit("");
  };

  const getStockStatus = (quantity: number, threshold: number) => {
    if (quantity <= 0) return { label: "Out of stock", classes: "bg-alert-red/10 text-alert-red" };
    if (threshold > 0 && quantity <= threshold) return { label: "Low stock", classes: "bg-alert-yellow/10 text-alert-yellow" };
    return { label: "In stock", classes: "bg-alert-green/10 text-alert-green" };
  };

  const typeBadge = (t: ItemTypeErp) => {
    if (t === "sellable") return { label: "Sellable", classes: "bg-emerald-500/15 text-emerald-800" };
    if (t === "consumable") return { label: "Consumable", classes: "bg-orange-500/15 text-orange-900" };
    return { label: "Equipment", classes: "bg-slate-500/10 text-slate-700" };
  };

  const actionClass =
    "inline-flex items-center justify-center gap-1 rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-50";

  return (
    <div className="space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <h1 className="text-2xl font-semibold text-slate-900">Inventory Management</h1>
        <button 
          onClick={() => setShowForm(!showForm)}
          className="flex items-center justify-center gap-2 px-4 py-2 bg-primary text-white rounded-lg hover:bg-primary/90 transition-colors"
        >
          <PackagePlus className="w-5 h-5" />
          <span>{showForm ? "Cancel" : "Add New Item"}</span>
        </button>
      </div>

      {showForm && (
        <form onSubmit={handleSubmit} className="bg-white p-6 rounded-xl shadow-sm border border-slate-100 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
          <div>
            <label className="block text-sm font-medium mb-1">Item Name</label>
            <input required type="text" value={formData.name} onChange={e => setFormData({...formData, name: e.target.value})} className="w-full px-3 py-2 border rounded-md" />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">SKU (optional)</label>
            <input type="text" value={formData.sku} onChange={(e) => setFormData({ ...formData, sku: e.target.value })} className="w-full px-3 py-2 border rounded-md" placeholder="e.g. FISH-001" />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Item type</label>
            <select
              value={formData.itemType}
              onChange={(e) => setFormData({ ...formData, itemType: e.target.value as ItemTypeErp })}
              className="w-full px-3 py-2 border rounded-md bg-white"
            >
              <option value="sellable">Sellable (fish / chicken)</option>
              <option value="consumable">Consumable (feed)</option>
              <option value="equipment">Equipment</option>
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Opening Quantity</label>
            <input
              required
              inputMode="numeric"
              type="text"
              value={formData.quantity}
              onChange={(e) => setFormData({ ...formData, quantity: normalizeInt(e.target.value) })}
              className="w-full px-3 py-2 border rounded-md"
              placeholder="e.g. 50"
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Unit</label>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
              <select
                value={unitMode === "custom" ? "__custom__" : formData.unit}
                onChange={(e) => {
                  const v = e.target.value;
                  if (v === "__custom__") {
                    setUnitMode("custom");
                    setCustomUnit("");
                  } else {
                    setUnitMode("preset");
                    setFormData({ ...formData, unit: v });
                  }
                }}
                className="w-full px-3 py-2 border rounded-md bg-white"
              >
                {commonUnits.map((u) => (
                  <option key={u} value={u}>
                    {u}
                  </option>
                ))}
                <option value="__custom__">Other…</option>
              </select>

              {unitMode === "custom" ? (
                <input
                  required
                  type="text"
                  value={customUnit}
                  onChange={(e) => setCustomUnit(e.target.value)}
                  className="w-full px-3 py-2 border rounded-md"
                  placeholder="Type unit (e.g. ton, meter)"
                />
              ) : null}
            </div>
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Cost Price (per unit)</label>
            <input
              required
              inputMode="numeric"
              type="text"
              value={formData.costPrice}
              onChange={(e) => setFormData({ ...formData, costPrice: normalizeMoney(e.target.value) })}
              className="w-full px-3 py-2 border rounded-md"
              placeholder="e.g. 800"
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Selling price (optional)</label>
            <input
              inputMode="numeric"
              type="text"
              value={formData.sellingPrice}
              onChange={(e) => setFormData({ ...formData, sellingPrice: normalizeMoney(e.target.value) })}
              className="w-full px-3 py-2 border rounded-md"
              placeholder="POS can override"
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Expiry Date (Optional)</label>
            <input type="date" value={formData.expiryDate} onChange={e => setFormData({...formData, expiryDate: e.target.value})} className="w-full px-3 py-2 border rounded-md" />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Reorder / low stock at</label>
            <input
              required
              inputMode="numeric"
              type="text"
              value={formData.minStockThreshold}
              onChange={(e) => setFormData({ ...formData, minStockThreshold: normalizeInt(e.target.value) })}
              className="w-full px-3 py-2 border rounded-md"
              placeholder="e.g. 10"
            />
          </div>
          <div className="sm:col-span-2 lg:col-span-4 flex justify-end mt-2">
            <button type="submit" className="px-6 py-2 bg-secondary text-white rounded-md hover:bg-secondary/90">Save Item</button>
          </div>
        </form>
      )}

      {!items || items.length === 0 ? (
        <div className="bg-white rounded-xl shadow-sm border border-slate-100 overflow-hidden">
          <div className="p-8 text-center text-slate-500">
            <PackagePlus className="w-12 h-12 mx-auto mb-4 text-slate-300" />
            <p className="text-lg font-medium text-slate-900">No items found</p>
            <p className="mt-1">Add items to start tracking your farm inventory.</p>
          </div>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
          {items.map((item) => {
            const t = resolveItemType(item);
            const tb = typeBadge(t);
            const th = item.reorderLevel ?? item.minStockThreshold ?? 0;
            const status = getStockStatus(item.quantity, th);
            const unitCost = Number(item.avgCost ?? item.costPrice ?? 0);
            const listCost = Number(item.costPrice ?? 0);
            const showBothCosts = Math.abs(unitCost - listCost) >= 0.01;
            const lineValue = Number(item.quantity ?? 0) * unitCost;
            return (
              <article
                key={item.id}
                className="flex flex-col rounded-xl border border-slate-200 bg-white p-5 shadow-sm hover:border-primary/25 hover:shadow-md transition-shadow"
              >
                <header className="flex flex-wrap items-start justify-between gap-2 border-b border-slate-100 pb-3">
                  <div className="min-w-0">
                    <h2 className="text-base font-semibold text-slate-900 leading-tight">{item.name}</h2>
                    {item.sku ? (
                      <p className="mt-1 text-xs font-medium uppercase tracking-wide text-slate-500">
                        SKU <span className="font-mono normal-case text-slate-700">{item.sku}</span>
                      </p>
                    ) : (
                      <p className="mt-1 text-xs text-slate-400">No SKU</p>
                    )}
                  </div>
                  <div className="flex shrink-0 flex-col items-end gap-1">
                    <span className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold ${tb.classes}`}>{tb.label}</span>
                    {item.active === false ? (
                      <span className="rounded-full bg-slate-200 px-2 py-0.5 text-[10px] font-semibold uppercase text-slate-600">Inactive</span>
                    ) : null}
                  </div>
                </header>

                <dl className="mt-4 grid grid-cols-2 gap-x-3 gap-y-3 text-sm">
                  <div className="col-span-2">
                    <dt className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Stock on hand</dt>
                    <dd className="mt-0.5 flex flex-wrap items-center gap-2">
                      <span className="text-lg font-bold tabular-nums text-slate-900">
                        {item.quantity} {item.unit}
                      </span>
                      <span className={`inline-flex items-center rounded-md px-2 py-0.5 text-xs font-medium ${status.classes}`}>{status.label}</span>
                    </dd>
                  </div>
                  <div>
                    <dt className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Avg cost</dt>
                    <dd className="mt-0.5 font-semibold tabular-nums text-slate-900">Rs. {unitCost.toLocaleString()}</dd>
                    <dd className="text-xs text-slate-400">per {item.unit}</dd>
                  </div>
                  {showBothCosts ? (
                    <div>
                      <dt className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">List cost</dt>
                      <dd className="mt-0.5 font-medium tabular-nums text-slate-700">Rs. {listCost.toLocaleString()}</dd>
                      <dd className="text-xs text-slate-400">per {item.unit}</dd>
                    </div>
                  ) : null}
                  <div>
                    <dt className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Selling price</dt>
                    <dd className="mt-0.5 font-semibold tabular-nums text-slate-900">Rs. {Number(item.sellingPrice ?? 0).toLocaleString()}</dd>
                    <dd className="text-xs text-slate-400">per {item.unit}</dd>
                  </div>
                  <div>
                    <dt className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Reorder at</dt>
                    <dd className="mt-0.5 font-medium tabular-nums text-slate-900">{th || "—"}</dd>
                    <dd className="text-xs text-slate-400">{item.unit}</dd>
                  </div>
                  <div className="col-span-2">
                    <dt className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Stock value (qty × avg)</dt>
                    <dd className="mt-0.5 text-sm font-semibold tabular-nums text-slate-900">Rs. {lineValue.toLocaleString()}</dd>
                  </div>
                  <div className="col-span-2">
                    <dt className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Expiry</dt>
                    <dd className="mt-0.5 text-slate-800">{item.expiryDate ? item.expiryDate : "—"}</dd>
                  </div>
                </dl>

                <footer className="mt-auto border-t border-slate-100 pt-4">
                  <div className="flex flex-wrap gap-1.5">
                    {isSellable(item) && item.id ? (
                      <Link href={`/orders?tab=Sales&itemId=${item.id}`} className={actionClass}>
                        <ShoppingCart className="w-3.5 h-3.5 shrink-0" aria-hidden />
                        Sell
                      </Link>
                    ) : null}
                    {isConsumable(item) && item.id ? (
                      <Link href={`/consumption?itemId=${item.id}`} className={actionClass}>
                        <Soup className="w-3.5 h-3.5 shrink-0" aria-hidden />
                        Use stock
                      </Link>
                    ) : null}
                    {item.id ? (
                      <Link href={`/stock-movement?itemId=${item.id}`} className={actionClass}>
                        Adjust
                      </Link>
                    ) : null}
                    {isSellable(item) && item.id ? (
                      <Link href={`/loss-wastage?itemId=${item.id}`} className={actionClass}>
                        <AlertTriangle className="w-3.5 h-3.5 shrink-0" aria-hidden />
                        Loss
                      </Link>
                    ) : null}
                    {isConsumable(item) && item.id ? (
                      <Link href={`/loss-wastage?itemId=${item.id}`} className={actionClass}>
                        Damage
                      </Link>
                    ) : null}
                    <button
                      type="button"
                      title="Delete item"
                      onClick={() => db.inventory.delete(item.id!)}
                      className="ml-auto inline-flex items-center justify-center rounded-md border border-transparent p-2 text-alert-red hover:bg-red-50"
                    >
                      <Trash2 className="w-4 h-4" />
                    </button>
                  </div>
                </footer>
              </article>
            );
          })}
        </div>
      )}
    </div>
  );
}
