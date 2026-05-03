"use client";

import { PackagePlus, Trash2, ArrowDownCircle, ArrowUpCircle } from "lucide-react";
import { useState } from "react";
import { useLiveQuery } from "dexie-react-hooks";
import { db } from "@/lib/db";
import { commonUnits } from "@/lib/units";
import { makeSyncEvent } from "@/lib/syncEvents";
import { newUid } from "@/lib/uid";

export default function InventoryPage() {
  const items = useLiveQuery(() => db.inventory.toArray());
  const [showForm, setShowForm] = useState(false);
  
  // New Item Form
  const [formData, setFormData] = useState({
    name: "",
    quantity: "",
    unit: "kg",
    costPrice: "",
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
    const item = {
      uid: newUid(),
      ...formData,
      unit: unitMode === "custom" ? (customUnit.trim() || formData.unit) : formData.unit,
      quantity: Number(formData.quantity || 0),
      costPrice: Number(formData.costPrice || 0),
      sellingPrice: 0,
      minStockThreshold: Number(formData.minStockThreshold || 0),
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
    setFormData({ name: "", quantity: "", unit: "kg", costPrice: "", minStockThreshold: "", expiryDate: "" });
    setUnitMode("preset");
    setCustomUnit("");
  };

  const getStockStatus = (quantity: number, threshold: number) => {
    if (quantity <= 0) return { label: "Out of stock", classes: "bg-alert-red/10 text-alert-red" };
    if (quantity <= threshold) return { label: "Low stock", classes: "bg-alert-yellow/10 text-alert-yellow" };
    return { label: "In stock", classes: "bg-alert-green/10 text-alert-green" };
  };

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
        <form onSubmit={handleSubmit} className="bg-white p-6 rounded-xl shadow-sm border border-slate-100 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          <div>
            <label className="block text-sm font-medium mb-1">Item Name</label>
            <input required type="text" value={formData.name} onChange={e => setFormData({...formData, name: e.target.value})} className="w-full px-3 py-2 border rounded-md" />
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
            <label className="block text-sm font-medium mb-1">Cost Price</label>
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
            <label className="block text-sm font-medium mb-1">Expiry Date (Optional)</label>
            <input type="date" value={formData.expiryDate} onChange={e => setFormData({...formData, expiryDate: e.target.value})} className="w-full px-3 py-2 border rounded-md" />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Low Stock Alert at</label>
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
        <div className="bg-white rounded-xl shadow-sm border border-slate-100 overflow-hidden">
          <table className="min-w-full divide-y divide-slate-200">
            <thead className="bg-slate-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">Item</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">Status</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">Cost</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">Expiry</th>
                <th className="px-6 py-3 text-right text-xs font-medium text-slate-500 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-slate-200">
              {items.map((item) => {
                const status = getStockStatus(item.quantity, item.minStockThreshold);
                return (
                  <tr key={item.id}>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="text-sm font-medium text-slate-900">{item.name}</div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="text-sm font-bold text-slate-900 mb-1">{item.quantity} {item.unit}</div>
                      <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${status.classes}`}>
                        {status.label}
                      </span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-500">
                      Rs. {item.costPrice}
                      <span className="block text-xs text-slate-400 mt-0.5">per {item.unit}</span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-500">
                      {item.expiryDate || '-'}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium space-x-2">
                      <button onClick={() => db.inventory.delete(item.id!)} className="text-alert-red hover:text-alert-red/80">
                        <Trash2 className="w-4 h-4" />
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
