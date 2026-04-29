"use client";

import { Save, Trash2, RefreshCw, CloudUpload, CloudDownload } from "lucide-react";
import { db } from "@/lib/db";
import { clearSession } from "@/lib/auth";
import { getOrCreateDeviceId } from "@/lib/device";
import { getSyncState } from "@/lib/syncState";
import { syncNow, pushOutbox, pullEvents } from "@/lib/sync";
import { useMemo, useState } from "react";

export default function SettingsPage() {
  const deviceId = useMemo(() => getOrCreateDeviceId(), []);
  const syncState = useMemo(() => getSyncState(), []);
  const [syncStatus, setSyncStatus] = useState<string>("");
  const [isSyncing, setIsSyncing] = useState(false);

  const runSync = async (mode: "all" | "push" | "pull") => {
    if (isSyncing) return;
    try {
      setIsSyncing(true);
      setSyncStatus("");
      if (mode === "push") {
        const r = await pushOutbox();
        setSyncStatus(`Pushed ${r.pushed} event(s).`);
      } else if (mode === "pull") {
        const r = await pullEvents();
        setSyncStatus(`Pulled ${r.pulled} event(s).`);
      } else {
        const r = await syncNow();
        setSyncStatus(`Pushed ${r.pushed} · Pulled ${r.pulled}`);
      }
    } catch (e: any) {
      console.error(e);
      setSyncStatus(e?.message ? `Sync failed: ${e.message}` : "Sync failed.");
    } finally {
      setIsSyncing(false);
    }
  };

  const resetAllData = async () => {
    if (!confirm("This will delete ALL saved data (inventory, ledger, day book, users, roles, etc.) and reset app settings. Continue?")) return;

    try {
      // Clear app localStorage keys
      const keysToRemove: string[] = [];
      for (let i = 0; i < localStorage.length; i++) {
        const k = localStorage.key(i);
        if (!k) continue;
        if (k.startsWith("pf.")) keysToRemove.push(k);
      }
      for (const k of keysToRemove) localStorage.removeItem(k);

      clearSession();

      // Wipe IndexedDB
      await db.delete();

      // Reload to re-init DB + UI
      location.href = "/login";
    } catch (e) {
      console.error(e);
      alert("Failed to reset data. Please try again.");
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold text-slate-900">Settings</h1>
      </div>

      <div className="bg-white rounded-xl shadow-sm border border-slate-100 overflow-hidden max-w-2xl">
        <div className="p-6 border-b border-slate-100">
          <h2 className="text-lg font-medium text-slate-900">Sync</h2>
          <p className="mt-1 text-sm text-slate-500">Sync offline data to Supabase when online.</p>
        </div>
        <div className="p-6 space-y-4">
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 text-sm">
            <div className="rounded-lg border border-slate-200 p-4">
              <div className="text-xs text-slate-500">Device ID</div>
              <div className="mt-1 font-mono text-xs text-slate-900 break-all">{deviceId}</div>
            </div>
            <div className="rounded-lg border border-slate-200 p-4">
              <div className="text-xs text-slate-500">Last pulled</div>
              <div className="mt-1 text-sm font-semibold text-slate-900">
                {syncState.lastPulledAt ? new Date(syncState.lastPulledAt).toLocaleString() : "Never"}
              </div>
            </div>
          </div>

          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              onClick={() => runSync("all")}
              disabled={isSyncing}
              className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-primary text-white font-semibold hover:bg-primary/90 disabled:opacity-60"
            >
              <RefreshCw className={`w-4 h-4 ${isSyncing ? "animate-spin" : ""}`} />
              Sync now
            </button>
            <button
              type="button"
              onClick={() => runSync("push")}
              disabled={isSyncing}
              className="inline-flex items-center gap-2 px-4 py-2 rounded-lg border border-slate-200 hover:bg-slate-50 font-semibold text-slate-700 disabled:opacity-60"
            >
              <CloudUpload className="w-4 h-4 text-slate-500" />
              Push
            </button>
            <button
              type="button"
              onClick={() => runSync("pull")}
              disabled={isSyncing}
              className="inline-flex items-center gap-2 px-4 py-2 rounded-lg border border-slate-200 hover:bg-slate-50 font-semibold text-slate-700 disabled:opacity-60"
            >
              <CloudDownload className="w-4 h-4 text-slate-500" />
              Pull
            </button>
          </div>

          {syncStatus ? (
            <div className="text-sm text-slate-700">{syncStatus}</div>
          ) : (
            null
          )}
        </div>
      </div>

      <div className="bg-white rounded-xl shadow-sm border border-slate-100 overflow-hidden max-w-2xl">
        <div className="p-6 border-b border-slate-100">
          <h2 className="text-lg font-medium text-slate-900">General Information</h2>
        </div>
        <div className="p-6 space-y-4">
          <div>
            <label className="block text-sm font-medium text-slate-700 mb-1">Farm Name</label>
            <input 
              type="text" 
              defaultValue="Patela Farm"
              className="w-full px-4 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/20 focus:border-primary"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-slate-700 mb-1">Currency</label>
            <select className="w-full px-4 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/20 focus:border-primary bg-white">
              <option value="NPR">Nepalese Rupee (NPR)</option>
              <option value="INR">Indian Rupee (INR)</option>
              <option value="USD">US Dollar (USD)</option>
            </select>
          </div>
          <div className="pt-4 flex justify-end">
            <button className="flex items-center justify-center gap-2 px-4 py-2 bg-primary text-white rounded-lg hover:bg-primary/90 transition-colors">
              <Save className="w-5 h-5" />
              <span>Save Changes</span>
            </button>
          </div>
        </div>
      </div>

      <div className="bg-white rounded-xl shadow-sm border border-rose-100 overflow-hidden max-w-2xl">
        <div className="p-6 border-b border-rose-100">
          <h2 className="text-lg font-medium text-slate-900">Danger Zone</h2>
          <p className="mt-1 text-sm text-slate-500">Reset the app to a fresh state.</p>
        </div>
        <div className="p-6">
          <button
            type="button"
            onClick={resetAllData}
            className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-alert-red text-white font-semibold hover:bg-alert-red/90"
          >
            <Trash2 className="w-5 h-5" />
            Reset All Data
          </button>
          <div className="mt-2 text-xs text-slate-500">
            This deletes all locally stored data (IndexedDB) and clears app preferences (localStorage).
          </div>
        </div>
      </div>
    </div>
  );
}
