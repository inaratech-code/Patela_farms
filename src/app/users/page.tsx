"use client";

import { Users, UserPlus, Trash2, ShieldPlus, X } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { useLiveQuery } from "dexie-react-hooks";
import { db } from "@/lib/db";
import { sha256Base64 } from "@/lib/auth";

type PermissionId =
  | "dashboard"
  | "reports"
  | "outstanding"
  | "inventory.items"
  | "inventory.consumption"
  | "inventory.stockMovement"
  | "inventory.lossWastage"
  | "transactions.overview"
  | "transactions.sales"
  | "transactions.purchases"
  | "transactions.expenses"
  | "accounts.ledger"
  | "accounts.dayBook"
  | "accounts.payments"
  | "accounts.accounts"
  | "people.customers"
  | "people.suppliers"
  | "people.workers"
  | "people.users"
  | "alerts"
  | "settings";

/** Matches `sidebarConfig` routes and labels for role-based access configuration. */
const PERMISSION_GROUPS: Array<{
  id: "top" | "inventory" | "transactions" | "accounts" | "people" | "bottom";
  label: string;
  items: Array<{ id: PermissionId; label: string }>;
}> = [
  {
    id: "top",
    label: "General",
    items: [
      { id: "dashboard", label: "Dashboard" },
      { id: "reports", label: "Reports" },
      { id: "outstanding", label: "Outstanding" },
    ],
  },
  {
    id: "inventory",
    label: "Inventory",
    items: [
      { id: "inventory.items", label: "Items" },
      { id: "inventory.consumption", label: "Feed usage" },
      { id: "inventory.stockMovement", label: "Stock Movement" },
      { id: "inventory.lossWastage", label: "Loss / Wastage" },
    ],
  },
  {
    id: "transactions",
    label: "Transactions",
    items: [
      { id: "transactions.overview", label: "Overview" },
      { id: "transactions.sales", label: "Sales" },
      { id: "transactions.purchases", label: "Purchases" },
      { id: "transactions.expenses", label: "Expenses" },
    ],
  },
  {
    id: "accounts",
    label: "Accounts",
    items: [
      { id: "accounts.ledger", label: "Ledger" },
      { id: "accounts.dayBook", label: "Day Book" },
      { id: "accounts.payments", label: "Payments" },
      { id: "accounts.accounts", label: "Financial Accounts" },
    ],
  },
  {
    id: "people",
    label: "People",
    items: [
      { id: "people.customers", label: "Customers" },
      { id: "people.suppliers", label: "Suppliers" },
      { id: "people.workers", label: "Workers" },
      { id: "people.users", label: "Users" },
    ],
  },
  {
    id: "bottom",
    label: "System",
    items: [
      { id: "alerts", label: "Alerts" },
      { id: "settings", label: "Settings" },
    ],
  },
];

const DEFAULT_PERMISSIONS = new Set<PermissionId>(["dashboard"]);

function toggleSet<T>(s: Set<T>, value: T, checked: boolean) {
  const next = new Set(s);
  if (checked) next.add(value);
  else next.delete(value);
  return next;
}

function isGroupAllSelected(group: (typeof PERMISSION_GROUPS)[number], selected: Set<PermissionId>) {
  return group.items.every((i) => selected.has(i.id));
}

function isGroupAnySelected(group: (typeof PERMISSION_GROUPS)[number], selected: Set<PermissionId>) {
  return group.items.some((i) => selected.has(i.id));
}

export default function UsersPage() {
  const users = useLiveQuery(() => db.users.toArray());
  const roles = useLiveQuery(() => db.roles.toArray());

  const roleById = useMemo(() => {
    const m = new Map<number, (typeof roles extends Array<infer R> ? R : any)>();
    for (const r of roles ?? []) if (typeof r.id === "number") m.set(r.id, r as any);
    return m;
  }, [roles]);

  const [showCreateRole, setShowCreateRole] = useState(false);
  const [showCreateUser, setShowCreateUser] = useState(false);

  const [roleForm, setRoleForm] = useState({
    name: "",
    description: "",
    permissions: new Set<PermissionId>(DEFAULT_PERMISSIONS),
  });

  const firstRoleId = useMemo(() => {
    const list = roles ?? [];
    return list.find((r) => typeof r.id === "number")?.id ?? 0;
  }, [roles]);

  const [userForm, setUserForm] = useState({
    username: "",
    roleId: 0,
    email: "",
    phone: "",
    password: "",
    confirmPassword: "",
  });

  // Keep roleId defaulted when roles load.
  useEffect(() => {
    if (!firstRoleId) return;
    setUserForm((v) => (v.roleId ? v : { ...v, roleId: firstRoleId }));
  }, [firstRoleId]);

  const selectedRole = roleById.get(userForm.roleId);
  const isAdminRole = selectedRole?.name?.toLowerCase?.() === "admin";

  const createRole = async (e: React.FormEvent) => {
    e.preventDefault();
    const name = roleForm.name.trim();
    if (!name) return;
    await db.roles.add({
      name,
      description: roleForm.description.trim() || undefined,
      permissions: Array.from(roleForm.permissions) as string[],
    });
    setShowCreateRole(false);
    setRoleForm({ name: "", description: "", permissions: new Set<PermissionId>(DEFAULT_PERMISSIONS) });
  };

  const createUser = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!userForm.username.trim()) return;
    if (!userForm.roleId) return;
    if (userForm.password.length < 4 || userForm.password.length > 20) return;
    if (userForm.password !== userForm.confirmPassword) return;
    if (isAdminRole && !userForm.email.trim()) return;

    const passwordHash = await sha256Base64(userForm.password);
    await db.users.add({
      username: userForm.username.trim(),
      roleId: userForm.roleId,
      email: userForm.email.trim() || undefined,
      phone: userForm.phone.trim() || undefined,
      passwordHash,
    });
    setShowCreateUser(false);
    setUserForm({
      username: "",
      roleId: firstRoleId || 0,
      email: "",
      phone: "",
      password: "",
      confirmPassword: "",
    });
  };

  return (
    <div className="space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <h1 className="text-2xl font-semibold text-slate-900">User Management</h1>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setShowCreateRole(true)}
            className="flex items-center justify-center gap-2 px-4 py-2 bg-slate-900 text-white rounded-lg hover:bg-slate-800 transition-colors"
          >
            <ShieldPlus className="w-5 h-5" />
            <span>Create Role</span>
          </button>
          <button
            onClick={() => setShowCreateUser(true)}
            className="flex items-center justify-center gap-2 px-4 py-2 bg-primary text-white rounded-lg hover:bg-primary/90 transition-colors"
            disabled={!roles || roles.length === 0}
            title={!roles || roles.length === 0 ? "Create a role first" : undefined}
          >
            <UserPlus className="w-5 h-5" />
            <span>Create User</span>
          </button>
        </div>
      </div>

      {/* Create Role Modal */}
      {showCreateRole ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
          <div className="absolute inset-0 bg-slate-900/50" onClick={() => setShowCreateRole(false)} />
          <div className="relative w-[760px] max-w-[96vw] rounded-2xl bg-white border border-slate-200 shadow-xl overflow-hidden">
            <div className="px-6 py-4 border-b border-slate-200 flex items-center justify-between">
              <div>
                <div className="text-lg font-semibold text-slate-900">Create Role</div>
                <div className="text-sm text-slate-500">Choose which sections this role can access.</div>
              </div>
              <button
                type="button"
                onClick={() => setShowCreateRole(false)}
                className="p-2 rounded-md hover:bg-slate-50 text-slate-600"
                aria-label="Close"
              >
                <X className="w-5 h-5" />
              </button>
            </div>

            <form onSubmit={createRole} className="p-6 space-y-5">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium mb-1">Role Name *</label>
                  <input
                    className="w-full rounded-md border border-slate-200 bg-white px-3 py-2 outline-none focus:ring-2 focus:ring-primary/20 focus:border-primary"
                    placeholder="e.g. manager"
                    value={roleForm.name}
                    onChange={(e) => setRoleForm((v) => ({ ...v, name: e.target.value }))}
                    required
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium mb-1">Description</label>
                  <input
                    className="w-full rounded-md border border-slate-200 bg-white px-3 py-2 outline-none focus:ring-2 focus:ring-primary/20 focus:border-primary"
                    placeholder="Optional"
                    value={roleForm.description}
                    onChange={(e) => setRoleForm((v) => ({ ...v, description: e.target.value }))}
                  />
                </div>
              </div>

              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <div className="text-sm font-semibold text-slate-900">Permissions *</div>
                  <div className="flex items-center gap-2">
                    <button
                      type="button"
                      className="px-3 py-1.5 text-xs font-semibold rounded-md border border-slate-200 hover:bg-slate-50"
                      onClick={() => setRoleForm((v) => ({ ...v, permissions: new Set(DEFAULT_PERMISSIONS) }))}
                    >
                      Reset
                    </button>
                    <button
                      type="button"
                      className="px-3 py-1.5 text-xs font-semibold rounded-md bg-primary text-white hover:bg-primary/90"
                      onClick={() => {
                        const all = new Set<PermissionId>();
                        for (const g of PERMISSION_GROUPS) for (const i of g.items) all.add(i.id);
                        // Always keep dashboard enabled
                        all.add("dashboard");
                        setRoleForm((v) => ({ ...v, permissions: all }));
                      }}
                    >
                      Select all
                    </button>
                  </div>
                </div>

                <div className="rounded-xl border border-slate-200 overflow-hidden bg-white">
                  <div className="max-h-[52vh] overflow-auto">
                    {PERMISSION_GROUPS.map((group) => {
                      const allSelected = isGroupAllSelected(group, roleForm.permissions);
                      const anySelected = isGroupAnySelected(group, roleForm.permissions);
                      return (
                        <div key={group.id} className="border-t border-slate-200 first:border-t-0">
                          <div className="px-4 py-3 bg-slate-50 flex items-center justify-between">
                            <div className="text-sm font-semibold text-slate-900">{group.label}</div>
                            <label className="flex items-center gap-2 text-xs font-semibold text-slate-600 select-none">
                              <input
                                type="checkbox"
                                checked={allSelected}
                                ref={(el) => {
                                  if (!el) return;
                                  el.indeterminate = anySelected && !allSelected;
                                }}
                                onChange={(e) => {
                                  const next = new Set(roleForm.permissions);
                                  for (const item of group.items) {
                                    if (item.id === "dashboard") continue; // keep always on
                                    if (e.target.checked) next.add(item.id);
                                    else next.delete(item.id);
                                  }
                                  next.add("dashboard");
                                  setRoleForm((v) => ({ ...v, permissions: next }));
                                }}
                                className="h-4 w-4 accent-primary"
                              />
                              All
                            </label>
                          </div>

                          <div className="divide-y divide-slate-100">
                            {group.items.map((item) => {
                              const checked = roleForm.permissions.has(item.id);
                              const disabled = item.id === "dashboard";
                              return (
                                <label key={item.id} className="flex items-center justify-between px-4 py-3 hover:bg-slate-50">
                                  <span className={`text-sm ${disabled ? "text-slate-500" : "text-slate-900"}`}>
                                    {item.label}
                                  </span>
                                  <input
                                    type="checkbox"
                                    checked={checked}
                                    disabled={disabled}
                                    onChange={(e) => {
                                      const next = toggleSet(roleForm.permissions, item.id, e.target.checked);
                                      next.add("dashboard");
                                      setRoleForm((v) => ({ ...v, permissions: next }));
                                    }}
                                    className="h-4 w-4 accent-primary"
                                  />
                                </label>
                              );
                            })}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              </div>

              <div className="flex items-center justify-end gap-3 pt-2 border-t border-slate-200">
                <button
                  type="button"
                  onClick={() => setShowCreateRole(false)}
                  className="px-4 py-2 rounded-md border border-slate-200 hover:bg-slate-50 text-slate-700 font-semibold"
                >
                  Cancel
                </button>
                <button type="submit" className="px-5 py-2 rounded-md bg-primary text-white font-semibold hover:bg-primary/90">
                  Create Role
                </button>
              </div>
            </form>
          </div>
        </div>
      ) : null}

      {/* Create User Modal */}
      {showCreateUser ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
          <div className="absolute inset-0 bg-slate-900/50" onClick={() => setShowCreateUser(false)} />
          <div className="relative w-[760px] max-w-[96vw] rounded-2xl bg-white border border-slate-200 shadow-xl overflow-hidden">
            <div className="px-6 py-4 border-b border-slate-200 flex items-center justify-between">
              <div>
                <div className="text-lg font-semibold text-slate-900">Create User</div>
                <div className="text-sm text-slate-500">Add a user and assign a role.</div>
              </div>
              <button
                type="button"
                onClick={() => setShowCreateUser(false)}
                className="p-2 rounded-md hover:bg-slate-50 text-slate-600"
                aria-label="Close"
              >
                <X className="w-5 h-5" />
              </button>
            </div>

            <form onSubmit={createUser} className="p-6 space-y-5">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium mb-1">Username *</label>
                  <input
                    className="w-full rounded-md border border-slate-200 bg-white px-3 py-2 outline-none focus:ring-2 focus:ring-primary/20 focus:border-primary"
                    placeholder="e.g. ram"
                    value={userForm.username}
                    onChange={(e) => setUserForm((v) => ({ ...v, username: e.target.value }))}
                    required
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium mb-1">Role *</label>
                  <select
                    value={userForm.roleId}
                    onChange={(e) => setUserForm((v) => ({ ...v, roleId: Number(e.target.value) }))}
                    className="w-full rounded-md border border-slate-200 bg-white px-3 py-2 outline-none focus:ring-2 focus:ring-primary/20 focus:border-primary"
                  >
                    {(roles ?? []).map((r) => (
                      <option key={r.id} value={r.id}>
                        {r.name}
                      </option>
                    ))}
                  </select>
                  {selectedRole?.description ? (
                    <div className="mt-1 text-xs text-slate-500">{selectedRole.description}</div>
                  ) : null}
                </div>

                <div>
                  <label className="block text-sm font-medium mb-1">
                    Email {isAdminRole ? "*" : "(optional)"}
                  </label>
                  <input
                    className="w-full rounded-md border border-slate-200 bg-white px-3 py-2 outline-none focus:ring-2 focus:ring-primary/20 focus:border-primary"
                    placeholder="name@example.com"
                    value={userForm.email}
                    onChange={(e) => setUserForm((v) => ({ ...v, email: e.target.value }))}
                    required={Boolean(isAdminRole)}
                  />
                  {isAdminRole ? <div className="mt-1 text-xs text-slate-500">Required for admin users.</div> : null}
                </div>

                <div>
                  <label className="block text-sm font-medium mb-1">Phone (optional)</label>
                  <input
                    className="w-full rounded-md border border-slate-200 bg-white px-3 py-2 outline-none focus:ring-2 focus:ring-primary/20 focus:border-primary"
                    placeholder="98XXXXXXXX"
                    value={userForm.phone}
                    onChange={(e) => setUserForm((v) => ({ ...v, phone: e.target.value }))}
                  />
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium mb-1">Password *</label>
                  <input
                    type="password"
                    className="w-full rounded-md border border-slate-200 bg-white px-3 py-2 outline-none focus:ring-2 focus:ring-primary/20 focus:border-primary"
                    placeholder="4-20 characters"
                    value={userForm.password}
                    onChange={(e) => setUserForm((v) => ({ ...v, password: e.target.value }))}
                    minLength={4}
                    maxLength={20}
                    required
                  />
                  <div className="mt-1 text-xs text-slate-500">{Math.min(userForm.password.length, 20)}/20</div>
                </div>
                <div>
                  <label className="block text-sm font-medium mb-1">Confirm Password *</label>
                  <input
                    type="password"
                    className="w-full rounded-md border border-slate-200 bg-white px-3 py-2 outline-none focus:ring-2 focus:ring-primary/20 focus:border-primary"
                    placeholder="Re-enter password"
                    value={userForm.confirmPassword}
                    onChange={(e) => setUserForm((v) => ({ ...v, confirmPassword: e.target.value }))}
                    minLength={4}
                    maxLength={20}
                    required
                  />
                  <div className="mt-1 text-xs text-slate-500">{Math.min(userForm.confirmPassword.length, 20)}/20</div>
                </div>
              </div>

              <div className="flex items-center justify-end gap-3 pt-2 border-t border-slate-200">
                <button
                  type="button"
                  onClick={() => setShowCreateUser(false)}
                  className="px-4 py-2 rounded-md border border-slate-200 hover:bg-slate-50 text-slate-700 font-semibold"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  className="px-5 py-2 rounded-md bg-primary text-white font-semibold hover:bg-primary/90"
                  disabled={
                    !userForm.username.trim() ||
                    !userForm.roleId ||
                    userForm.password.length < 4 ||
                    userForm.password.length > 20 ||
                    userForm.password !== userForm.confirmPassword ||
                    (isAdminRole && !userForm.email.trim())
                  }
                >
                  Create User
                </button>
              </div>
            </form>
          </div>
        </div>
      ) : null}

      {!users || users.length === 0 ? (
        <div className="bg-white rounded-xl shadow-sm border border-slate-100 overflow-hidden">
          <div className="p-8 text-center text-slate-500">
            <Users className="w-12 h-12 mx-auto mb-4 text-slate-300" />
            <p className="text-lg font-medium text-slate-900">No users found</p>
            <p className="mt-1">Add workers, managers, or admins to the system.</p>
          </div>
        </div>
      ) : (
        <div className="bg-white rounded-xl shadow-sm border border-slate-100 overflow-hidden">
          <table className="min-w-full divide-y divide-slate-200">
            <thead className="bg-slate-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">Name</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">Role</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">Phone</th>
                <th className="px-6 py-3 text-right text-xs font-medium text-slate-500 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-slate-200">
              {users.map((user) => (
                <tr key={user.id}>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-slate-900 flex items-center gap-3">
                    <div className="w-8 h-8 rounded-full bg-slate-100 flex items-center justify-center text-slate-500 font-semibold">
                      {user.username.charAt(0).toUpperCase()}
                    </div>
                    {user.username}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-500">
                    <span className="px-2 py-1 rounded text-xs font-medium bg-slate-100 text-slate-700">
                      {roleById.get(user.roleId)?.name ?? "role"}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-500">{user.phone || "-"}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                    <button onClick={() => db.users.delete(user.id!)} className="text-alert-red hover:text-alert-red/80">
                      <Trash2 className="w-4 h-4" />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
