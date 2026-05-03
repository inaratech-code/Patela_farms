import Dexie, { type EntityTable } from 'dexie';

/** ERP catalog + stock row (single-table item master). */
export type ItemTypeErp = 'sellable' | 'consumable' | 'equipment';

export interface InventoryItem {
  id?: number;
  uid?: string; // stable id for sync
  name: string;
  /** Fish/Chicken = sellable; Feed = consumable; optional equipment. */
  itemType?: ItemTypeErp;
  /** When false, hidden from default pickers (soft delete). */
  active?: boolean;
  /** Reorder / low-stock alert level (defaults from minStockThreshold in migration). */
  reorderLevel?: number;
  /** Moving average unit cost for inventory valuation. */
  avgCost?: number;
  quantity: number;
  unit: string;
  costPrice: number;
  sellingPrice: number;
  minStockThreshold: number;
}

export interface StockMovement {
  id?: number;
  uid?: string;
  itemId: number;
  quantity: number;
  type: 'IN' | 'OUT';
  reason: 'Harvest' | 'Purchase' | 'Sale' | 'Usage' | 'Damage' | 'Loss';
  date: string;
}

export type InventoryLossType = "Dead" | "Damaged" | "Spoiled" | "Missing" | "Theft" | "Wastage";

export interface InventoryLoss {
  id?: number;
  uid?: string;
  itemId: number;
  lossType: InventoryLossType;
  quantity: number;
  unit: string;
  estimatedCost: number;
  reason?: string;
  date: string; // ISO
  createdBy?: string;
}

export type ConsumptionCategory = 'feed_used' | 'farm_use' | 'spoilage';

/** Feed / consumable usage (reduces stock + expense trail). */
export interface ConsumptionLog {
  id?: number;
  uid?: string;
  itemId: number;
  quantity: number;
  cost: number;
  category: ConsumptionCategory;
  notes?: string;
  date: string;
}

export type PaymentStatusErp = 'paid' | 'partial' | 'due';

export interface Sale {
  id?: number;
  uid?: string;
  itemId: number;
  quantity: number;
  totalPrice: number;
  /** Per-unit selling price for this sale (set at POS, not from inventory). */
  unitPrice?: number;
  customerName?: string;
  paymentType: 'Cash' | 'Credit';
  date: string;
  customerId?: number;
  paidAmount?: number;
  dueAmount?: number;
  paymentStatus?: PaymentStatusErp;
}

export interface Purchase {
  id?: number;
  uid?: string;
  supplierName: string;
  supplierId?: number;
  itemId: number;
  quantity: number;
  totalCost: number;
  date: string;
  paidAmount?: number;
  dueAmount?: number;
  paymentStatus?: PaymentStatusErp;
}

export interface DayBookEntry {
  id?: number;
  uid?: string;
  time: string; // ISO String
  description: string;
  amount: number;
  type: 'Income' | 'Expense';
  category: 'Sale' | 'Purchase' | 'Transport' | 'Wage' | 'Other';
  accountId?: number; // FinancialAccount.id (Cash/Bank/QR)
  method?: 'Cash' | 'QR' | 'BankTransfer';
  /** Optional link back to domain entity (consumption, loss, sale id as string, etc.). */
  refType?: string;
  refId?: string;
}

export interface LedgerAccount {
  id?: number;
  uid?: string;
  name: string;
  type: 'Customer' | 'Supplier' | 'Worker';
}

export interface LedgerEntry {
  id?: number;
  uid?: string;
  accountId: number;
  date: string;
  description: string;
  debit: number;
  credit: number;
  balance: number;
}

export interface Payment {
  id?: number;
  uid?: string;
  partyAccountId: number;
  partyType: 'Customer' | 'Supplier' | 'Worker';
  direction: 'Receive' | 'Pay';
  amount: number;
  date: string; // ISO
  note?: string;
  method: 'Cash' | 'QR' | 'BankTransfer';
  accountId?: number; // FinancialAccount.id
  linkedLedgerEntryId?: number;
  linkedDayBookEntryId?: number;
}

export interface FinancialAccount {
  id?: number;
  uid?: string;
  name: string; // e.g. Cash in Hand, Nabil Bank, eSewa QR
  type: 'Cash' | 'Bank' | 'QR';
}

export interface User {
  id?: number;
  username: string;
  email?: string;
  passwordHash?: string;
  phone?: string;
  roleId: number;
}

export interface Role {
  id?: number;
  name: string;
  description?: string;
  permissions: string[]; // e.g. ["dashboard.read", "orders.write"]
  isSystem?: boolean; // true for seeded roles
}

export type SyncEventOp = "create" | "update" | "delete";

export interface SyncEvent {
  id: string; // uuid
  deviceId: string;
  createdAt: string; // ISO
  entityType: string;
  entityId: string;
  op: SyncEventOp;
  payload: unknown;
  appliedAt?: string; // ISO
  pushedAt?: string; // ISO
}

export class PatelaFarmDatabase extends Dexie {
  inventory!: EntityTable<InventoryItem, 'id'>;
  inventoryLosses!: EntityTable<InventoryLoss, "id">;
  stockMovement!: EntityTable<StockMovement, 'id'>;
  sales!: EntityTable<Sale, 'id'>;
  purchases!: EntityTable<Purchase, 'id'>;
  dayBook!: EntityTable<DayBookEntry, 'id'>;
  ledgerAccounts!: EntityTable<LedgerAccount, 'id'>;
  ledgerEntries!: EntityTable<LedgerEntry, 'id'>;
  payments!: EntityTable<Payment, 'id'>;
  financialAccounts!: EntityTable<FinancialAccount, 'id'>;
  users!: EntityTable<User, 'id'>;
  roles!: EntityTable<Role, 'id'>;
  outbox!: EntityTable<SyncEvent, "id">;
  consumptionLogs!: EntityTable<ConsumptionLog, "id">;

  constructor() {
    super('PatelaFarmDB_v2');
    this.version(2).stores({
      inventory: '++id, name, category, quantity',
      stockMovement: '++id, itemId, type, reason, date',
      sales: '++id, itemId, paymentType, date',
      purchases: '++id, supplierName, itemId, date',
      dayBook: '++id, time, type, category',
      ledgerAccounts: '++id, name, type',
      ledgerEntries: '++id, accountId, date',
      payments: '++id, partyAccountId, direction, date',
      users: '++id, name, role'
    });

    this.version(3)
      .stores({
        inventory: '++id, name, category, quantity',
        stockMovement: '++id, itemId, type, reason, date',
        sales: '++id, itemId, paymentType, date',
        purchases: '++id, supplierName, itemId, date',
        dayBook: '++id, time, type, category, accountId',
        ledgerAccounts: '++id, name, type',
        ledgerEntries: '++id, accountId, date',
        payments: '++id, partyAccountId, direction, date, accountId',
        financialAccounts: '++id, name, type',
        users: '++id, name, role',
      })
      .upgrade(async (tx) => {
        const financialAccounts = tx.table("financialAccounts");
        const dayBook = tx.table("dayBook");
        const payments = tx.table("payments");

        const cashId = await financialAccounts.add({ name: "Cash in Hand", type: "Cash" });

        await dayBook.toCollection().modify((e: Record<string, unknown>) => {
          if (typeof e.accountId !== "number") e.accountId = cashId;
          if (!e.method) e.method = "Cash";
        });

        await payments.toCollection().modify((p: Record<string, unknown>) => {
          if (typeof p.accountId !== "number") p.accountId = cashId;
          if (!p.method) p.method = "Cash";
        });
      });

    this.version(4)
      .stores({
        inventory: '++id, name, category, quantity',
        stockMovement: '++id, itemId, type, reason, date',
        sales: '++id, itemId, paymentType, date',
        purchases: '++id, supplierName, itemId, date',
        dayBook: '++id, time, type, category, accountId',
        ledgerAccounts: '++id, name, type',
        ledgerEntries: '++id, accountId, date',
        payments: '++id, partyAccountId, direction, date, accountId',
        financialAccounts: '++id, name, type',
        roles: '++id, name',
        users: '++id, username, roleId',
      })
      .upgrade(async (tx) => {
        const roles = tx.table("roles");
        const users = tx.table("users");

        // Seed default roles (IDs are generated, so we resolve by name).
        const adminId = await roles.add({
          name: "admin",
          description: "Full access to all sections",
          permissions: ["*"],
          isSystem: true,
        });
        const managerId = await roles.add({
          name: "manager",
          description: "Manage daily operations",
          permissions: ["dashboard", "orders", "inventory", "transactions", "accounts", "people", "reports"],
          isSystem: true,
        });
        const workerId = await roles.add({
          name: "worker",
          description: "Limited access",
          permissions: ["dashboard", "inventory", "orders"],
          isSystem: true,
        });

        // Migrate old user shape -> new user shape.
        await users.toCollection().modify((u: Record<string, unknown>) => {
          const oldRole = String(u.role ?? "").toLowerCase();
          const roleId =
            oldRole === "admin" ? adminId : oldRole === "manager" ? managerId : workerId;

          // Old schema used `name`; keep it as username.
          u.username = (u.username ?? u.name ?? "user") as unknown;
          delete (u as Record<string, unknown>).name;
          delete (u as Record<string, unknown>).role;
          delete (u as Record<string, unknown>).permissions;

          u.roleId = roleId;
          if (!u.email) delete (u as Record<string, unknown>).email;
          if (!u.passwordHash) delete (u as Record<string, unknown>).passwordHash;
        });
      });

    this.version(5).stores({
      inventory: '++id, uid, name, category, quantity',
      stockMovement: '++id, uid, itemId, type, reason, date',
      sales: '++id, uid, itemId, paymentType, date',
      purchases: '++id, uid, supplierName, itemId, date',
      dayBook: '++id, uid, time, type, category, accountId',
      ledgerAccounts: '++id, uid, name, type',
      ledgerEntries: '++id, uid, accountId, date',
      payments: '++id, uid, partyAccountId, direction, date, accountId',
      financialAccounts: '++id, name, type',
      roles: '++id, name',
      users: '++id, username, roleId',
      outbox: 'id, createdAt, pushedAt, entityType, entityId',
    });

    this.version(6)
      .stores({
        inventory: '++id, uid, name, category, quantity',
        stockMovement: '++id, uid, itemId, type, reason, date',
        sales: '++id, uid, itemId, paymentType, date',
        purchases: '++id, uid, supplierName, itemId, date',
        dayBook: '++id, uid, time, type, category, accountId',
        ledgerAccounts: '++id, uid, name, type',
        ledgerEntries: '++id, uid, accountId, date',
        payments: '++id, uid, partyAccountId, direction, date, accountId',
        financialAccounts: '++id, uid, name, type',
        roles: '++id, name',
        users: '++id, username, roleId',
        outbox: 'id, createdAt, pushedAt, entityType, entityId',
      })
      .upgrade(async (tx) => {
        const uuidv4 = () =>
          (globalThis.crypto as unknown as { randomUUID?: () => string })?.randomUUID?.() ??
          `${Math.random().toString(16).slice(2)}-${Math.random().toString(16).slice(2)}`;

        const ensureUid = async (tableName: string) => {
          const t = tx.table(tableName);
          await t.toCollection().modify((row: Record<string, unknown>) => {
            if (!row.uid) row.uid = uuidv4();
          });
        };

        await ensureUid("inventory");
        await ensureUid("stockMovement");
        await ensureUid("sales");
        await ensureUid("purchases");
        await ensureUid("dayBook");
        await ensureUid("ledgerAccounts");
        await ensureUid("ledgerEntries");
        await ensureUid("payments");
      });

    this.version(7)
      .stores({
        inventory: '++id, uid, name, category, quantity',
        stockMovement: '++id, uid, itemId, type, reason, date',
        sales: '++id, uid, itemId, paymentType, date',
        purchases: '++id, uid, supplierName, itemId, date',
        dayBook: '++id, uid, time, type, category, accountId',
        ledgerAccounts: '++id, uid, name, type',
        ledgerEntries: '++id, uid, accountId, date',
        payments: '++id, uid, partyAccountId, direction, date, accountId',
        financialAccounts: '++id, uid, name, type',
        roles: '++id, name',
        users: '++id, username, roleId',
        outbox: 'id, createdAt, pushedAt, entityType, entityId',
      })
      .upgrade(async (tx) => {
        const uuidv4 = () =>
          (globalThis.crypto as unknown as { randomUUID?: () => string })?.randomUUID?.() ??
          `${Math.random().toString(16).slice(2)}-${Math.random().toString(16).slice(2)}`;
        const t = tx.table("financialAccounts");
        await t.toCollection().modify((row: Record<string, unknown>) => {
          if (!row.uid) row.uid = uuidv4();
        });
      });

    this.version(8)
      .stores({
        inventory: '++id, uid, name, quantity',
        stockMovement: '++id, uid, itemId, type, reason, date',
        sales: '++id, uid, itemId, paymentType, date',
        purchases: '++id, uid, supplierName, itemId, date',
        dayBook: '++id, uid, time, type, category, accountId',
        ledgerAccounts: '++id, uid, name, type',
        ledgerEntries: '++id, uid, accountId, date',
        payments: '++id, uid, partyAccountId, direction, date, accountId',
        financialAccounts: '++id, uid, name, type',
        roles: '++id, name',
        users: '++id, username, roleId',
        outbox: 'id, createdAt, pushedAt, entityType, entityId',
      })
      .upgrade(async (tx) => {
        const inventory = tx.table("inventory");
        await inventory.toCollection().modify((row: Record<string, unknown>) => {
          if ("category" in row) delete (row as Record<string, unknown>).category;
        });
      });

    this.version(9).stores({
      inventory: '++id, uid, name, quantity',
      inventoryLosses: '++id, uid, itemId, lossType, date',
      stockMovement: '++id, uid, itemId, type, reason, date',
      sales: '++id, uid, itemId, paymentType, date',
      purchases: '++id, uid, supplierName, itemId, date',
      dayBook: '++id, uid, time, type, category, accountId',
      ledgerAccounts: '++id, uid, name, type',
      ledgerEntries: '++id, uid, accountId, date',
      payments: '++id, uid, partyAccountId, direction, date, accountId',
      financialAccounts: '++id, uid, name, type',
      roles: '++id, name',
      users: '++id, username, roleId',
      outbox: 'id, createdAt, pushedAt, entityType, entityId',
    });

    this.version(10)
      .stores({
        inventory: '++id, uid, name, quantity, itemType, active',
        inventoryLosses: '++id, uid, itemId, lossType, date',
        stockMovement: '++id, uid, itemId, type, reason, date',
        sales: '++id, uid, itemId, paymentType, date, paymentStatus',
        purchases: '++id, uid, supplierName, itemId, date, paymentStatus',
        dayBook: '++id, uid, time, type, category, accountId',
        ledgerAccounts: '++id, uid, name, type',
        ledgerEntries: '++id, uid, accountId, date',
        payments: '++id, uid, partyAccountId, direction, date, accountId',
        financialAccounts: '++id, uid, name, type',
        roles: '++id, name',
        users: '++id, username, roleId',
        outbox: 'id, createdAt, pushedAt, entityType, entityId',
        consumptionLogs: '++id, uid, itemId, category, date',
      })
      .upgrade(async (tx) => {
        const inv = tx.table("inventory");
        await inv.toCollection().modify((row: Record<string, unknown>) => {
          if (!row.itemType) row.itemType = "sellable";
          if (row.active === undefined) row.active = true;
          const minT = Number(row.minStockThreshold ?? 0);
          if (typeof row.reorderLevel !== "number") row.reorderLevel = minT;
          const cp = Number(row.costPrice ?? 0);
          if (typeof row.avgCost !== "number" || row.avgCost <= 0) row.avgCost = cp;
        });

        const sales = tx.table("sales");
        await sales.toCollection().modify((row: Record<string, unknown>) => {
          const total = Number(row.totalPrice ?? 0);
          if (row.paymentType === "Cash") {
            if (row.paidAmount === undefined) row.paidAmount = total;
            if (row.dueAmount === undefined) row.dueAmount = 0;
            if (!row.paymentStatus) row.paymentStatus = "paid";
          } else {
            if (row.paidAmount === undefined) row.paidAmount = 0;
            if (row.dueAmount === undefined) row.dueAmount = total;
            if (!row.paymentStatus) row.paymentStatus = "due";
          }
        });

        const purchases = tx.table("purchases");
        await purchases.toCollection().modify((row: Record<string, unknown>) => {
          const total = Number(row.totalCost ?? 0);
          if (!row.paymentStatus) {
            row.paidAmount = total;
            row.dueAmount = 0;
            row.paymentStatus = "paid";
          }
        });
      });

    this.version(11)
      .stores({
        inventory: '++id, uid, name, quantity, itemType, active',
        inventoryLosses: '++id, uid, itemId, lossType, date',
        stockMovement: '++id, uid, itemId, type, reason, date',
        sales: '++id, uid, itemId, paymentType, date, paymentStatus',
        purchases: '++id, uid, supplierName, itemId, date, paymentStatus',
        dayBook: '++id, uid, time, type, category, accountId',
        ledgerAccounts: '++id, uid, name, type',
        ledgerEntries: '++id, uid, accountId, date',
        payments: '++id, uid, partyAccountId, direction, date, accountId',
        financialAccounts: '++id, uid, name, type',
        roles: '++id, name',
        users: '++id, username, roleId',
        outbox: 'id, createdAt, pushedAt, entityType, entityId',
        consumptionLogs: '++id, uid, itemId, category, date',
      })
      .upgrade(async (tx) => {
        await tx.table("inventory").toCollection().modify((row: Record<string, unknown>) => {
          delete row.sku;
        });
      });

    this.version(12)
      .stores({
        inventory: '++id, uid, name, quantity, itemType, active',
        inventoryLosses: '++id, uid, itemId, lossType, date',
        stockMovement: '++id, uid, itemId, type, reason, date',
        sales: '++id, uid, itemId, paymentType, date, paymentStatus',
        purchases: '++id, uid, supplierName, itemId, date, paymentStatus',
        dayBook: '++id, uid, time, type, category, accountId',
        ledgerAccounts: '++id, uid, name, type',
        ledgerEntries: '++id, uid, accountId, date',
        payments: '++id, uid, partyAccountId, direction, date, accountId',
        financialAccounts: '++id, uid, name, type',
        roles: '++id, name',
        users: '++id, username, roleId',
        outbox: 'id, createdAt, pushedAt, entityType, entityId',
        consumptionLogs: '++id, uid, itemId, category, date',
      })
      .upgrade(async (tx) => {
        await tx.table("inventory").toCollection().modify((row: Record<string, unknown>) => {
          delete row.expiryDate;
        });
      });

    this.inventory = this.table('inventory');
    this.inventoryLosses = this.table("inventoryLosses");
    this.stockMovement = this.table('stockMovement');
    this.sales = this.table('sales');
    this.purchases = this.table('purchases');
    this.dayBook = this.table('dayBook');
    this.ledgerAccounts = this.table('ledgerAccounts');
    this.ledgerEntries = this.table('ledgerEntries');
    this.payments = this.table('payments');
    this.financialAccounts = this.table('financialAccounts');
    this.users = this.table('users');
    this.roles = this.table('roles');
    this.outbox = this.table("outbox");
    this.consumptionLogs = this.table("consumptionLogs");
  }
}

export const db = new PatelaFarmDatabase();
