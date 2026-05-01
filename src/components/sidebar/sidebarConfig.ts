import {
  LayoutDashboard,
  Package,
  ArrowUpDown,
  AlertTriangle,
  ShoppingCart,
  HandCoins,
  Truck,
  Receipt,
  BookOpenText,
  Clock,
  Landmark,
  Users,
  Bell,
  Settings,
} from "lucide-react";

export type SidebarGroupId = "inventory" | "transactions" | "accounts" | "people";

export type SidebarItemConfig = {
  id: string;
  label: string;
  href: string;
  icon: React.ComponentType<{ className?: string }>;
  badge?: "alerts";
};

export type SidebarGroupConfig = {
  id: SidebarGroupId;
  label: string;
  icon: React.ComponentType<{ className?: string }>;
  items: SidebarItemConfig[];
};

export const sidebarGroups: SidebarGroupConfig[] = [
  {
    id: "inventory",
    label: "Inventory",
    icon: Package,
    items: [
      { id: "items", label: "Items", href: "/inventory", icon: Package },
      { id: "stockMovement", label: "Stock Movement", href: "/stock-movement", icon: ArrowUpDown },
      { id: "lossWastage", label: "Loss / Wastage", href: "/loss-wastage", icon: AlertTriangle },
    ],
  },
  {
    id: "transactions",
    label: "Transactions",
    icon: ShoppingCart,
    items: [
      { id: "sales", label: "Sales", href: "/orders", icon: ShoppingCart }, // route stays /orders
      { id: "purchases", label: "Purchases", href: "/purchases", icon: Truck },
      { id: "expenses", label: "Expenses", href: "/expenses", icon: Receipt },
    ],
  },
  {
    id: "accounts",
    label: "Accounts",
    icon: BookOpenText,
    items: [
      { id: "ledger", label: "Ledger", href: "/ledger", icon: BookOpenText },
      { id: "dayBook", label: "Day Book", href: "/daybook", icon: Clock },
      { id: "payments", label: "Payments", href: "/payments", icon: HandCoins },
      { id: "financialAccounts", label: "Financial Accounts", href: "/accounts", icon: Landmark },
    ],
  },
  {
    id: "people",
    label: "People",
    icon: Users,
    items: [
      { id: "customers", label: "Customers", href: "/customers", icon: Users },
      { id: "suppliers", label: "Suppliers", href: "/suppliers", icon: Truck },
      { id: "workers", label: "Workers", href: "/workers", icon: Users },
      { id: "users", label: "Users", href: "/users", icon: Users },
    ],
  },
];

export const sidebarTopLevel = [
  { id: "dashboard", label: "Dashboard", href: "/", icon: LayoutDashboard },
  { id: "outstanding", label: "Outstanding", href: "/outstanding", icon: HandCoins },
  { id: "alerts", label: "Alerts", href: "/alerts", icon: Bell, badge: "alerts" as const },
  { id: "settings", label: "Settings", href: "/settings", icon: Settings },
] satisfies SidebarItemConfig[];

