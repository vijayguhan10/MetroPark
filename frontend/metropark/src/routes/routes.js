import { Activity, AlertTriangle, BarChart3, Settings2 } from "lucide-react";
import { LiveMonitorPage } from "../pages/live-monitor/LiveMonitorPage";
import { AnalyticsPage } from "../pages/analytics/AnalyticsPage";
import { AlertsPage } from "../pages/alerts/AlertsPage";
import { SimulationPage } from "../pages/Simulation/Simulation";

export const routes = [
  {
    path: "/",
    label: "Live Monitor",
    icon: Activity,
    component: LiveMonitorPage,
  },
  {
    path: "/analytics",
    label: "Analytics",
    icon: BarChart3,
    component: AnalyticsPage,
  },
  {
    path: "/alerts",
    label: "System Alerts",
    icon: AlertTriangle,
    component: AlertsPage,
  },
  {
    path: "/simulation",
    label: "Simulation",
    icon: Settings2,
    component: SimulationPage,
  },
];

export const routeMap = Object.fromEntries(
  routes.map((route) => [route.path, route]),
);
