export const alertsSummary = [
  { label: "System integrity", value: "99.4%", hint: "-0.2%", tone: "cyan" },
  { label: "Critical faults", value: "02", hint: "Immediate action", tone: "rose" },
  { label: "Memory load", value: "72%", hint: "Alpha-7", tone: "amber" },
];

export const activeAlerts = [
  {
    code: "PAYMENT_GATEWAY_TIMEOUT",
    priority: "P1",
    description: "Session #9042 handshake failure at Bank API 3.",
    time: "12:04:12 UTC",
    severity: "critical",
  },
  {
    code: "SECURITY_ANOMALY",
    priority: "P2",
    description: "Restricted vehicle detected in Zone B.",
    time: "11:58:05 UTC",
    severity: "warning",
  },
];

export const logStream = [
  ["12:00:01", "SYSTEM_INIT", "MetroPark Core initialized. Scanning nodes..."],
  ["12:00:05", "HEARTBEAT", "Cluster OMEGA healthy across all modules."],
  ["12:01:15", "ENTRY_EVENT", "Vehicle entry at Slot 45, RFID verified."],
  ["12:02:44", "PAYMENT_AUTH", "Payment authorized for Session #902, $14.50."],
  ["12:04:12", "ERROR_LOG", "Critical timeout in payment gateway for Session #9042."],
];
