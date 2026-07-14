import { PageHeader } from "../../components/ui/PageHeader";
import { StatCard } from "../../components/ui/StatCard";
import { liveSessions, liveStats } from "../../data/liveMonitorData";

export function LiveMonitorPage() {
  return (
    <section>
      <PageHeader
        eyebrow="Real-time operations"
        title="Live monitoring feed"
        description="A cleaner operations view for active sessions, occupancy, and the current health of the parking network."
      />

      <div className="grid gap-4 md:grid-cols-3">
        {liveStats.map((item) => (
          <StatCard key={item.label} {...item} />
        ))}
      </div>

      <div className="mt-6 grid gap-6 xl:grid-cols-[2fr_1fr]">
        <div className="rounded-3xl border border-white/10 bg-slate-900/70">
          <div className="border-b border-white/10 px-5 py-4">
            <h3 className="text-sm font-semibold text-white">Active session ledger</h3>
          </div>

          <div className="overflow-x-auto">
            <table className="min-w-full text-left text-sm">
              <thead className="text-slate-400">
                <tr>
                  {["Session", "Plate", "Vehicle", "Slot", "Status"].map((header) => (
                    <th key={header} className="px-5 py-4 font-medium">
                      {header}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {liveSessions.map((row) => (
                  <tr key={row[0]} className="border-t border-white/5 text-slate-200">
                    {row.map((cell) => (
                      <td key={cell} className="px-5 py-4">
                        {cell}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="space-y-6">
          <div className="rounded-3xl border border-white/10 bg-white/5 p-5">
            <h3 className="text-sm font-semibold text-white">Entry camera 04</h3>
            <div className="mt-4 aspect-video rounded-2xl bg-gradient-to-br from-slate-700 to-slate-950" />
            <p className="mt-3 text-sm text-slate-400">
              ANPR active. Latest plate detected: XYZ-9876.
            </p>
          </div>

          <div className="rounded-3xl border border-white/10 bg-white/5 p-5">
            <h3 className="text-sm font-semibold text-white">Telemetry logs</h3>
            <div className="mt-4 space-y-3 text-sm text-slate-300">
              <p>14:32:01 INFO — New session initiated #SN-88224</p>
              <p>14:31:58 INFO — Barrier 02 closed successfully</p>
              <p className="text-amber-300">14:31:12 WARN — Sensor mismatch on B1-12</p>
              <p className="text-rose-300">14:31:45 CRIT — Payment failure on #SN-88102</p>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
