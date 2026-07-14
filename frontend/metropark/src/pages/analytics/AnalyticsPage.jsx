import { PageHeader } from "../../components/ui/PageHeader";
import { StatCard } from "../../components/ui/StatCard";
import { analyticsStats, zoneMetrics } from "../../data/analyticsData";

export function AnalyticsPage() {
  return (
    <section>
      <PageHeader
        eyebrow="Performance intelligence"
        title="Analytics dashboard"
        description="Occupancy, revenue, and throughput are organized into smaller, reusable sections instead of one oversized page file."
      />

      <div className="grid gap-4 md:grid-cols-3">
        {analyticsStats.map((item) => (
          <StatCard key={item.label} {...item} />
        ))}
      </div>

      <div className="mt-6 grid gap-6 lg:grid-cols-[1.4fr_1fr]">
        <div className="rounded-3xl border border-white/10 bg-slate-900/70 p-5">
          <h3 className="text-sm font-semibold text-white">Zone occupancy matrix</h3>
          <div className="mt-5 grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
            {zoneMetrics.map((zone) => (
              <div key={zone.zone} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                <p className="text-xs uppercase tracking-[0.2em] text-slate-400">
                  Zone {zone.zone}
                </p>
                <p className="mt-3 text-3xl font-semibold text-white">
                  {zone.occupancy}%
                </p>
                <p className="mt-1 text-sm text-slate-400">{zone.traffic} traffic</p>
                <div className="mt-4 h-2 rounded-full bg-white/10">
                  <div
                    className="h-2 rounded-full bg-cyan-400"
                    style={{ width: `${zone.occupancy}%` }}
                  />
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="rounded-3xl border border-white/10 bg-white/5 p-5">
          <h3 className="text-sm font-semibold text-white">Operational summary</h3>
          <div className="mt-4 space-y-4 text-sm text-slate-300">
            <div className="rounded-2xl border border-white/10 p-4">
              12 newly added nodes this month
            </div>
            <div className="rounded-2xl border border-amber-400/20 bg-amber-400/10 p-4">
              08 payment-due locations need follow-up
            </div>
            <div className="rounded-2xl border border-rose-400/20 bg-rose-400/10 p-4">
              03 suspended locations in exception state
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
