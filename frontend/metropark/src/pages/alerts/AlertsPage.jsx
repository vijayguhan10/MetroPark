import { PageHeader } from "../../components/ui/PageHeader";
import { StatCard } from "../../components/ui/StatCard";
import { activeAlerts, alertsSummary, logStream } from "../../data/alertsData";

export function AlertsPage() {
  return (
    <section>
      <PageHeader
        eyebrow="System resilience"
        title="System alerts console"
        description="Critical incidents and the live event stream now sit in separate sections so the page is easier to maintain."
      />

      <div className="grid gap-4 md:grid-cols-3">
        {alertsSummary.map((item) => (
          <StatCard key={item.label} {...item} />
        ))}
      </div>

      <div className="mt-6 grid gap-6 xl:grid-cols-[1fr_1.6fr]">
        <div className="space-y-4">
          {activeAlerts.map((alert) => (
            <article
              key={alert.code}
              className={`rounded-3xl border p-5 ${
                alert.severity === "critical"
                  ? "border-rose-400/20 bg-rose-400/10"
                  : "border-amber-400/20 bg-amber-400/10"
              }`}
            >
              <div className="flex items-center justify-between gap-4">
                <h3 className="text-sm font-semibold text-white">{alert.code}</h3>
                <span className="text-xs uppercase tracking-[0.2em] text-slate-300">
                  {alert.priority}
                </span>
              </div>
              <p className="mt-3 text-sm text-slate-300">{alert.description}</p>
              <p className="mt-3 text-xs text-slate-400">{alert.time}</p>
            </article>
          ))}
        </div>

        <div className="rounded-3xl border border-white/10 bg-slate-900/70">
          <div className="border-b border-white/10 px-5 py-4">
            <h3 className="text-sm font-semibold text-white">Live event stream</h3>
          </div>
          <div className="divide-y divide-white/5">
            {logStream.map(([time, origin, message]) => (
              <div key={`${time}-${origin}`} className="grid gap-3 px-5 py-4 md:grid-cols-[120px_160px_1fr]">
                <p className="text-sm text-slate-500">{time}</p>
                <p className="text-sm font-medium text-cyan-300">{origin}</p>
                <p className="text-sm text-slate-300">{message}</p>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}
