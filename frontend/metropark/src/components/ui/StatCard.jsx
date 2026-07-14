export function StatCard({ label, value, hint, tone = "default" }) {
  const toneClass = {
    default: "border-white/10 bg-white/5",
    cyan: "border-cyan-400/20 bg-cyan-400/10",
    rose: "border-rose-400/20 bg-rose-400/10",
    amber: "border-amber-400/20 bg-amber-400/10",
  };

  return (
    <div className={`rounded-3xl border p-5 ${toneClass[tone]}`}>
      <p className="text-xs uppercase tracking-[0.2em] text-slate-400">{label}</p>
      <div className="mt-3 flex items-end justify-between gap-4">
        <p className="text-3xl font-semibold text-white">{value}</p>
        {hint ? <p className="text-xs text-slate-400">{hint}</p> : null}
      </div>
    </div>
  );
}
