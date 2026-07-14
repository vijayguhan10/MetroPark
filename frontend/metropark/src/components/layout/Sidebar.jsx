import { LifeBuoy, LogOut } from "lucide-react";
import { routes } from "../../routes/routes";
import { navigateTo } from "../../routes/AppRouter";

function isActivePath(routePath) {
  return window.location.pathname === routePath;
}

export function Sidebar() {
  return (
    <aside className="hidden w-72 shrink-0 border-r border-white/10 bg-slate-900/95 lg:fixed lg:inset-y-0 lg:flex lg:flex-col">
      <div className="border-b border-white/10 px-6 py-6">
        <div className="flex items-center gap-3">
          <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-cyan-400 text-slate-950">
            MP
          </div>
          <div>
            <h2 className="text-lg font-semibold">MetroPark</h2>
            <p className="text-xs uppercase tracking-[0.2em] text-slate-400">
              Frontend Console
            </p>
          </div>
        </div>
      </div>

      <nav className="flex-1 px-4 py-6">
        <div className="space-y-2">
          {routes.map((route) => {
            const Icon = route.icon;
            const active = isActivePath(route.path);

            return (
              <button
                key={route.path}
                onClick={() => navigateTo(route.path)}
                className={`flex w-full items-center gap-3 rounded-2xl px-4 py-3 text-left text-sm transition ${
                  active
                    ? "bg-cyan-400 text-slate-950"
                    : "text-slate-300 hover:bg-white/5"
                }`}
              >
                {Icon ? <Icon size={18} /> : null}
                <span className="font-medium">{route.label}</span>
              </button>
            );
          })}
        </div>
      </nav>

      <div className="border-t border-white/10 p-4">
        <button className="mb-2 flex w-full items-center gap-3 rounded-2xl px-4 py-3 text-left text-sm text-slate-300 hover:bg-white/5">
          <LifeBuoy size={18} />
          Support
        </button>
        <button className="flex w-full items-center gap-3 rounded-2xl px-4 py-3 text-left text-sm text-slate-300 hover:bg-white/5">
          <LogOut size={18} />
          Sign out
        </button>
      </div>
    </aside>
  );
}
