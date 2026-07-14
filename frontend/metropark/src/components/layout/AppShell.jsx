import { Bell, Menu, Search } from "lucide-react";
import { Sidebar } from "./Sidebar";

export function AppShell({ children }) {
  return (
    <div className="min-h-screen bg-slate-950 text-slate-100">
      <div className="flex min-h-screen">
        <Sidebar />

        <div className="flex min-w-0 flex-1 flex-col lg:pl-72">
          <header className="sticky top-0 z-30 border-b border-white/10 bg-slate-950/90 backdrop-blur">
            <div className="flex h-16 items-center justify-between gap-4 px-4 sm:px-6">
              <div className="flex items-center gap-3">
                <button className="rounded-xl border border-white/10 p-2 text-slate-300 lg:hidden">
                  <Menu size={18} />
                </button>
                <div>
                  <p className="text-xs uppercase tracking-[0.25em] text-cyan-300/80">
                    MetroPark
                  </p>
                  <h1 className="text-sm font-semibold sm:text-base">
                    Operations Control Center
                  </h1>
                </div>
              </div>

              <div className="hidden flex-1 justify-center md:flex">
                <div className="flex w-full max-w-xl items-center gap-3 rounded-2xl border border-white/10 bg-white/5 px-4 py-2">
                  <Search size={16} className="text-slate-400" />
                  <input
                    className="w-full bg-transparent text-sm text-slate-200 outline-none placeholder:text-slate-500"
                    placeholder="Search sessions, zones, alerts..."
                    type="text"
                  />
                </div>
              </div>

              <div className="flex items-center gap-3">
                <button className="relative rounded-xl border border-white/10 p-2 text-slate-300">
                  <Bell size={18} />
                  <span className="absolute right-1.5 top-1.5 h-2 w-2 rounded-full bg-rose-500" />
                </button>
                <div className="hidden text-right sm:block">
                  <p className="text-sm font-medium">Admin Operator</p>
                  <p className="text-xs text-slate-400">Level 4 Access</p>
                </div>
                <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-cyan-400/15 text-sm font-semibold text-cyan-300">
                  MP
                </div>
              </div>
            </div>
          </header>

          <main className="flex-1 px-4 py-6 sm:px-6">{children}</main>
        </div>
      </div>
    </div>
  );
}
