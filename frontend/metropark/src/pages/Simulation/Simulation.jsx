import { useMemo, useState } from "react";

const vehicleOptions = ["Car", "Bike", "Delivery"];

const stageCopy = [
  { id: 1, title: "Location Setup", badge: "Configuration Base" },
  { id: 2, title: "Vehicle Mapping", badge: "Locked" },
  { id: 3, title: "Slot Inventory", badge: "Locked" },
];

function StageBadge({ isUnlocked, label }) {
  return (
    <span
      className={`rounded-full border px-3 py-1 text-xs font-semibold uppercase tracking-[0.2em] ${
        isUnlocked
          ? "border-cyan-400/30 bg-cyan-400/10 text-cyan-200"
          : "border-white/10 bg-white/5 text-slate-400"
      }`}
    >
      {isUnlocked ? "Unlocked" : label}
    </span>
  );
}

export function SimulationPage() {
  const [maxUnlockedStage, setMaxUnlockedStage] = useState(1);
  const [selectedVehicles, setSelectedVehicles] = useState(["Car"]);

  const stageStates = useMemo(
    () => stageCopy.map((stage) => stage.id <= maxUnlockedStage),
    [maxUnlockedStage],
  );

  const unlockStage = (stageNumber) => {
    setMaxUnlockedStage((current) => Math.max(current, stageNumber));
  };

  const toggleVehicle = (vehicle) => {
    setSelectedVehicles((current) =>
      current.includes(vehicle)
        ? current.filter((item) => item !== vehicle)
        : [...current, vehicle],
    );
  };

  return (
    <section className="space-y-6">
      <div className="rounded-3xl border border-cyan-400/10 bg-linear-to-br from-slate-900 via-slate-900 to-slate-950 p-6 shadow-2xl shadow-cyan-950/20">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-xs uppercase tracking-[0.3em] text-cyan-300/80">
              Configuration workflow
            </p>
            <h2 className="mt-2 text-3xl font-semibold text-white">
              Infrastructure Configuration Wizard
            </h2>
            <p className="mt-3 max-w-2xl text-sm text-slate-300">
              Provision a parking site in three dependent stages. Each stage
              unlocks the next one once the required configuration has been
              entered.
            </p>
          </div>

          <div className="rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-right">
            <p className="text-xs uppercase tracking-[0.2em] text-slate-400">
              Current stage
            </p>
            <p className="mt-1 text-lg font-semibold text-white">
              Stage {maxUnlockedStage}
            </p>
          </div>
        </div>

        <div className="mt-6 grid gap-3 md:grid-cols-3">
          {stageCopy.map((stage) => {
            const active = stage.id === maxUnlockedStage;
            const unlocked = stage.id <= maxUnlockedStage;

            return (
              <div
                key={stage.id}
                className={`rounded-2xl border p-4 transition ${
                  active
                    ? "border-cyan-400/30 bg-cyan-400/10"
                    : "border-white/10 bg-white/5"
                }`}
              >
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <p className="text-xs uppercase tracking-[0.2em] text-slate-400">
                      Stage {stage.id}
                    </p>
                    <p className="mt-1 font-medium text-white">{stage.title}</p>
                  </div>
                  <StageBadge isUnlocked={unlocked} label={stage.badge} />
                </div>
              </div>
            );
          })}
        </div>
      </div>

      <div className="space-y-6">
        <section
          className={`rounded-3xl border p-6 ${
            stageStates[0]
              ? "border-cyan-400/20 bg-slate-900/80"
              : "border-white/10 bg-slate-900/50"
          }`}
        >
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div>
              <div className="flex items-center gap-3">
                <span className="material-symbols-outlined text-cyan-300">
                  location_on
                </span>
                <h3 className="text-lg font-semibold text-white">
                  Stage 1: Location Setup
                </h3>
              </div>
              <p className="mt-2 text-sm text-slate-400">Configuration Base</p>
            </div>
            <StageBadge isUnlocked={stageStates[0]} label="Locked" />
          </div>

          <div className="mt-6 grid gap-4 md:grid-cols-2">
            <label className="space-y-2">
              <span className="text-sm text-slate-300">Location Type</span>
              <select className="w-full rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white outline-none">
                <option>Multi-level Garage</option>
                <option>Surface Lot</option>
                <option>Curb-side Hub</option>
                <option>Underground Complex</option>
              </select>
            </label>

            <label className="space-y-2">
              <span className="text-sm text-slate-300">Location Name</span>
              <input
                className="w-full rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white outline-none placeholder:text-slate-500"
                placeholder="e.g. Central Plaza Deck A"
                type="text"
              />
            </label>

            <label className="space-y-2 md:col-span-2">
              <span className="text-sm text-slate-300">Address</span>
              <input
                className="w-full rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white outline-none placeholder:text-slate-500"
                placeholder="Street name and number"
                type="text"
              />
            </label>

            <label className="space-y-2">
              <span className="text-sm text-slate-300">City</span>
              <input
                className="w-full rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white outline-none placeholder:text-slate-500"
                placeholder="City"
                type="text"
              />
            </label>

            <div className="flex items-center justify-between rounded-2xl border border-white/10 bg-white/5 px-4 py-3">
              <div>
                <p className="text-sm font-medium text-white">Initial Status</p>
                <p className="text-xs text-slate-400">
                  Activate infrastructure immediately
                </p>
              </div>
              <div className="h-6 w-11 rounded-full bg-cyan-400/40 p-1">
                <div className="h-4 w-4 rounded-full bg-white" />
              </div>
            </div>
          </div>

          <div className="mt-6 flex justify-end">
            <button
              className="rounded-2xl bg-cyan-400 px-5 py-3 text-sm font-semibold text-slate-950 transition hover:bg-cyan-300"
              onClick={() => unlockStage(2)}
              type="button"
            >
              Save and continue
            </button>
          </div>
        </section>

        <section
          className={`rounded-3xl border p-6 ${
            stageStates[1]
              ? "border-cyan-400/20 bg-slate-900/80"
              : "border-white/10 bg-slate-900/50 opacity-70"
          }`}
        >
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div>
              <div className="flex items-center gap-3">
                <span className="material-symbols-outlined text-cyan-300">
                  directions_car
                </span>
                <h3 className="text-lg font-semibold text-white">
                  Stage 2: Vehicle Mapping
                </h3>
              </div>
              <p className="mt-2 text-sm text-slate-400">
                Define the reservation model and vehicle types.
              </p>
            </div>
            <StageBadge isUnlocked={stageStates[1]} label="Locked" />
          </div>

          <div className="mt-6 grid gap-4 md:grid-cols-2">
            <label className="space-y-2">
              <span className="text-sm text-slate-300">Location Selector</span>
              <select className="w-full rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white outline-none">
                <option>Central Plaza Deck A (Mapped from Stage 1)</option>
              </select>
            </label>

            <label className="space-y-2">
              <span className="text-sm text-slate-300">
                Reservation Class Assignment
              </span>
              <select className="w-full rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white outline-none">
                <option>General</option>
                <option>VIP</option>
                <option>Reserved (Contractual)</option>
                <option>Staff Only</option>
              </select>
            </label>

            <div className="md:col-span-2">
              <p className="text-sm text-slate-300">
                Vehicle Types Multi-select
              </p>
              <div className="mt-3 grid gap-3 md:grid-cols-3">
                {vehicleOptions.map((vehicle) => {
                  const selected = selectedVehicles.includes(vehicle);

                  return (
                    <button
                      key={vehicle}
                      type="button"
                      onClick={() => toggleVehicle(vehicle)}
                      className={`flex items-center gap-3 rounded-2xl border px-4 py-4 text-left transition ${
                        selected
                          ? "border-cyan-400/30 bg-cyan-400/10 text-white"
                          : "border-white/10 bg-white/5 text-slate-300 hover:bg-white/10"
                      }`}
                    >
                      <span
                        className={`flex h-5 w-5 items-center justify-center rounded border text-[10px] ${
                          selected
                            ? "border-cyan-300 bg-cyan-300 text-slate-950"
                            : "border-white/20 bg-transparent text-transparent"
                        }`}
                      >
                        ✓
                      </span>
                      <span className="text-sm font-medium">{vehicle}</span>
                    </button>
                  );
                })}
              </div>
            </div>
          </div>

          <div className="mt-6 flex justify-end">
            <button
              className="rounded-2xl bg-cyan-400 px-5 py-3 text-sm font-semibold text-slate-950 transition hover:bg-cyan-300 disabled:cursor-not-allowed disabled:opacity-40"
              onClick={() => unlockStage(3)}
              type="button"
              disabled={!stageStates[0]}
            >
              Validate and continue
            </button>
          </div>
        </section>

        <section
          className={`rounded-3xl border p-6 ${
            stageStates[2]
              ? "border-cyan-400/20 bg-slate-900/80"
              : "border-white/10 bg-slate-900/50 opacity-70"
          }`}
        >
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div>
              <div className="flex items-center gap-3">
                <span className="material-symbols-outlined text-cyan-300">
                  inventory_2
                </span>
                <h3 className="text-lg font-semibold text-white">
                  Stage 3: Slot Inventory
                </h3>
              </div>
              <p className="mt-2 text-sm text-slate-400">
                Finalize the configured location and sensor details.
              </p>
            </div>
            <StageBadge isUnlocked={stageStates[2]} label="Locked" />
          </div>

          <div className="mt-6 grid gap-4 md:grid-cols-2">
            <label className="space-y-2">
              <span className="text-sm text-slate-300">Target Location</span>
              <input
                className="w-full rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-slate-400 outline-none"
                disabled
                type="text"
                value="Central Plaza Deck A"
              />
            </label>

            <label className="space-y-2">
              <span className="text-sm text-slate-300">
                Allowed Vehicle Type
              </span>
              <select className="w-full rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white outline-none">
                <option>Car (Inherited)</option>
                <option>Bike (Inherited)</option>
              </select>
            </label>

            <label className="space-y-2">
              <span className="text-sm text-slate-300">Reservation Class</span>
              <input
                className="w-full rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-slate-400 outline-none"
                disabled
                type="text"
                value="General / VIP"
              />
            </label>

            <label className="space-y-2">
              <span className="text-sm text-slate-300">Display Code</span>
              <input
                className="w-full rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white outline-none placeholder:text-slate-500"
                placeholder="e.g. ZONE-01-A"
                type="text"
              />
            </label>

            <label className="space-y-2 md:col-span-2">
              <span className="text-sm text-slate-300">Sensor Hardware ID</span>
              <div className="flex gap-3">
                <input
                  className="min-w-0 flex-1 rounded-2xl border border-white/10 bg-white/5 px-4 py-3 font-mono text-sm text-white outline-none placeholder:text-slate-500"
                  placeholder="S-000-000-000"
                  type="text"
                />
                <button
                  className="rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm font-medium text-slate-200 transition hover:bg-white/10"
                  type="button"
                >
                  Scan hardware
                </button>
              </div>
            </label>
          </div>

          <div className="mt-6 flex flex-wrap items-center justify-between gap-4 border-t border-white/10 pt-5">
            <div className="flex items-center gap-3 text-sm text-slate-300">
              <span className="flex h-8 w-8 items-center justify-center rounded-full border border-emerald-400/30 bg-emerald-400/10 text-emerald-300">
                ✓
              </span>
              <p>
                System validation ready. No errors detected in configuration
                sequence.
              </p>
            </div>

            <button
              className="rounded-2xl bg-cyan-400 px-5 py-3 text-sm font-semibold text-slate-950 transition hover:bg-cyan-300 disabled:cursor-not-allowed disabled:opacity-40"
              type="button"
              disabled={!stageStates[2]}
            >
              Finalize configuration
            </button>
          </div>
        </section>
      </div>
    </section>
  );
}
