export function PageHeader({ eyebrow, title, description, actions }) {
  return (
    <div className="mb-6 flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
      <div>
        {eyebrow ? (
          <p className="mb-2 text-xs uppercase tracking-[0.25em] text-cyan-300/70">
            {eyebrow}
          </p>
        ) : null}
        <h2 className="text-2xl font-semibold text-white sm:text-3xl">{title}</h2>
        {description ? (
          <p className="mt-2 max-w-3xl text-sm text-slate-400">{description}</p>
        ) : null}
      </div>
      {actions ? <div className="flex gap-3">{actions}</div> : null}
    </div>
  );
}
