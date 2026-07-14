import { useEffect, useMemo, useState } from "react";
import { routeMap } from "./routes";

const normalizePath = (path) => {
  if (!path || path === "") return "/";
  return path.endsWith("/") && path !== "/" ? path.slice(0, -1) : path;
};

export function navigateTo(path) {
  const nextPath = normalizePath(path);
  window.history.pushState({}, "", nextPath);
  window.dispatchEvent(new PopStateEvent("popstate"));
}

export function AppRouter() {
  const [pathname, setPathname] = useState(() =>
    normalizePath(window.location.pathname),
  );

  useEffect(() => {
    const handleLocationChange = () => {
      setPathname(normalizePath(window.location.pathname));
    };

    window.addEventListener("popstate", handleLocationChange);
    return () => window.removeEventListener("popstate", handleLocationChange);
  }, []);

  const activeRoute = useMemo(
    () => routeMap[pathname] ?? routeMap["/"],
    [pathname],
  );

  const ActivePage = activeRoute.component;
  return <ActivePage currentPath={activeRoute.path} />;
}
