"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { api, ApiError } from "./api";

type Loaded<T> = { path: string; data: T };

/**
 * Loads `path` from the admin API, optionally polling it. Polling keeps the
 * previous data on screen (no loading flash) and pauses in hidden tabs.
 * `null` skips the request.
 */
export function useApi<T>(path: string | null, refreshMs?: number) {
  const [loaded, setLoaded] = useState<Loaded<T> | null>(null);
  const [error, setError] = useState<ApiError | null>(null);
  const [tick, setTick] = useState(0);

  useEffect(() => {
    if (!path) return;
    let cancelled = false;
    const load = () => {
      if (document.visibilityState === "hidden") return;
      api<T>(path).then(
        (data) => {
          if (cancelled) return;
          setLoaded({ path, data });
          setError(null);
        },
        (e: unknown) => {
          if (cancelled) return;
          setError(e instanceof ApiError ? e : new ApiError(0, "Error", String(e)));
        },
      );
    };
    load();
    const timer = refreshMs ? window.setInterval(load, refreshMs) : undefined;
    const onVisible = () => document.visibilityState === "visible" && refreshMs && load();
    document.addEventListener("visibilitychange", onVisible);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, [path, refreshMs, tick]);

  const data = loaded && loaded.path === path ? loaded.data : undefined;
  const reload = useCallback(() => setTick((t) => t + 1), []);
  const replace = useCallback((data: T) => path && setLoaded({ path, data }), [path]);
  return { data, error: data === undefined ? error : null, staleError: error, reload, replace };
}

/** Copies text, also on plain-HTTP pages where `navigator.clipboard` is missing. */
export async function copyText(text: string): Promise<boolean> {
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch {
    // fall through to the textarea method
  }
  const area = document.createElement("textarea");
  area.value = text;
  area.setAttribute("readonly", "");
  area.style.position = "fixed";
  area.style.opacity = "0";
  document.body.appendChild(area);
  area.select();
  let ok = false;
  try {
    ok = document.execCommand("copy");
  } catch {
    ok = false;
  }
  area.remove();
  return ok;
}

/** `true` for a moment after `mark()`, for "Copied" feedback. */
export function useFlag(ms = 1600): [boolean, () => void] {
  const [on, setOn] = useState(false);
  const timer = useRef<number | undefined>(undefined);
  useEffect(() => () => window.clearTimeout(timer.current), []);
  const mark = useCallback(() => {
    setOn(true);
    window.clearTimeout(timer.current);
    timer.current = window.setTimeout(() => setOn(false), ms);
  }, [ms]);
  return [on, mark];
}

/** Re-renders every `ms`, for "3 min ago" style labels. */
export function useNow(ms = 30_000): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), ms);
    return () => window.clearInterval(timer);
  }, [ms]);
  return now;
}
