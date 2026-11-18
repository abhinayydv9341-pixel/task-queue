import { useEffect, useRef, useState } from 'react';

/** Calls `fetcher` every `intervalMs`, pausing while the tab is hidden. */
export function usePolling(fetcher, intervalMs, deps = []) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [updatedAt, setUpdatedAt] = useState(null);
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  useEffect(() => {
    let cancelled = false;
    let timer;
    const tick = async () => {
      if (document.visibilityState !== 'hidden') {
        try {
          const result = await fetcherRef.current();
          if (!cancelled) {
            setData(result);
            setError(null);
            setUpdatedAt(new Date());
          }
        } catch (err) {
          if (!cancelled) setError(err);
        }
      }
      if (!cancelled) timer = setTimeout(tick, intervalMs);
    };
    tick();
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [intervalMs, ...deps]);

  return { data, error, updatedAt };
}

export async function getJSON(url, options) {
  const res = await fetch(url, options);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}
