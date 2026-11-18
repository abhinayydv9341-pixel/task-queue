import { useEffect, useMemo, useState } from 'react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { getJSON, usePolling } from './usePolling.js';

const POLL_MS = 2000;
const SERIES_COLORS = ['#2563eb', '#16a34a', '#d97706', '#9333ea', '#dc2626', '#0891b2'];
const STATES = ['ready', 'delayed', 'inflight', 'dead', 'completed'];
const fmtTime = (ms) => (ms ? new Date(ms).toLocaleTimeString() : '—');
const fmtMs = (ms) => (ms == null ? '—' : ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(1)} s`);

export default function App() {
  const { data, error, updatedAt } = usePolling(() => getJSON('/api/overview'), POLL_MS);
  const [selected, setSelected] = useState(null);
  const queues = data?.queues ?? [];
  const current = selected ?? queues[0]?.name ?? null;

  return (
    <div className="page">
      <header>
        <h1>Task Queue Monitor</h1>
        <span className={`status ${error ? 'bad' : 'ok'}`}>
          {error ? `API unreachable: ${error.message}` : updatedAt ? `Live · updated ${updatedAt.toLocaleTimeString()}` : 'Connecting…'}
        </span>
      </header>

      {data && (
        <>
          <Totals data={data} />
          <section className="grid">
            {queues.map((q) => (
              <QueueCard key={q.name} queue={q} active={q.name === current} onSelect={() => setSelected(q.name)} />
            ))}
            {queues.length === 0 && <p className="muted">No queues yet. Enqueue a job to get started.</p>}
          </section>
          <section className="panel">
            <h2>Throughput (jobs completed per 5 s)</h2>
            <ThroughputChart queues={queues} />
          </section>
          <section className="panel">
            <h2>Queue depth</h2>
            <DepthHistory queues={queues} />
          </section>
          <section className="panel">
            <h2>Workers</h2>
            <Workers workers={data.workers} />
          </section>
          {current && <JobsTable queue={current} />}
        </>
      )}
    </div>
  );
}

function Totals({ data }) {
  const sum = (pick) => data.queues.reduce((acc, q) => acc + pick(q), 0);
  const tiles = [
    ['Queued', sum((q) => q.counts.ready + q.counts.delayed)],
    ['Running', sum((q) => q.counts.inflight)],
    ['Completed', sum((q) => q.stats.completed)],
    ['Retried', sum((q) => q.stats.retried)],
    ['Dead-lettered', sum((q) => q.counts.dead)],
    ['Worker utilisation', `${Math.round(data.totals.utilisation * 100)}% of ${data.totals.capacity}`],
  ];
  return (
    <section className="tiles">
      {tiles.map(([label, value]) => (
        <div className="tile" key={label}>
          <div className="tile-value">{value}</div>
          <div className="tile-label">{label}</div>
        </div>
      ))}
    </section>
  );
}

function QueueCard({ queue, active, onSelect }) {
  const { counts, stats, latency_ms: lat } = queue;
  return (
    <button className={`card ${active ? 'active' : ''}`} onClick={onSelect}>
      <div className="card-title">{queue.name}</div>
      <div className="depth">
        <Stat label="ready" value={counts.ready} />
        <Stat label="delayed" value={counts.delayed} />
        <Stat label="running" value={counts.inflight} />
        <Stat label="dead" value={counts.dead} tone={counts.dead ? 'bad' : undefined} />
      </div>
      <div className="card-foot">
        <span>{stats.completed} done</span>
        <span>{stats.failed} failed attempts</span>
        <span>p50 {fmtMs(lat.p50)} · p95 {fmtMs(lat.p95)}</span>
      </div>
    </button>
  );
}

function Stat({ label, value, tone }) {
  return (
    <div className={`stat ${tone ?? ''}`}>
      <b>{value}</b>
      <small>{label}</small>
    </div>
  );
}

function ThroughputChart({ queues }) {
  const rows = useMemo(() => {
    if (!queues.length) return [];
    return queues[0].throughput.map((point, i) => {
      const row = { t: point.t };
      for (const q of queues) row[q.name] = q.throughput[i]?.completed ?? 0;
      return row;
    });
  }, [queues]);
  return (
    <ResponsiveContainer width="100%" height={240}>
      <AreaChart data={rows}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--line)" />
        <XAxis dataKey="t" tickFormatter={fmtTime} minTickGap={40} stroke="var(--muted)" />
        <YAxis allowDecimals={false} stroke="var(--muted)" />
        <Tooltip labelFormatter={fmtTime} />
        <Legend />
        {queues.map((q, i) => (
          <Area
            key={q.name}
            type="monotone"
            dataKey={q.name}
            stackId="1"
            stroke={SERIES_COLORS[i % SERIES_COLORS.length]}
            fill={SERIES_COLORS[i % SERIES_COLORS.length]}
            fillOpacity={0.25}
            isAnimationActive={false}
          />
        ))}
      </AreaChart>
    </ResponsiveContainer>
  );
}

/** Depth is a point-in-time value, so the history is accumulated client-side from each poll. */
function DepthHistory({ queues }) {
  const [history, setHistory] = useState([]);
  useEffect(() => {
    const row = { t: Date.now() };
    for (const q of queues) row[q.name] = q.counts.ready + q.counts.delayed;
    setHistory((h) => [...h.slice(-89), row]);
  }, [queues]);
  return (
    <ResponsiveContainer width="100%" height={200}>
      <LineChart data={history}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--line)" />
        <XAxis dataKey="t" tickFormatter={fmtTime} minTickGap={40} stroke="var(--muted)" />
        <YAxis allowDecimals={false} stroke="var(--muted)" />
        <Tooltip labelFormatter={fmtTime} />
        <Legend />
        {queues.map((q, i) => (
          <Line
            key={q.name}
            type="monotone"
            dataKey={q.name}
            stroke={SERIES_COLORS[i % SERIES_COLORS.length]}
            dot={false}
            isAnimationActive={false}
          />
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
}

function Workers({ workers }) {
  if (!workers.length) return <p className="muted">No live workers.</p>;
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Worker</th>
            <th>Queue</th>
            <th>Utilisation</th>
            <th>Busy</th>
            <th>Processed</th>
            <th>Failed</th>
            <th>Last heartbeat</th>
          </tr>
        </thead>
        <tbody>
          {workers.map((w) => (
            <tr key={w.id}>
              <td className="mono">{w.id}</td>
              <td>{w.queue}</td>
              <td>
                <div className="bar">
                  <div className="bar-fill" style={{ width: `${Math.round(w.utilisation * 100)}%` }} />
                  <span>{Math.round(w.utilisation * 100)}%</span>
                </div>
              </td>
              <td>
                {w.busy}/{w.concurrency}
              </td>
              <td>{w.processed}</td>
              <td>{w.failed}</td>
              <td>{fmtTime(w.last_seen)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function JobsTable({ queue }) {
  const [state, setState] = useState('ready');
  const [refresh, setRefresh] = useState(0);
  const { data: jobs } = usePolling(
    () => getJSON(`/api/queues/${encodeURIComponent(queue)}/jobs?state=${state}&limit=25`),
    POLL_MS,
    [queue, state, refresh],
  );

  const retry = async (id) => {
    await getJSON(`/api/jobs/${id}/retry`, { method: 'POST' });
    setRefresh((n) => n + 1);
  };

  return (
    <section className="panel">
      <div className="panel-head">
        <h2>
          Jobs in <span className="mono">{queue}</span>
        </h2>
        <div className="tabs">
          {STATES.map((s) => (
            <button key={s} className={s === state ? 'active' : ''} onClick={() => setState(s)}>
              {s}
            </button>
          ))}
        </div>
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Job</th>
              <th>Task</th>
              <th>Priority</th>
              <th>Status</th>
              <th>Attempts</th>
              <th>Latency</th>
              <th>Last error</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {(jobs ?? []).map((j) => (
              <tr key={j.id}>
                <td className="mono">{j.id.slice(0, 8)}</td>
                <td>{j.task}</td>
                <td>{j.priority}</td>
                <td>
                  <span className={`pill ${j.status}`}>{j.status}</span>
                </td>
                <td>
                  {j.attempts}/{j.max_retries + 1}
                </td>
                <td>{fmtMs(j.latency_ms)}</td>
                <td className="error">{j.last_error ?? ''}</td>
                <td>{j.status === 'dead' && <button onClick={() => retry(j.id)}>Retry</button>}</td>
              </tr>
            ))}
            {jobs && jobs.length === 0 && (
              <tr>
                <td colSpan={8} className="muted">
                  No {state} jobs.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}
