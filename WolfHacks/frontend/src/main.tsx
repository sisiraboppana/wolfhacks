import React, { useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';
import { ChatResponseSchema, TelemetrySchema, type TelemetryEvent } from './schemas';
import './style.css';
import { CloudMetrics } from './CloudMetrics';

const api = (import.meta as unknown as {env: Record<string,string>}).env.VITE_API_BASE || '/api';
function App() {
  const [events, setEvents] = useState<TelemetryEvent[]>([]);
  const [status, setStatus] = useState('Connecting');
  const [error, setError] = useState('');
  const [prompt, setPrompt] = useState('Explain my current heart-rate pattern');
  const [reply, setReply] = useState('Ask for a session summary to see the local deterministic analysis.');
  const [busy, setBusy] = useState(false);
  const [clock, setClock] = useState(Date.now());
  const [title, setTitle] = useState('Live heart rate');
  useEffect(() => {
    const source = new EventSource(`${api}/stream/telemetry?user_id=demo-athlete`);
    source.onopen = () => setStatus('Connected');
    source.onerror = () => setStatus('Reconnecting');
    source.onmessage = ({data}) => {
      try {
        const event = TelemetrySchema.parse(JSON.parse(data));
        setError('');
        setEvents(prev => prev.some(e => e.session_id === event.session_id && e.sequence === event.sequence)
          ? prev : [...prev, event].slice(-120));
      } catch { setError('Received an invalid telemetry record'); }
    };
    const timer = setInterval(() => setClock(Date.now()),1000);
    return () => { source.close(); clearInterval(timer); };
  }, []);
  const latest = events.at(-1);
  const age = latest ? Math.max(0, Math.floor((clock-Date.parse(latest.timestamp))/1000)) : null;
  const sessionEvents = events.filter(e => e.session_id === latest?.session_id);
  async function ask(event: React.FormEvent) {
    event.preventDefault(); setBusy(true); setError('');
    try {
      const response = await fetch(`${api}/chat`, { method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({prompt, user_id:'demo-athlete', session_id:latest?.session_id || 'builtin-demo'}), signal:AbortSignal.timeout(15000) });
      if (!response.ok) throw new Error(`Analysis failed (${response.status})`);
      const result = ChatResponseSchema.parse(await response.json());
      setReply(result.message); setTitle(result.chart_spec.title);
    } catch (err) { setError(err instanceof Error ? err.message : 'Analysis unavailable'); }
    finally { setBusy(false); }
  }
  return <main>
    <header><div><span className="eyebrow">WEARABLE INTELLIGENCE</span><h1>PacePilot<span> / live session</span></h1></div><span className="badge">{latest?.source || 'Mock'} · {status}</span></header>
    <p className="notice">{latest?.recording ? `Real recorded-data replay · ${latest.recording.dataset} · ${latest.recording.hr_method} · Activity: ${latest.recording.activity_label ?? 'unlabeled'}` : latest ? 'Synthetic Polar H10 telemetry' : 'Waiting for an incoming recording'} · Local stream · Dates assigned for replay when using recordings</p>
    <section className="cards">
      <article><label>Heart rate</label><strong>{latest?.heart_rate ?? '—'} <small>BPM</small></strong></article>
      <article><label>Latest RR interval</label><strong>{latest?.rr_intervals_ms.at(-1)?.toFixed(1) ?? '—'} <small>ms</small></strong></article>
      <article><label>{latest?.recording ? 'Recording clock' : 'Telemetry freshness'}</label><strong>{latest?.recording ? 'Replay' : age ?? '—'} <small>{latest?.recording ? '' : 'seconds'}</small></strong><span>{latest?.recording ? 'Virtual event time; original intervals preserved' : age !== null && age > 5 ? 'Stale signal' : 'Gateway receipt time'}</span></article>
    </section>
    <section className="panel"><span className="eyebrow">1 · ARRIVING ON YOUR LAPTOP</span><h2>{title}</h2><p>Incoming events · {latest?.session_id || 'Start a replay to see incoming data'}. Local receipt does not confirm a Databricks upload.</p>
      <div className="chart"><ResponsiveContainer width="100%" height="100%"><LineChart data={sessionEvents}>
        <CartesianGrid stroke="#26344a" strokeDasharray="3 3"/><XAxis dataKey="timestamp" tickFormatter={v=>new Date(v).toLocaleTimeString()} minTickGap={55}/>
        <YAxis domain={['auto','auto']} unit=" bpm"/><Tooltip labelFormatter={v=>new Date(String(v)).toLocaleTimeString()}/>
        <Line type="monotone" dataKey="heart_rate" stroke="#67e8c4" strokeWidth={3} dot={false}/>
      </LineChart></ResponsiveContainer></div>
    </section>
    <details className="panel local-preview"><summary>Local HRV preview · runs on your laptop</summary><p>This deterministic preview uses the local buffer. For the hosted AI explanation, use Session insights below.</p><p className="reply">{reply}</p><form onSubmit={ask}><input aria-label="Ask about the local buffer" value={prompt} maxLength={2000} onChange={e=>setPrompt(e.target.value)}/><button disabled={busy || !prompt.trim()}>{busy ? 'Calculating…' : 'Calculate local preview'}</button></form></details>
    <CloudMetrics api={api} liveSession={latest?.source === 'replay' ? latest.session_id : undefined}/>
    {error && <p role="alert" className="error">{error}</p>}
    <footer>Real recordings are replayed, not newly measured. Dataset labels describe recorded activity; speed, power and personal baselines remain unavailable.</footer>
  </main>;
}
createRoot(document.getElementById('root')!).render(<React.StrictMode><App/></React.StrictMode>);
