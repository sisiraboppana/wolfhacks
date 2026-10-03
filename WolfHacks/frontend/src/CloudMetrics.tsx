import { useState } from 'react';
import { z } from 'zod';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';

const Result = z.object({source:z.literal('databricks_gold'),data_kind:z.literal('synthetic'),
  fetched_at:z.string().datetime({offset:true}),windows:z.array(z.object({
    session_id:z.string(), window_start:z.string().datetime({offset:true}), window_end:z.string().datetime({offset:true}),
    avg_hr:z.number().finite(), min_hr:z.number().finite(), max_hr:z.number().finite(), notification_count:z.number().int().nonnegative(),
  }))});

export function CloudMetrics({api}:{api:string}) {
  const [result,setResult] = useState<z.infer<typeof Result> | null>(null);
  const [busy,setBusy] = useState(false);
  const [error,setError] = useState('');
  const [analysis,setAnalysis] = useState<{mode:string;message:string;next_step?:string|null;question?:string|null;error?:string|null; rmssd:number|null; sdnn:number|null; count:number; notifications:number; trace:string[];evidence?:Record<string,unknown>|null}|null>(null);
  const [prompt,setPrompt] = useState('What changed during this session, was it sustained, and what should I check next?');
  const [analyzing,setAnalyzing] = useState(false);
  async function load() {
    setBusy(true); setError('');
    try {
      const response = await fetch(`${api}/databricks/gold`,{signal:AbortSignal.timeout(65000)});
      if (!response.ok) {
        const body = await response.json().catch(()=>null);
        throw new Error(body?.detail || `Databricks request failed (${response.status})`);
      }
      setResult(Result.parse(await response.json()));
      setAnalysis(null);
    } catch(err) { setError(err instanceof Error ? err.message : 'Databricks unavailable'); }
    finally { setBusy(false); }
  }
  const latest = result?.windows.at(-1);
  async function analyze() {
    if (!latest) return;
    setAnalyzing(true); setAnalysis(null); setError('');
    try {
      const response = await fetch(`${api}/databricks/analyze`,{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({session_id:latest.session_id,prompt}),signal:AbortSignal.timeout(250000)});
      if (!response.ok) {
        const body = await response.json().catch(()=>null);
        throw new Error(body?.detail || `Hosted analysis failed (${response.status})`);
      }
      const parsed = z.object({source:z.literal('databricks_model_serving'), notification_count:z.number().int(),
        analysis:z.object({mode:z.enum(['deterministic_preview','llm_agent','llm_unavailable']),message:z.string(),
          next_step:z.string().nullable().optional(),question:z.string().nullable().optional(),error:z.string().nullable().optional(),
          evidence:z.record(z.unknown()).nullable().optional(),
          metrics:z.object({rmssd_ms:z.number().nullable(),sdnn_ms:z.number().nullable(),rr_count:z.number().int()}),
          tool_trace:z.array(z.object({tool:z.string(),status:z.string(),requested_by:z.string().optional()}))})}).parse(await response.json());
      setAnalysis({mode:parsed.analysis.mode,message:parsed.analysis.message,next_step:parsed.analysis.next_step,
        question:parsed.analysis.question,error:parsed.analysis.error,evidence:parsed.analysis.evidence,
        rmssd:parsed.analysis.metrics.rmssd_ms,sdnn:parsed.analysis.metrics.sdnn_ms,
        count:parsed.analysis.metrics.rr_count, notifications:parsed.notification_count,
        trace:parsed.analysis.tool_trace.map(t=>`${t.tool}: ${t.status}${t.requested_by ? ` (${t.requested_by})` : ''}`)});
    } catch(err) {setError(err instanceof Error ? err.message : 'Hosted analysis unavailable');}
    finally {setAnalyzing(false);}
  }
  return <section className="panel">
    <h2>Databricks · processed session</h2>
    <p>Synthetic uploaded data · Finalized one-minute windows · Separate from the local live stream</p>
    <button onClick={load} disabled={busy || analyzing}>{busy ? 'Reading warehouse…' : result ? 'Refresh cloud results' : 'Load cloud results'}</button>
    {error && <p role="alert" className="error">{error}{result ? ' Previous results remain below.' : ''}</p>}
    {result && <>
      <p>Fetched {new Date(result.fetched_at).toLocaleString()} · {result.windows.length} windows · {latest?.session_id || 'No finalized sessions'}</p>
      {latest && <>
        <p><input aria-label="Question for the hosted agent" value={prompt} maxLength={2000} onChange={e=>setPrompt(e.target.value)} style={{width:'100%'}}/></p>
        <p><button onClick={analyze} disabled={analyzing || busy || !prompt.trim()}>{analyzing ? 'Running hosted analysis…' : 'Analyze with hosted agent'}</button></p>
        {analysis && <div aria-live="polite"><h3>Hosted agent · {analysis.mode === 'llm_agent' ? 'LLM analysis' : analysis.mode === 'llm_unavailable' ? 'LLM unavailable' : 'deterministic preview'}</h3><p>{analysis.message}</p>
          {analysis.next_step && <p><b>Next step:</b> {analysis.next_step}</p>}
          {analysis.question && <p><b>Question:</b> {analysis.question}</p>}
          {analysis.error && <p className="error">{analysis.error}</p>}
          <p>Silver notifications: {analysis.notifications} · RR measurements: {analysis.count}</p>
          <p>RMSSD: {analysis.rmssd ?? 'Unavailable'} ms · SDNN: {analysis.sdnn ?? 'Unavailable'} ms</p>
          <p>Tools: {analysis.trace.join(' · ')}</p><p>Executed on Databricks Model Serving. {analysis.mode === 'llm_agent' ? 'Tools selected and explained by the LLM.' : 'No successful LLM explanation in this response.'}</p>
          {analysis.evidence && <details><summary>Tool evidence</summary><pre style={{whiteSpace:'pre-wrap',overflowWrap:'anywhere'}}>{JSON.stringify(analysis.evidence,null,2)}</pre></details>}</div>}
        <p>Latest window mean: <b>{latest.avg_hr.toFixed(1)} BPM</b> · {latest.notification_count} readings · Window ended {new Date(latest.window_end).toLocaleString()}</p>
        <div className="chart"><ResponsiveContainer width="100%" height="100%"><LineChart data={result.windows}>
          <CartesianGrid stroke="#26344a" strokeDasharray="3 3"/>
          <XAxis dataKey="window_start" tickFormatter={v=>new Date(v).toLocaleTimeString()} minTickGap={40}/>
          <YAxis domain={['auto','auto']} unit=" bpm"/><Tooltip labelFormatter={v=>new Date(String(v)).toLocaleString()}/>
          <Line dataKey="avg_hr" name="Mean HR" stroke="#a5b4fc" strokeWidth={3}/>
        </LineChart></ResponsiveContainer></div>
      </>}
    </>}
  </section>;
}
