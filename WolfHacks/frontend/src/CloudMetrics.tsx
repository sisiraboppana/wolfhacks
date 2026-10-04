import { useEffect, useState } from 'react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';
import { Analysis, Sessions, Snapshot, friendlyDataset, analysisIsOlder, type CloudSnapshot, type HostedAnalysis } from './cloudSchemas';

const at = (value:string) => new Date(value).toLocaleTimeString();
async function read(url:string, signal:AbortSignal, options:RequestInit={}) {
  const response = await fetch(url,{...options,signal});
  const body = await response.json().catch(()=>null);
  if (response.status === 404) throw new Error('Restart the backend from your Git checkout to load the updated cloud-session API.');
  if (!response.ok) throw new Error(body?.detail || `Cloud request failed (${response.status})`);
  return body;
}
function kindLabel(kind:string) {
  return ({recorded_replay:'Real recorded-data replay',synthetic:'Synthetic mock data',device:'Device measurements',mixed:'Mixed sources',unknown:'Waiting for cloud data'}[kind] || kind);
}

export function CloudMetrics({api,liveSession}:{api:string;liveSession?:string}) {
  const [connected,setConnected] = useState(false);
  const [following,setFollowing] = useState(true);
  const [selected,setSelected] = useState('');
  const [sessions,setSessions] = useState<{session_id:string;device_id:string;source:string}[]>([]);
  const [snapshot,setSnapshot] = useState<CloudSnapshot|null>(null);
  const [analysis,setAnalysis] = useState<HostedAnalysis|null>(null);
  const [busy,setBusy] = useState(false);
  const [analyzing,setAnalyzing] = useState(false);
  const [error,setError] = useState('');
  const [analysisError,setAnalysisError] = useState('');
  const [autoRefresh,setAutoRefresh] = useState(true);
  const [revision,setRevision] = useState(0);
  const [prompt,setPrompt] = useState('Explain the heart-rate changes and data quality in this recorded slice. Use activity labels where available.');

  useEffect(()=> {
    if (following && liveSession) setSelected(liveSession);
  },[following,liveSession]);

  useEffect(()=> {
    if (!connected || analyzing) return;
    const controller = new AbortController();
    let active = true;
    const timeout = setTimeout(()=>controller.abort(),70000);
    setBusy(true);setError('');
    void (async()=> {
      try {
        const listed = Sessions.parse(await read(`${api}/databricks/sessions`,controller.signal));
        if (!active) return;
        setSessions(listed.sessions);
        const session = selected || listed.sessions[0]?.session_id;
        if (!session) { setSnapshot(null);return; }
        if (!selected) { setSelected(session);return; }
        const loaded = Snapshot.parse(await read(`${api}/databricks/session?session_id=${encodeURIComponent(session)}`,controller.signal));
        if (loaded.session_id !== session) throw new Error('Cloud returned a different session. Please retry.');
        if (active) setSnapshot(loaded);
      } catch(err) {
        if (active) setError(controller.signal.aborted ? 'The cloud read timed out. Retry after the SQL warehouse wakes up.' :
          err instanceof Error ? err.message : 'Cloud data is unavailable.');
      } finally {
        clearTimeout(timeout);
        if (active) setBusy(false);
      }
    })();
    return ()=>{active=false;clearTimeout(timeout);controller.abort();};
  },[api,connected,selected,revision,analyzing]);

  useEffect(()=> {
    if (!connected || !autoRefresh || busy || analyzing) return;
    const timer = setTimeout(()=> {
      if (!document.hidden) setRevision(v=>v+1);
    },error ? 120000 : 60000);
    const visible = ()=>{if (!document.hidden) setRevision(v=>v+1);};
    document.addEventListener('visibilitychange',visible);
    return ()=>{clearTimeout(timer);document.removeEventListener('visibilitychange',visible);};
  },[connected,autoRefresh,busy,analyzing,revision,error]);

  const current = snapshot?.session_id === selected ? snapshot : null;
  const report = analysis?.session_id === selected ? analysis : null;
  const missingContext = current?.data_kind === 'recorded_replay' && !current.context_complete;
  const stale = !!(report && current && analysisIsOlder(report,current));
  const selectable = sessions.some(s=>s.session_id===selected) || !selected ? sessions :
    [{session_id:selected,device_id:'active replay · awaiting ingestion',source:'replay'},...sessions];

  async function analyze() {
    if (!current?.analysis_scope || busy || analyzing || missingContext) return;
    const session = current.session_id;
    setAnalyzing(true);setAnalysisError('');
    try {
      const result = Analysis.parse(await read(`${api}/databricks/analyze`,AbortSignal.timeout(250000),{
        method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({session_id:session,prompt,through_sequence:current.analysis_scope.last_sequence,expected_snapshot_id:current.snapshot_id}),
      }));
      if (result.session_id !== session || result.snapshot_id !== current.snapshot_id) throw new Error('Analysis did not match the displayed saved slice. Refresh and retry.');
      setAnalysis(result);
    } catch(err) {
      setAnalysisError(err instanceof Error ? err.message : 'Hosted analysis is unavailable.');
    } finally {setAnalyzing(false);}
  }

  return <section className="panel cloud-panel">
    <div className="panel-heading"><div><span className="eyebrow">2 · SAVED AND PROCESSED IN DATABRICKS</span><h2>Cloud session</h2></div>
      <span className="badge">{busy ? 'Checking cloud…' : current?.status === 'ready' ? 'Data received' : connected ? 'Waiting for data' : 'Not connected'}</span>
    </div>
    <p>The live chart above shows what reaches your laptop. This panel checks what actually reached Databricks, then analyzes that saved recording.</p>
    <div className="cloud-controls">
      <button onClick={()=>{setConnected(true);setRevision(v=>v+1);}} disabled={busy || analyzing}>{busy ? 'Reading Databricks…' : connected ? 'Refresh cloud session' : 'Connect to Databricks'}</button>
      <label className="check"><input type="checkbox" checked={autoRefresh} onChange={e=>setAutoRefresh(e.target.checked)}/> Refresh every minute while this tab is visible</label>
    </div>
    <label className="check"><input type="checkbox" checked={following} disabled={analyzing} onChange={e=>setFollowing(e.target.checked)}/> Follow the incoming replay{liveSession ? '' : ' · no replay arriving locally'}</label>
    {selectable.length > 0 && <label className="session-picker">Recording to inspect
      <select aria-label="Cloud session" value={selected} disabled={analyzing} onChange={e=>{setFollowing(false);setSelected(e.target.value);setAnalysisError('');}}>
        {selectable.map(s=><option key={s.session_id} value={s.session_id}>{s.session_id} · {s.source === 'mock' ? 'synthetic' : s.device_id}</option>)}
      </select>
    </label>}
    {following && liveSession && <p className="session-id">Following: {liveSession}</p>}
    {error && <p role="alert" className="error">{error} {current ? 'Last successful snapshot is shown below.' : ''}</p>}
    {!connected && <div className="empty-state">Connect once to load saved sessions. Incoming replay will be selected automatically; older mock sessions remain available in the recording picker.</div>}
    {connected && !selected && !busy && !error && <div className="empty-state">No sessions are in Silver yet. Replay with <code>--sink both</code> or <code>--sink volume</code> and keep the Databricks pipeline running.</div>}
    {current && <>
      <p><b>{kindLabel(current.data_kind)}</b>{current.datasets.length ? ' · '+current.datasets.map(friendlyDataset).join(', ') : ''}</p>
      <p className="session-id">{current.session_id} · Last checked {at(current.fetched_at)}</p>
      <div className="pipeline-progress">
        <article><label>Raw data received · Bronze</label><strong>{current.counts.bronze.toLocaleString()}</strong><small>uploaded events ingested</small></article>
        <article><label>Validated events · Silver</label><strong>{current.counts.silver.toLocaleString()}</strong><small>saved events available to analyze</small></article>
        <article><label>Minute summaries · Gold</label><strong>{current.counts.gold.toLocaleString()}</strong><small>finalized heart-rate windows</small></article>
      </div>
      {current.counts.bronze > current.counts.silver && <p className="subtle">The raw/validated difference can include pending processing, duplicates, quality filtering, or late events.</p>}
      {current.latest_silver_ingested_at && <p className="subtle">Last validated arrival: {new Date(current.latest_silver_ingested_at).toLocaleString()}. Recording timestamps may use a virtual replay clock.</p>}
      {current.status === 'waiting_for_upload' && <div className="empty-state">This replay is visible locally but has not reached Bronze yet. Batches upload about every 20 recording seconds. If this persists, check the producer says “uploaded” and the pipeline is running.</div>}
      {current.status === 'processing' && <div className="empty-state">Databricks has received raw data and is still validating it. Analysis becomes available when Silver events appear.</div>}
      {missingContext && <p role="alert" className="error">This replay is saved, but its dataset/activity context is missing. Update the pipeline Python source and run an update before requesting activity-aware analysis.</p>}
      {current.events.length > 0 && <>
        <h3>Saved heart rate · analysis preview</h3>
        <p>The latest {current.events.length} validated events{current.analysis_scope ? `, ${at(current.analysis_scope.first_event_at)}–${at(current.analysis_scope.last_event_at)}` : ''}. These are the events sent to the hosted agent.</p>
        <div className="chart"><ResponsiveContainer width="100%" height="100%"><LineChart data={current.events}>
          <CartesianGrid stroke="#26344a" strokeDasharray="3 3"/><XAxis dataKey="timestamp" tickFormatter={at} minTickGap={45}/>
          <YAxis domain={['auto','auto']} unit=" bpm"/><Tooltip labelFormatter={v=>at(String(v))}/>
          <Line dataKey="heart_rate" name="Saved HR" stroke="#a5b4fc" strokeWidth={2} dot={false} isAnimationActive={false}/>
        </LineChart></ResponsiveContainer></div>
      </>}
      <details className="window-details"><summary>Minute summaries · {current.counts.gold} finalized windows</summary>
        <p>Gold summarizes the recording into one-minute averages. Its two-minute event-time watermark delays finalization; the final windows of a stopped replay can remain pending. The agent uses Silver and does not wait for Gold.</p>
        {current.windows.length > 0 ? <div className="chart"><ResponsiveContainer width="100%" height="100%"><LineChart data={current.windows}>
          <CartesianGrid stroke="#26344a" strokeDasharray="3 3"/><XAxis dataKey="window_start" tickFormatter={at} minTickGap={45}/>
          <YAxis domain={['auto','auto']} unit=" bpm"/><Tooltip labelFormatter={v=>at(String(v))}/>
          <Line dataKey="avg_hr" name="One-minute mean" stroke="#67e8c4" strokeWidth={3} isAnimationActive={false}/>
        </LineChart></ResponsiveContainer></div> : <p>No minute windows have finalized yet.</p>}
      </details>
      <div className="hosted-analysis">
        <span className="eyebrow">3 · UNDERSTAND THE SAVED RECORDING</span><h3>Session insights</h3>
        <p>The hosted agent uses DeepSeek and three read-only tools to explain HR changes, sustained rises and RR quality. Analysis runs when you request it; refreshing data does not call the LLM.</p>
        <label htmlFor="cloud-question">Question about this recording</label>
        <textarea id="cloud-question" value={prompt} maxLength={2000} onChange={e=>setPrompt(e.target.value)} rows={2}/>
        <button onClick={analyze} disabled={!current.analysis_scope || missingContext || busy || analyzing || !prompt.trim()}>{analyzing ? 'Analyzing saved recording…' : 'Analyze this saved slice'}</button>
        {analysisError && <p role="alert" className="error">{analysisError}</p>}
        {report && <div className="analysis-report" aria-live="polite">
          <h3>{report.analysis.mode === 'llm_agent' ? 'Agent explanation' : report.analysis.mode === 'llm_unavailable' ? 'Tools completed · explanation unavailable' : 'Deterministic tool preview'}</h3>
          <p className="subtle">Analyzed {report.notification_count} events at {at(report.analyzed_at)}{report.analysis_scope ? ` · recording time ${at(report.analysis_scope.first_event_at)}–${at(report.analysis_scope.last_event_at)}` : ''}</p>
          {stale && <p className="update-notice">New saved events have arrived. This explanation remains tied to the earlier slice; analyze again for an updated explanation.</p>}
          <p className="reply">{report.analysis.message}</p>
          {report.analysis.next_step && <p><b>Next step:</b> {report.analysis.next_step}</p>}
          {report.analysis.question && <p><b>Context to investigate:</b> {report.analysis.question}</p>}
          {report.analysis.error && <p className="error">{report.analysis.error}</p>}
          <div className="hrv-summary"><span>RMSSD: <b>{report.analysis.metrics.rmssd_ms ?? 'Unavailable'}</b> ms</span><span>SDNN: <b>{report.analysis.metrics.sdnn_ms ?? 'Unavailable'}</b> ms</span></div>
          <p className="subtle">{report.analysis.metrics.rr_count} RR measurements used from the latest usable continuous segment. HRV is descriptive, not a recovery score.</p>
          <details><summary>Tools and evidence</summary>
            <p>{report.analysis.tool_trace.map(t=>`${t.tool}: ${t.status}${t.requested_by ? ` (${t.requested_by})` : ''}`).join(' · ')}</p>
            {report.analysis.evidence && <pre>{JSON.stringify(report.analysis.evidence,null,2)}</pre>}
          </details>
        </div>}
      </div>
    </>}
  </section>;
}
