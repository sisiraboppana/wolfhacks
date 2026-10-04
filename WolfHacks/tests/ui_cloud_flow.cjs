/* Isolated browser regression: simulated cloud responses backed by real recording values.
   Start Vite preview on 4179; this test never calls Databricks or the hosted LLM. */
const fs=require('node:fs');
const assert=require('node:assert/strict');
const {chromium}=require(process.env.PACEPILOT_PLAYWRIGHT_MODULE || 'playwright');
const rows=fs.readFileSync('data/real/ppg-dalia-transitions/events.jsonl','utf8').trim().split('\n').map(JSON.parse)
  .filter(e=>e.heart_rate>=30 && e.heart_rate<=230).map(e=>({...e,session_id:'ui-real-replay'}));
const take=(n)=>rows.slice(0,n).slice(-600);
const scope=(events)=>events.length ? {first_sequence:events[0].sequence,last_sequence:events.at(-1).sequence,
  first_event_at:events[0].timestamp,last_event_at:events.at(-1).timestamp,event_count:events.length} : null;
let phase='waiting',n=650,analysisCalls=0;
const requests=[],errors=[];
function snapshot(session) {
 const synthetic=session==='synthetic-session';
 const ready=synthetic || phase==='ready' || phase==='missing-context';
 const events=ready ? synthetic ? take(360).map(e=>({...e,session_id:session,source:'mock',recording:null})) :
  take(n).map(e=>phase==='missing-context' ? {...e,recording:null} : e) : [];
 return {source:'databricks_session',session_id:session,data_kind:ready ? synthetic ? 'synthetic' : 'recorded_replay' : 'unknown',
  datasets:ready && !synthetic && phase!=='missing-context' ? ['ppg_dalia'] : [],
  hr_methods:ready && !synthetic ? ['ecg_rpeaks_derived'] : [],
  context_complete:ready && !synthetic && phase!=='missing-context',
  status:ready ? 'ready' : phase==='processing' ? 'processing' : 'waiting_for_upload',
  fetched_at:new Date().toISOString(),counts:{bronze:phase==='waiting' && !synthetic ? 0 : synthetic ? 360 : 946,
  silver:ready ? synthetic ? 360 : n : 0,gold:0},
  latest_bronze_ingested_at:null,latest_silver_ingested_at:null,events,windows:[],analysis_scope:scope(events),snapshot_id:events.length ? 'a'.repeat(64) : null,analysis_limit:600};
}
(async()=>{
 const browser=await chromium.launch({channel:'msedge',headless:true});
 try {
 const page=await browser.newPage({viewport:{width:1440,height:1100}});
 page.on('pageerror',error=>errors.push(error.message));
 await page.route('**/api/**',async route=>{
  const url=new URL(route.request().url());
  if (url.pathname==='/api/stream/telemetry') {
   await route.fulfill({status:200,contentType:'text/event-stream',body:'data: '+JSON.stringify(rows[649])+'\n\n'});
   return;
  }
  let body,status=200;
  if (url.pathname==='/api/databricks/sessions') body={sessions:[
   {session_id:'ui-real-replay',device_id:'respiban-ppg-dalia',source:'replay'},
   {session_id:'synthetic-session',device_id:'mock-polar-h10',source:'mock'}]};
  else if (url.pathname==='/api/databricks/session') body=snapshot(url.searchParams.get('session_id'));
  else if (url.pathname==='/api/databricks/analyze') {
   analysisCalls++;
   const input=route.request().postDataJSON();requests.push(input);
   const current=snapshot(input.session_id);
   assert.equal(input.through_sequence,current.analysis_scope.last_sequence);
   assert.equal(input.expected_snapshot_id,current.snapshot_id);
   body={source:'databricks_model_serving',session_id:input.session_id,notification_count:current.events.length,
     analyzed_at:new Date().toISOString(),analysis_scope:current.analysis_scope,snapshot_id:current.snapshot_id,
     analysis:{mode:'llm_agent',message:'Recorded HR increased during the annotated stair segment.',
     next_step:'Compare the recorded activities.',question:'Is more workload context available?',
     metrics:{rmssd_ms:17,sdnn_ms:35,rr_count:200},
     tool_trace:[{tool:'summarize_hr_trend',status:'completed',requested_by:'llm'}],evidence:{dataset:'ppg_dalia'}}};
  } else {status=404;body={detail:'Not Found'};}
  await route.fulfill({status,contentType:'application/json',body:JSON.stringify(body)});
 });
 await page.goto('http://127.0.0.1:4179');
 await page.getByText('Following: ui-real-replay',{exact:true}).waitFor();
 await page.getByRole('button',{name:'Connect to Databricks',exact:true}).click();
 await page.getByText(/This replay is visible locally but has not reached Bronze yet/).waitFor();
 assert.equal(analysisCalls,0);
 phase='processing';
 await page.getByRole('button',{name:'Refresh cloud session',exact:true}).click();
 await page.getByText(/Databricks has received raw data and is still validating it/).waitFor();
 phase='ready';
 await page.getByRole('button',{name:'Refresh cloud session',exact:true}).click();
 await page.getByText('Saved heart rate · analysis preview',{exact:true}).waitFor();
 await page.getByRole('button',{name:'Analyze this saved slice',exact:true}).waitFor();
 assert.equal(await page.getByRole('button',{name:'Analyze this saved slice',exact:true}).isEnabled(),true);
 assert.equal(analysisCalls,0,'refresh must not call LLM');
 phase='missing-context';
 await page.getByRole('button',{name:'Refresh cloud session',exact:true}).click();
 await page.getByText(/its dataset\/activity context is missing/).waitFor();
 assert.equal(await page.getByRole('button',{name:'Analyze this saved slice',exact:true}).isEnabled(),false);
 phase='ready';
 await page.getByRole('button',{name:'Refresh cloud session',exact:true}).click();
 await page.getByText('PPG-DaLiA · RespiBAN ECG',{exact:false}).waitFor();
 await page.getByRole('button',{name:'Analyze this saved slice',exact:true}).click();
 await page.getByText('Agent explanation',{exact:true}).waitFor();
 assert.equal(analysisCalls,1);
 n=700;
 await page.getByRole('button',{name:'Refresh cloud session',exact:true}).click();
 await page.getByText(/New saved events have arrived/).waitFor();
 await page.getByText('Recorded HR increased during the annotated stair segment.',{exact:true}).waitFor();
 assert.equal(analysisCalls,1);
 await page.getByText('Cloud session',{exact:true}).scrollIntoViewIfNeeded();
 await page.screenshot({path:'output/cloud-session-desktop.png',fullPage:true});
 await page.setViewportSize({width:390,height:844});
 await page.screenshot({path:'output/cloud-session-mobile.png',fullPage:true});
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth),true,'mobile must not overflow');
 await page.getByRole('combobox',{name:'Cloud session',exact:true}).selectOption('synthetic-session');
 await page.getByText('Synthetic mock data',{exact:true}).waitFor();
 assert.equal(await page.getByText('Agent explanation',{exact:true}).count(),0,'another session must not retain previous explanation');
 assert.deepEqual(errors,[]);
 console.log('PASS: waiting, processing, real-data preview, missing context, snapshot cutoff, preserved stale explanation, session isolation, and mobile layout.');
 } finally {await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
