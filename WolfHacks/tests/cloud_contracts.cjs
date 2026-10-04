const {test}=require('node:test');
const assert=require('node:assert/strict');
const {Snapshot,Analysis,analysisIsOlder,friendlyDataset}=require('../tmp/cloud-contract-tests/cloudSchemas.js');

const time='2026-10-03T12:00:00Z';
const event={schema_version:'1.0',user_id:'demo-athlete',device_id:'respiban-ppg-dalia',session_id:'real',
  sequence:5,timestamp:time,heart_rate:80,rr_intervals_ms:[750],source:'replay',
  recording:{dataset:'ppg_dalia',subject:'S10',hr_method:'ecg_rpeaks_derived',original_offset_seconds:1000,activity_label:'stairs'}};
const scope={first_sequence:5,last_sequence:5,first_event_at:time,last_event_at:time,event_count:1};
const snapshot={source:'databricks_session',session_id:'real',data_kind:'recorded_replay',datasets:['ppg_dalia'],
  hr_methods:['ecg_rpeaks_derived'],context_complete:true,status:'ready',fetched_at:time,
  counts:{bronze:1,silver:1,gold:0},latest_bronze_ingested_at:time,latest_silver_ingested_at:time,
  events:[event],analysis_scope:scope,snapshot_id:'a'.repeat(64),analysis_limit:600,windows:[]};
const report={source:'databricks_model_serving',session_id:'real',notification_count:1,analyzed_at:time,analysis_scope:scope,snapshot_id:'a'.repeat(64),
  analysis:{mode:'llm_agent',message:'Real data',metrics:{rmssd_ms:null,sdnn_ms:null,rr_count:0},tool_trace:[]}};

test('real replay with no Gold windows is ready for Silver analysis',()=>{
 assert.equal(Snapshot.parse(snapshot).status,'ready');
 assert.equal(friendlyDataset('ppg_dalia'),'PPG-DaLiA · RespiBAN ECG');
});
test('waiting cloud session is distinct from synthetic data',()=>{
 const waiting={...snapshot,status:'waiting_for_upload',data_kind:'unknown',datasets:[],hr_methods:[],
 context_complete:false,events:[],analysis_scope:null,snapshot_id:null,counts:{bronze:0,silver:0,gold:0}};
 assert.equal(Snapshot.parse(waiting).data_kind,'unknown');
});
test('new events mark a saved explanation as older without discarding it',()=>{
 const old=Analysis.parse(report);
 assert.equal(analysisIsOlder(old,Snapshot.parse(snapshot)),false);
 assert.equal(analysisIsOlder(old,{...snapshot,analysis_scope:{...scope,last_sequence:6}}),true);
 assert.equal(old.analysis.message,'Real data');
});
test('an explanation from another session is never presented as a new snapshot of this session',()=>{
 assert.equal(analysisIsOlder({...report,session_id:'synthetic'},snapshot),false);
});
test('malformed recording metadata is rejected rather than displayed as valid',()=>{
 assert.equal(Snapshot.safeParse({...snapshot,events:[{...event,recording:{...event.recording,dataset:'invented'}}]}).success,false);
});
