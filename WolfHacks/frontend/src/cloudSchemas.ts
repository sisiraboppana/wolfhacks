import { z } from 'zod';
import { TelemetrySchema } from './schemas';

export const Scope = z.object({
  first_sequence:z.number().int().nonnegative(), last_sequence:z.number().int().nonnegative(),
  first_event_at:z.string().datetime({offset:true}), last_event_at:z.string().datetime({offset:true}),
  event_count:z.number().int().positive(),
});
export const Sessions = z.object({sessions:z.array(z.object({
  session_id:z.string(),device_id:z.string(),source:z.string(),
}))});
export const Snapshot = z.object({
  source:z.literal('databricks_session'),session_id:z.string(),
  data_kind:z.enum(['synthetic','recorded_replay','device','mixed','unknown']),
  datasets:z.array(z.string()),hr_methods:z.array(z.string()),context_complete:z.boolean(),
  status:z.enum(['ready','processing','waiting_for_upload']),
  fetched_at:z.string().datetime({offset:true}),
  counts:z.object({bronze:z.number().int().nonnegative(),silver:z.number().int().nonnegative(),gold:z.number().int().nonnegative()}),
  latest_silver_ingested_at:z.string().nullable(),latest_bronze_ingested_at:z.string().nullable(),
  events:z.array(TelemetrySchema),analysis_scope:Scope.nullable(),snapshot_id:z.string().regex(/^[0-9a-f]{64}$/).nullable(),analysis_limit:z.number().int(),
  windows:z.array(z.object({session_id:z.string(),window_start:z.string(),window_end:z.string(),
    avg_hr:z.number().finite(),min_hr:z.number().finite(),max_hr:z.number().finite(),notification_count:z.number().int()})),
});
export const Analysis = z.object({
  source:z.literal('databricks_model_serving'),session_id:z.string(),notification_count:z.number().int(),
  analyzed_at:z.string().datetime({offset:true}),analysis_scope:Scope.nullable(),snapshot_id:z.string().regex(/^[0-9a-f]{64}$/),
  analysis:z.object({mode:z.enum(['deterministic_preview','llm_agent','llm_unavailable']),message:z.string(),
    next_step:z.string().nullish(),question:z.string().nullish(),error:z.string().nullish(),
    evidence:z.record(z.unknown()).nullish(),
    metrics:z.object({rmssd_ms:z.number().nullable(),sdnn_ms:z.number().nullable(),rr_count:z.number().int()}),
    tool_trace:z.array(z.object({tool:z.string(),status:z.string(),requested_by:z.string().optional()})),
  }),
});
export type CloudSnapshot = z.infer<typeof Snapshot>;
export type HostedAnalysis = z.infer<typeof Analysis>;
export const friendlyDataset = (name:string) => ({hrv_acc:'HRV-ACC · Polar H10',ppg_dalia:'PPG-DaLiA · RespiBAN ECG'}[name] || name);
export function analysisIsOlder(analysis:HostedAnalysis, snapshot:CloudSnapshot) {
  return analysis.session_id === snapshot.session_id && analysis.analysis_scope !== null &&
    snapshot.analysis_scope !== null && analysis.analysis_scope.last_sequence < snapshot.analysis_scope.last_sequence;
}
