import { z } from 'zod';
export const TelemetrySchema = z.object({
  schema_version: z.literal('1.0'), user_id: z.string().min(1).max(128), device_id: z.string().min(1).max(128),
  session_id: z.string().min(1).max(128), sequence: z.number().int().nonnegative(),
  timestamp: z.string().datetime({ offset: true }), heart_rate: z.number().int().min(0).max(65535),
  rr_intervals_ms: z.array(z.number().finite().positive().max(64000)).max(256),
  source: z.enum(['mock','polar_h10','replay']), sensor_contact: z.boolean().nullable(),
}).strict();
export type TelemetryEvent = z.infer<typeof TelemetrySchema>;
export const ChatResponseSchema = z.object({
  mode: z.literal('deterministic_preview'), message: z.string(),
  chart_spec: z.object({chartType:z.literal('LineChart'),metric:z.enum(['heart_rate','rr_ms']),title:z.string().max(120)}),
});
