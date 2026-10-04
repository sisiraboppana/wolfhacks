"""DLT scaffold for a Unity Catalog volume or classic AWS Kinesis compute.
Run in Databricks, not local Python. No cloud resources are provisioned here.
"""
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType

schema = StructType([
    StructField('schema_version', StringType()), StructField('user_id', StringType()),
    StructField('device_id', StringType()), StructField('session_id', StringType()),
    StructField('sequence', LongType()), StructField('timestamp', StringType()),
    StructField('heart_rate', IntegerType()), StructField('rr_intervals_ms', ArrayType(DoubleType())),
    StructField('source', StringType()), StructField('sensor_contact', BooleanType()),
    StructField('recording', StructType([
        StructField('dataset', StringType()), StructField('subject', StringType()),
        StructField('hr_method', StringType()), StructField('original_offset_seconds', DoubleType()),
        StructField('activity_label', StringType()), StructField('hr_window_seconds', DoubleType()),
    ])),
])

@dlt.table(name='wearable_bronze', comment='Preserve raw notification JSON and arrival metadata')
def bronze():
    if spark.conf.get('wearable.source', 'volume') == 'volume':
        return (spark.readStream.format('cloudFiles')
            .option('cloudFiles.format', 'text')
            .load(spark.conf.get('wearable.volume_path', '/Volumes/workspace/pacepilot/telemetry/incoming'))
            .select(F.col('value').alias('raw_json'), F.current_timestamp().alias('ingested_at')))
    raw = (spark.readStream.format('kinesis')
        .option('streamName', spark.conf.get('wearable.stream_name'))
        .option('region', spark.conf.get('wearable.aws_region'))
        .option('initialPosition', 'earliest').load())
    return raw.select(F.col('data').cast('string').alias('raw_json'), F.current_timestamp().alias('ingested_at'))

@dlt.table(name='wearable_silver', comment='Normalized, quality-checked, deduplicated notifications')
@dlt.expect_or_drop('valid_identity', "user_id IS NOT NULL AND length(user_id) > 0 AND session_id IS NOT NULL AND length(session_id) > 0 AND device_id IS NOT NULL AND length(device_id) > 0 AND sequence >= 0")
@dlt.expect_or_drop('valid_hr', 'heart_rate BETWEEN 30 AND 230')
@dlt.expect_or_drop('valid_time', 'timestamp IS NOT NULL')
@dlt.expect_or_drop('valid_contract', "schema_version = '1.0' AND source IN ('mock', 'polar_h10', 'replay') AND rr_intervals_ms IS NOT NULL AND size(rr_intervals_ms) <= 256 AND forall(rr_intervals_ms, x -> x > 0 AND x <= 64000)")
def silver():
    parsed = (dlt.read_stream('wearable_bronze')
        .select(F.from_json('raw_json', schema).alias('event'), 'ingested_at')
        .select('event.*', 'ingested_at').withColumn('timestamp', F.to_timestamp('timestamp')))
    return (parsed.withWatermark('timestamp', '2 minutes')
        .dropDuplicatesWithinWatermark(['user_id', 'session_id', 'sequence']))

@dlt.table(name='wearable_gold_1m', comment='Finalized one-minute HR summaries; not subsecond telemetry')
def gold():
    return (dlt.read_stream('wearable_silver').withWatermark('timestamp', '2 minutes')
        .groupBy('user_id', 'session_id', F.window('timestamp', '1 minute'))
        .agg(F.avg('heart_rate').alias('avg_hr'), F.min('heart_rate').alias('min_hr'),
             F.max('heart_rate').alias('max_hr'), F.count('*').alias('notification_count'),
             F.max('timestamp').alias('latest_event_at'))
        .select('user_id', 'session_id', F.col('window.start').alias('window_start'),
            F.col('window.end').alias('window_end'), 'avg_hr', 'min_hr', 'max_hr', 'notification_count', 'latest_event_at'))

# Cloud HRV requires ordered beat reconstruction and continuity handling.
# Do not flatten unordered collect_list arrays into RMSSD.
# Historical baselines and Feature Store are not implemented.
