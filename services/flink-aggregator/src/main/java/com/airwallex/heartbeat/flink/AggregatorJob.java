package com.airwallex.heartbeat.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Objects;

/**
 * Flink job: Kafka {@code heartbeats} -> tumbling 1m windows keyed by
 * (region, status) -> t-digest + distinct-set aggregate -> ClickHouse
 * {@code heartbeat_aggregates}.
 *
 * <p>All runtime knobs come from env variables so the same jar runs in
 * docker-compose and production.
 *
 * <pre>
 *   KAFKA_BROKERS        default kafka:9092
 *   KAFKA_TOPIC          default heartbeats
 *   KAFKA_GROUP          default flink-aggregator
 *   CH_JDBC_URL          default jdbc:clickhouse://clickhouse:8123/default
 *   WINDOW_SECONDS       default 60
 *   ALLOWED_LATENESS_SEC default 30
 * </pre>
 */
public class AggregatorJob {

    public static void main(String[] args) throws Exception {
        String brokers = env("KAFKA_BROKERS", "kafka:9092");
        String topic   = env("KAFKA_TOPIC", "heartbeats");
        String group   = env("KAFKA_GROUP", "flink-aggregator");
        String jdbcUrl = env("CH_JDBC_URL", "jdbc:clickhouse://clickhouse:8123/default");
        long windowSec = Long.parseLong(env("WINDOW_SECONDS", "60"));
        long latenessSec = Long.parseLong(env("ALLOWED_LATENESS_SEC", "30"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);

        KafkaSource<Heartbeat> source = KafkaSource.<Heartbeat>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new HeartbeatDeserializer())
                .build();

        // Watermark = max event time - lateness. Out-of-order events
        // within the bound are still included in their window.
        //
        // .withIdleness(...) prevents watermark from freezing when no new
        // events arrive (common in dev + during traffic lulls). After 60s
        // of no data, the source declares itself idle and downstream
        // watermark advances by processing time, which lets open windows
        // close and emit instead of staying stuck forever.
        WatermarkStrategy<Heartbeat> wm = WatermarkStrategy
                .<Heartbeat>forBoundedOutOfOrderness(Duration.ofSeconds(latenessSec))
                .withTimestampAssigner((h, ts) -> h.timestampMs)
                .withIdleness(Duration.ofSeconds(60));

        DataStream<Heartbeat> stream = env
                .fromSource(source, wm, "kafka-heartbeats")
                .filter(Objects::nonNull);

        // Raw sink: every heartbeat lands in `heartbeats` so /api/v1/distinct
        // (which runs `uniqExact(device_id)`) and ad-hoc per-device queries
        // have something to read. The aggregate sink below is independent;
        // losing either one does not affect the other.
        stream.addSink(JdbcSink.sink(
                "INSERT INTO heartbeats " +
                    "(event_time, device_id, region, status, latency_ms, cpu_usage, mem_usage) " +
                "VALUES (?,?,?,?,?,?,?)",
                (ps, h) -> {
                    ps.setTimestamp(1, new Timestamp(h.timestampMs));
                    ps.setString(2, nz(h.deviceId));
                    ps.setString(3, nz(h.region));
                    ps.setString(4, nz(h.status));
                    ps.setDouble(5, h.latencyMs);
                    ps.setFloat(6, (float) h.cpuUsage);
                    ps.setFloat(7, (float) h.memUsage);
                },
                JdbcExecutionOptions.builder()
                        // Raw table is append-heavy; larger batches + the
                        // same 1s flush keep throughput high without
                        // growing p95 ingestion-to-visible latency.
                        .withBatchSize(1_000)
                        .withBatchIntervalMs(1_000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(jdbcUrl)
                        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                        .build()
        )).name("clickhouse-raw-sink");

        // An anonymous (non-lambda) KeySelector preserves the
        // Tuple2<String,String> generic signature at runtime. The lambda
        // version loses it to type erasure and Flink's type extractor
        // can't recover Tuple2<> without an explicit TypeInformation.
        KeySelector<Heartbeat, Tuple2<String,String>> byRegionStatus =
                new KeySelector<Heartbeat, Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String,String> getKey(Heartbeat h) {
                        return Tuple2.of(nz(h.region), nz(h.status));
                    }
                };
        TypeInformation<Tuple2<String,String>> keyType =
                TypeInformation.of(new TypeHint<Tuple2<String,String>>() {});

        SingleOutputStreamOperator<WindowAggregate> aggregates = stream
                .keyBy(byRegionStatus, keyType)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSec)))
                .aggregate(new HeartbeatAggregateFunction(), new AggregateAnnotator());

        aggregates.addSink(JdbcSink.sink(
                "INSERT INTO heartbeat_aggregates " +
                    "(window_start, window_end, region, status, count, distinct_count, " +
                    " avg_latency, p50_latency, p95_latency, p99_latency) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (ps, a) -> {
                    ps.setTimestamp(1, new Timestamp(a.windowStartMs));
                    ps.setTimestamp(2, new Timestamp(a.windowEndMs));
                    ps.setString(3, a.region);
                    ps.setString(4, a.status);
                    ps.setLong(5, a.count);
                    ps.setLong(6, a.distinctCount);
                    ps.setDouble(7, a.avgLatencyMs);
                    ps.setDouble(8, a.p50LatencyMs);
                    ps.setDouble(9, a.p95LatencyMs);
                    ps.setDouble(10, a.p99LatencyMs);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(500)
                        .withBatchIntervalMs(1_000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(jdbcUrl)
                        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                        .build()
        )).name("clickhouse-sink");

        env.execute("heartbeat-aggregator");
    }

    /**
     * Converts an AggregateAccumulator plus the window & key back into a
     * flat WindowAggregate row.
     */
    static class AggregateAnnotator
            extends ProcessWindowFunction<AggregateAccumulator, WindowAggregate, Tuple2<String,String>, TimeWindow> {
        @Override
        public void process(Tuple2<String,String> key, Context ctx,
                            Iterable<AggregateAccumulator> it, Collector<WindowAggregate> out) {
            AggregateAccumulator acc = it.iterator().next();
            WindowAggregate w = new WindowAggregate();
            w.windowStartMs = ctx.window().getStart();
            w.windowEndMs   = ctx.window().getEnd();
            w.region        = key.f0;
            w.status        = key.f1;
            w.count         = acc.count;
            w.distinctCount = acc.devices.size();
            w.avgLatencyMs  = acc.avg();
            w.p50LatencyMs  = acc.p(0.50);
            w.p95LatencyMs  = acc.p(0.95);
            w.p99LatencyMs  = acc.p(0.99);
            out.collect(w);
        }
    }

    private static String env(String k, String d) {
        String v = System.getenv(k);
        return v == null || v.isEmpty() ? d : v;
    }

    private static String nz(String s) { return s == null ? "" : s; }
}
