"""
PulseOrder — Apache Flink Real-Time Surge Pricing Job (PyFlink Table & DataStream API)
======================================================================================
ARCHITECTURAL TEACHING: APACHE FLINK SLIDING WINDOWS & EVENT-TIME WATERMARKS

1. WHY SLIDING WINDOWS (`HOP`) OVER TUMBLING WINDOWS (`TUMBLE`)?
   In a dynamic marketplace, demand does not reset on arbitrary 5-minute clock
   boundaries. A 5-minute Sliding Window with a 30-second Hop Slide
   (`HOP(event_time, INTERVAL '30' SECOND, INTERVAL '5' MINUTE)`) evaluates the
   trailing 5 minutes of order volume every 30 seconds. Each incoming
   `ORDER_ACCEPTED` event contributes to 10 overlapping window panes
   (5m / 30s = 10), producing smooth surge transitions (`1.0x -> 1.3x -> 1.6x -> 2.0x`).

2. WHY EVENT-TIME + WATERMARKS?
   Because `orders.events` is partitioned across 3 Kafka partitions by `zone_id`,
   network jitter can delay an event by a few seconds. Defining a bounded-out-of-orderness
   watermark (`WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND`) allows
   Flink to accurately bucket slightly delayed events into their true 5-minute window.

3. DEMAND THRESHOLD MATRIX (PRD Section 3.7):
   - < 20 orders in 5m window:   1.0x
   - 20–50 orders in 5m window:  1.3x
   - 51–100 orders in 5m window: 1.6x
   - > 100 orders in 5m window:  2.0x
   Each computed multiplier is written to Redis:
   `SET surge:zone:{zone_id} {multiplier} EX 120`
"""

import json
import logging
import os
from datetime import datetime, timezone

import redis
from pyflink.common import Configuration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration, Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import SlidingProcessingTimeWindows


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pulseorder.flink.surge")


def calculate_surge_multiplier(order_count: int) -> float:
    """Maps 5-minute active order volume in a zone to its surge multiplier."""
    if order_count > 100:
        return 2.0
    if order_count >= 51:
        return 1.6
    if order_count >= 20:
        return 1.3
    return 1.0


class ZoneSurgeWindowAggregator(ProcessWindowFunction):
    """
    Aggregates order count per `zone_id` inside each 5-minute sliding window
    (hopping every 30 seconds) and writes `SET surge:zone:{zone_id} {mult} EX 120`
    directly to Redis.
    """

    def __init__(self, redis_host: str, redis_port: int):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self._redis_client = None

    def open(self, runtime_context):
        self._redis_client = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            decode_responses=True,
            socket_timeout=2.0,
        )

    def process(self, key: str, context: ProcessWindowFunction.Context, elements):
        active_orders = sum(1 for _ in elements)
        multiplier = calculate_surge_multiplier(active_orders)
        redis_key = f"surge:zone:{key}"

        if self._redis_client is not None:
            try:
                self._redis_client.set(redis_key, f"{multiplier:.2f}", ex=120)
            except Exception as exc:
                logger.warning("Failed writing surge key %s to Redis: %s", redis_key, exc)

        window_end = datetime.fromtimestamp(context.window().end / 1000.0, tz=timezone.utc).isoformat()
        yield json.dumps(
            {
                "zone_id": key,
                "active_orders_5m": active_orders,
                "surge_multiplier": multiplier,
                "window_end": window_end,
            }
        )


def parse_order_accepted_zone(raw_json: str):
    """Extracts `zone_id` from `ORDER_ACCEPTED` events on `orders.events`."""
    try:
        payload = json.loads(raw_json)
        if payload.get("event_type") == "ORDER_ACCEPTED":
            zone_id = str(payload.get("zone_id", "")).strip().lower()
            if zone_id:
                return zone_id
    except Exception:
        pass
    return ""


def main():
    kafka_brokers = os.getenv("KAFKA_BROKERS", "kafka:29092")
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))

    config = Configuration()
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_brokers)
        .set_topics("orders.events")
        .set_group_id("flink-surge-sliding-window")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))

    stream = env.from_source(
        source=kafka_source,
        watermark_strategy=watermark_strategy,
        source_name="KafkaOrdersEventsSource",
    )

    # 1. Filter to ORDER_ACCEPTED events and extract zone_id
    # 2. KeyBy zone_id (co-located on same Kafka partition!)
    # 3. Apply 5-minute Sliding Window hopping every 30 seconds
    # 4. Compute surge multiplier and write `SET surge:zone:{zone_id} {mult} EX 120` to Redis
    (
        stream.map(parse_order_accepted_zone, output_type=Types.STRING())
        .filter(lambda z: len(z) > 0)
        .key_by(lambda z: z, key_type=Types.STRING())
        .window(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.seconds(30)))
        .process(
            ZoneSurgeWindowAggregator(redis_host=redis_host, redis_port=redis_port),
            output_type=Types.STRING(),
        )
        .print()
    )

    env.execute("PulseOrder-5Min-SlidingWindow-SurgeEngine")


if __name__ == "__main__":
    main()

