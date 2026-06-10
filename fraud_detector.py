import json
import math
from pyflink.common import Configuration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

# ============================================================================
# KONFIGURACJA
# ============================================================================

JAR_PATH = "file:///home/andrzejek123pl/Downloads/flink-2.0.2/opt/flink-sql-connector-kafka-4.0.1-2.0.jar"

KAFKA_BROKER = "localhost:9092"
TOPIC_IN = "transactions"
TOPIC_OUT = "alarms"

MIN_INTERVAL_SEC = 300
MAX_SPEED_MPS = 30
NIGHT_START = 1
NIGHT_END = 5
STD_MULTIPLIER = 3

# ============================================================================
# STAN (parallelism=1)
# Klucz słownika = pełny card_id hex, np. "6B46B2F1A3D8E09C"
# ============================================================================

last_tx = {}
stats_count = 0
stats_mean = 0.0
stats_m2 = 0.0

# ============================================================================
# POMOCNICZE
# ============================================================================

def haversine_meters(lat1, lon1, lat2, lon2):
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


def parse_timestamp(ts_str):
    from datetime import datetime
    dt = datetime.fromisoformat(ts_str)
    return dt.timestamp(), dt.hour


# ============================================================================
# DETEKCJA
# ============================================================================

def detect_fraud(record_str: str):
    global last_tx, stats_count, stats_mean, stats_m2

    try:
        tx = json.loads(record_str)
    except (json.JSONDecodeError, TypeError):
        return

    card_id = tx["card_id"]       # np. "6B46B2F1A3D8E09C"
    amount = tx["amount"]
    lat = tx["gps"]["latitude"]
    lon = tx["gps"]["longitude"]
    ts_str = tx["timestamp"]
    epoch, hour = parse_timestamp(ts_str)

    reasons = []
    prev_tx_info = None

    # A) Częstotliwość & B) Prędkość
    # Szukamy last_tx[card_id] — dopasowanie po PEŁNYM identyfikatorze karty
    if card_id in last_tx:
        prev = last_tx[card_id]
        dt = epoch - prev["ts"]
        dist = haversine_meters(prev["lat"], prev["lon"], lat, lon)
        speed = dist / dt if dt > 0 else 0

        if 0 < dt < MIN_INTERVAL_SEC:
            reasons.append("frequency")

        if dt > 0 and speed > MAX_SPEED_MPS:
            reasons.append("speed")

        prev_tx_info = {
            "card_id": prev["card_id"],         # pełny hex, np. "6B46B2F1A3D8E09C"
            "user_id": prev["user_id"],
            "timestamp": prev["timestamp"],
            "gps": {"latitude": prev["lat"], "longitude": prev["lon"]},
            "amount": prev["amount"],
            "computed": {
                "time_diff_sec": round(dt, 1),
                "distance_m": round(dist, 1),
                "speed_mps": round(speed, 2),
            },
        }

    # C) Pora nocna
    if NIGHT_START <= hour < NIGHT_END:
        reasons.append("night_hour")

    # D) Kwota outlier (Welford)
    stats_count += 1
    delta = amount - stats_mean
    stats_mean += delta / stats_count
    delta2 = amount - stats_mean
    stats_m2 += delta * delta2

    if stats_count > 30:
        std = math.sqrt(stats_m2 / stats_count)
        if std > 0 and amount > stats_mean + STD_MULTIPLIER * std:
            reasons.append("amount_outlier")

    # Zapisz jako ostatnią transakcję DLA TEJ KARTY (klucz = pełny card_id hex)
    last_tx[card_id] = {
        "card_id": card_id,
        "user_id": tx["user_id"],
        "ts": epoch,
        "lat": lat,
        "lon": lon,
        "timestamp": ts_str,
        "amount": amount,
    }

    # Alarm
    if reasons:
        alarm = {
            "card_id": card_id,                 # bieżąca transakcja
            "user_id": tx["user_id"],
            "amount": amount,
            "timestamp": ts_str,
            "gps": tx["gps"],
            "reasons": reasons,
            "description": ", ".join([f"scam likely - {r}" for r in reasons]),
            "previous_transaction": prev_tx_info,
            "current_stats": {
                "global_mean": round(stats_mean, 2),
                "global_std": round(math.sqrt(stats_m2 / stats_count), 2) if stats_count > 1 else 0,
            },
        }
        yield json.dumps(alarm)


# ============================================================================
# PIPELINE FLINK
# ============================================================================

config = Configuration()
config.set_string("pipeline.jars", JAR_PATH)

env = StreamExecutionEnvironment.get_execution_environment(config)
env.set_parallelism(1)

source = (
    KafkaSource.builder()
    .set_bootstrap_servers(KAFKA_BROKER)
    .set_topics(TOPIC_IN)
    .set_group_id("flink-fraud-detector")
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

sink = (
    KafkaSink.builder()
    .set_bootstrap_servers(KAFKA_BROKER)
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic(TOPIC_OUT)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    .build()
)

env.from_source(
    source, WatermarkStrategy.no_watermarks(), "Kafka-Source-Transactions"
).flat_map(
    detect_fraud, output_type=Types.STRING()
).sink_to(
    sink
).name(
    "Kafka-Sink-Alarms"
)

if __name__ == "__main__":
    print("Uruchamiam Flink Fraud Detector...")
    print(f"  Odczyt:  topic '{TOPIC_IN}'")
    print(f"  Zapis:   topic '{TOPIC_OUT}'")
    env.execute("FraudDetectorJob")