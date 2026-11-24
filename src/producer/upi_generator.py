import json
import random
import time
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer
from faker import Faker

fake = Faker("en_IN")

KAFKA_BOOTSTRAP = "localhost:19092"
TOPIC = "upi_transactions"

UPI_HANDLES = ["@okicici", "@okhdfcbank", "@oksbi", "@ybl", "@paytm", "@axl"]
CITIES = ["Hyderabad", "Bengaluru", "Mumbai", "Chennai", "Delhi", "Pune", "Vijayawada"]
CATEGORIES = ["GROCERY", "FOOD_DELIVERY", "UTILITIES", "P2P_TRANSFER", "ELECTRONICS", "TRAVEL"]
USERS = [f"USR-{i:04d}" for i in range(1, 201)]


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for {msg.key()}: {err}")


def build_txn(user_id: str, city: str | None = None, amount: float | None = None) -> dict:
    return {
        "txn_id": f"UPI-{uuid.uuid4().hex[:12].upper()}",
        "user_id": user_id,
        "upi_id": f"{user_id.lower()}{random.choice(UPI_HANDLES)}",
        "merchant_id": f"MCH-{random.randint(100, 999)}",
        "merchant_category": random.choice(CATEGORIES),
        "amount": amount if amount is not None else round(random.uniform(20.0, 4500.0), 2),
        "city": city if city is not None else random.choice(CITIES),
        "device_id": f"DEV-{random.randint(1000, 9999)}",
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    }


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    print(f"Streaming live UPI transactions to '{TOPIC}' on {KAFKA_BOOTSTRAP}... (Ctrl+C to stop)")

    count = 0
    try:
        while True:
            user = random.choice(USERS)
            roll = random.random()

            if roll < 0.05:
                # Fraud Pattern 1: High-velocity burst (6 txns in rapid succession)
                burst_city = random.choice(CITIES)
                for _ in range(6):
                    txn = build_txn(user, city=burst_city, amount=round(random.uniform(1500, 8000), 2))
                    producer.produce(TOPIC, key=user, value=json.dumps(txn), callback=delivery_report)
                    count += 1
                print(f"[INJECTED VELOCITY BURST] {user} -> 6 rapid txns in {burst_city}")

            elif roll < 0.09:
                # Fraud Pattern 2: High-value anomaly (> ₹50,000)
                txn = build_txn(user, amount=round(random.uniform(55000.0, 99000.0), 2))
                producer.produce(TOPIC, key=user, value=json.dumps(txn), callback=delivery_report)
                count += 1
                print(f"[INJECTED HIGH VALUE] {user} -> ₹{txn['amount']} in {txn['city']}")

            elif roll < 0.12:
                # Fraud Pattern 3: Impossible travel (2 distant cities within 1 second)
                city1, city2 = random.sample(CITIES, 2)
                for c in (city1, city2):
                    txn = build_txn(user, city=c, amount=round(random.uniform(500, 5000), 2))
                    producer.produce(TOPIC, key=user, value=json.dumps(txn), callback=delivery_report)
                    count += 1
                print(f"[INJECTED IMPOSSIBLE TRAVEL] {user} -> {city1} & {city2}")

            else:
                # Normal UPI transaction
                txn = build_txn(user)
                producer.produce(TOPIC, key=user, value=json.dumps(txn), callback=delivery_report)
                count += 1

            producer.poll(0)
            if count % 25 == 0:
                print(f"Total events published: {count}")

            time.sleep(0.3)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()