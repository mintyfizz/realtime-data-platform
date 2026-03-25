




import json
import random
import string
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer


def _fallback_user(index: int) -> dict:
    suffix = "".join(random.choices(string.ascii_lowercase, k=5))
    now = datetime.now(timezone.utc).isoformat()
    return {
        "first_name": f"User{index}",
        "last_name": suffix,
        "gender": random.choice(["male", "female"]),
        "address": f"{index} Main St City State Country",
        "postcode": str(10000 + index),
        "email": f"user{index}_{suffix}@example.com",
        "username": f"user{index}_{suffix}",
        "dob": now,
        "registered_date": now,
        "phone": f"+1-555-{1000 + index}",
        "picture": "https://example.com/pic.jpg",
    }


def _fetch_user(index: int) -> dict:
    try:
        response = requests.get("https://randomuser.me/api/", timeout=10)
        response.raise_for_status()
        payload = response.json()
        results = payload.get("results") or []
        if not results:
            return _fallback_user(index)

        user = results[0]
        return {
            "first_name": user["name"]["first"],
            "last_name": user["name"]["last"],
            "gender": user["gender"],
            "address": (
                f"{user['location']['street']['name']} "
                f"{user['location']['street']['number']} "
                f"{user['location']['city']} "
                f"{user['location']['state']} "
                f"{user['location']['country']}"
            ),
            "postcode": str(user["location"]["postcode"]),
            "email": user["email"],
            "username": user["login"]["username"],
            "dob": user["dob"]["date"],
            "registered_date": user["registered"]["date"],
            "phone": user["phone"],
            "picture": user["picture"]["medium"],
        }
    except Exception:
        return _fallback_user(index)


def send_users(total: int = 25) -> None:
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    sent = 0
    for index in range(total):
        user = _fetch_user(index)
        producer.send("user_data", json.dumps(user).encode("utf-8")).get(timeout=10)
        sent += 1

    producer.flush()
    print(f"Sent {sent} messages to topic user_data")


if __name__ == "__main__":
    send_users()