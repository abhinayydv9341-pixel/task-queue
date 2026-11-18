"""Generates a steady synthetic workload across several queues for the demo dashboard."""

from __future__ import annotations

import os
import random
import time

import redis

from taskqueue import TaskQueue

client = redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
emails = TaskQueue(client, "emails")
media = TaskQueue(client, "media")
reports = TaskQueue(client, "reports")
billing = TaskQueue(client, "billing")
rate = float(os.environ.get("JOBS_PER_SECOND", "8"))

print(f"producing ~{rate} jobs/s", flush=True)
while True:
    roll = random.random()
    if roll < 0.5:
        emails.enqueue("send_email", {"to": f"user{random.randint(1, 999)}@example.com"}, priority=random.choice([0, 0, 5, 9]))
    elif roll < 0.8:
        media.enqueue("resize_image", {"width": 1600, "height": 1200}, delay_ms=random.choice([0, 0, 2000]))
    elif roll < 0.92:
        reports.enqueue("generate_report", {"day": time.strftime("%Y-%m-%d")}, max_retries=2)
    else:
        card = "declined" if random.random() < 0.2 else "ok"
        billing.enqueue("charge_card", {"amount": random.randint(5, 500), "card": card}, priority=9)
    time.sleep(random.expovariate(rate))
