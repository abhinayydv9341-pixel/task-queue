"""Demo tasks used by docker-compose. Some fail on purpose so the dashboard shows retries and dead letters."""

from __future__ import annotations

import random
import time

from taskqueue import PermanentError, TaskRegistry

registry = TaskRegistry()


@registry.task()
def send_email(payload):
    time.sleep(random.uniform(0.05, 0.2))
    if random.random() < 0.1:
        raise ConnectionError("SMTP server timed out")
    return {"delivered_to": payload.get("to")}


@registry.task()
def resize_image(payload):
    time.sleep(random.uniform(0.2, 0.6))
    return {"width": payload.get("width", 800) // 2, "height": payload.get("height", 600) // 2}


@registry.task()
def generate_report(payload):
    time.sleep(random.uniform(0.5, 1.5))
    if random.random() < 0.3:
        raise RuntimeError("upstream warehouse query failed")
    return {"rows": random.randint(100, 10_000)}


@registry.task()
def charge_card(payload):
    time.sleep(0.05)
    if payload.get("card") == "declined":
        raise PermanentError("card declined, retrying will not help")
    return {"charged": payload.get("amount")}
