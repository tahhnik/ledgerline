"""Task 5: the same feature must have the same value offline and online."""
import os
import time

import pandas as pd
import pytest
from ledgerline.generator import GeneratorConfig, generate

from features import offline_hour_of_day, online_hour_of_day


@pytest.fixture(params=["UTC", "Asia/Dhaka", "America/New_York"])
def server_timezone(request):
    """Run the online path on servers configured with different time zones."""
    old = os.environ.get("TZ")
    os.environ["TZ"] = request.param
    time.tzset()
    yield request.param
    if old is None:
        os.environ.pop("TZ")
    else:
        os.environ["TZ"] = old
    time.tzset()


def test_offline_and_online_values_match(server_timezone):
    events = [r.event for r in generate(GeneratorConfig(n_events=300, events_per_second=0.01, seed=21))]
    offline = offline_hour_of_day(pd.DataFrame(events)).tolist()
    online = [online_hour_of_day(e) for e in events]
    mismatches = sum(a != b for a, b in zip(offline, online))
    assert mismatches == 0, f"{mismatches} of {len(events)} values differ on a {server_timezone} server"
