from datetime import datetime
import json
import re
from typing import Callable

from aiohttp import BasicAuth, ClientTimeout
from apscheduler.schedulers.base import BaseScheduler


def parse_check_settings(check: dict):
    if 'status' not in check:
        check['status'] = 200
    timeout = check.get('timeout')
    if timeout:
        check['timeout'] = ClientTimeout(total=timeout)
    auth = check.get('auth')
    if auth and isinstance(auth, list):
        check['auth'] = BasicAuth(*auth)
    data = check.get('data')
    if data and isinstance(data, str):
        check['data'] = data.encode('utf-8')


def parse_sites(data: str) -> dict:
    sites = json.loads(data)
    for site in sites.values():
        parse_check_settings(site)
    return sites


def parse_sites_file(fn: str) -> dict:
    data = open(fn, 'r', encoding='utf-8').read()
    return parse_sites(data)


def schedule_sites(schedule: BaseScheduler, func: Callable, sites: dict):
    trigger_fields = ['weeks', 'days', 'hours', 'minutes', 'seconds', 'start_date', 'end_date', 'timezone', 'jitter']

    for check_name, data in sites.items():
        trigger_kwargs = {k: data.pop(k) for k in trigger_fields if k in data}
        schedule.add_job(
            func=func, name=check_name, trigger="interval", **trigger_kwargs,
            next_run_time=datetime.now(), max_instances=1, coalesce=True, kwargs={
                'check_name': check_name,
                **data,
                'regexp': re.compile(data.pop('regexp')) if data.get('regexp') else None,
            })
