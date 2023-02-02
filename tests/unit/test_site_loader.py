from datetime import timedelta
import json
import re

from aiohttp import BasicAuth, ClientTimeout
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.job import Job
from apscheduler.triggers.interval import IntervalTrigger

from sites_loader import parse_sites, schedule_sites


def check_website(*args, **kwargs):
    pass


TEST_SITES = {
    "test1": {
        "url": "http://test1.com",
        "seconds": 5,
        "method": "POST",
        "auth": ['user', 'pass'],
        "data": "hello",
        "headers": {
            "X-API-KEY": "123456789AAA"
        },
        "timeout": 3,
    },
    "test2": {
        "url": "https://test2.com",
        "minutes": 10,
        "status": None,
        "regexp": "(?<=aaa)\\d+(?=bbb)",
    }
}

EXPECTED_JOBS = [
    {
        'name': 'test1',
        'func': check_website,
        'args': tuple(),
        'kwargs': {
            'check_name': 'test1',
            "url": "http://test1.com",
            "method": "POST",
            "auth": BasicAuth("user", "pass"),
            "headers": {
                "X-API-KEY": "123456789AAA"
            },
            "data": b'hello',
            "timeout": ClientTimeout(total=3),
            "status": 200,
            "regexp": None,
        },
        'trigger': {
            'type': IntervalTrigger,
            'interval': timedelta(seconds=5),
        },
        'max_instances': 1,
        'coalesce': True,
    },
    {
        'name': 'test2',
        'func': check_website,
        'args': tuple(),
        'kwargs': {
            'check_name': 'test2',
            "url": "https://test2.com",
            "status": None,
            "regexp": re.compile(r"(?<=aaa)\d+(?=bbb)"),
        },
        'trigger': {
            'type': IntervalTrigger,
            'interval': timedelta(minutes=10),
        },
        'max_instances': 1,
        'coalesce': True,
    }
]


def dump_job(job: Job):
    return {
        'name': job.name,
        'func': job.func,
        'args': job.args,
        'kwargs': job.kwargs,
        'trigger': {
            'type': type(job.trigger),
            'interval': job.trigger.interval,
        },
        'max_instances': job.max_instances,
        'coalesce': job.coalesce,
    }


def test_schedule_sites():
    sites = parse_sites(json.dumps(TEST_SITES))
    schedule = BlockingScheduler()
    schedule_sites(schedule, check_website, sites)
    actual_jobs = list(map(dump_job, schedule.get_jobs()))
    assert actual_jobs == EXPECTED_JOBS
