import asyncio
from datetime import datetime
from functools import partial
import logging
from typing import Union, Optional
from uuid import uuid4
import re

import aiohttp
from aiohttp import ClientTimeout
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import broker
from broker import get_kafka_producer, KafkaProducer, get_kafka_admin, create_topic_if_not_exists
from sites_loader import parse_sites_file, schedule_sites


log = logging.getLogger('app')

# define settings default values
LOG_LEVEl = logging.DEBUG
DEFAULT_TIMEOUT = ClientTimeout(total=3)
SITES_FILE = 'conf/sites.json'

# override settings by local values
try:
    from conf.local_settings import *       # noqa: F401, F403
except ImportError:
    pass


def create_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop


def check_resp(
        check_name: str,
        req_id: str,
        req_dt: datetime,
        res_status: int,
        res_text: str,
        exp_status: int = None,
        regexp: Optional[Union[str, re.Pattern]] = None):

    health = False
    res_sample = None
    res_length = len(res_text) if res_text else None
    res_duration = (datetime.now() - req_dt).total_seconds()

    if res_status and not exp_status or res_status == exp_status:
        health = not regexp
        if regexp and res_length and res_text:
            if not isinstance(regexp, re.Pattern):
                regexp = re.compile(regexp)
            res_sample = regexp.search(res_text)
            if res_sample:
                res_sample = res_sample.group()
                health = True

    result = {
        'id': req_id,
        'check_name': check_name,
        'dt': req_dt.isoformat(),
        'health': health,
        'status': res_status,
        'duration': res_duration,
        'length': res_length,
        'sample': res_sample,
    }
    return result


async def do_request(
        check_name: str,
        req_id: str,
        url: str,
        method: str = 'GET',
        timeout: Union[float, tuple] = None,
        **kwargs
):
    timeout = timeout or DEFAULT_TIMEOUT
    try:
        log.debug(f'start  req {req_id} {check_name} {url}')

        async with aiohttp.request(method, url, timeout=timeout, **kwargs) as resp:
            text = await resp.text()
            log.debug(f'finish req {req_id} with {resp.status} {check_name} {url}')
            return resp.status, text
    except (asyncio.TimeoutError, aiohttp.ClientError):
        return None, None


async def do_check(
        check_name: str,
        url: str,
        method: str = None,
        timeout: Union[float, tuple] = None,
        status: int = None,
        regexp: Optional[Union[str, re.Pattern]] = None,
        **kwargs) -> dict:
    method = method or 'GET'

    req_id = str(uuid4())
    req_dt = datetime.now()
    res_status, res_text = await do_request(
        check_name=check_name, req_id=req_id, url=url, method=method, timeout=timeout, **kwargs)

    return check_resp(check_name, req_id, req_dt, res_status=res_status, res_text=res_text, exp_status=status,
                      regexp=regexp)


async def check_website(
        producer: KafkaProducer,
        check_name: str,
        url: str,
        method: str = None,
        timeout: Union[float, tuple] = None,
        status: int = None,
        regexp: Optional[Union[str, re.Pattern]] = None,
        topic_name: str = None,
        **kwargs
):
    topic_name = topic_name or broker.TOPIC_NAME
    method = method or 'GET'
    res = await do_check(check_name, url, method=method, timeout=timeout, status=status, regexp=regexp, **kwargs)
    producer.send(topic_name, res)


def start_checking(producer: KafkaProducer, sites: dict, timeout: float = None):
    loop = create_loop()

    schedule = AsyncIOScheduler(event_loop=loop)
    schedule_sites(schedule, partial(check_website, producer), sites=sites)
    schedule.start()

    try:
        if timeout:
            loop.run_until_complete(asyncio.sleep(timeout))
        else:
            loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')
    log.setLevel(LOG_LEVEl)

    sites = parse_sites_file(SITES_FILE)
    create_topic_if_not_exists(admin=get_kafka_admin(), topic_name=broker.TOPIC_NAME)
    producer = get_kafka_producer()
    start_checking(producer=producer, sites=sites)
    producer.close()
