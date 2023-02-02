import asyncio
from datetime import datetime, timedelta
from unittest import mock
from uuid import UUID

from aiohttp import ClientTimeout
import pytest
from pytest import mark, param

import broker
from health_checker import check_resp, do_check, check_website


def mock_resp(content, status_code=200, text=None, elapsed=timedelta(seconds=0.01)):
    resp = mock.Mock()
    resp.status_code = status_code
    resp.content = content
    resp.text = text or (content or b'').decode('utf-8')
    resp.elapsed = elapsed
    return resp


@pytest.fixture()
def mock_producer():
    res = mock.Mock()
    mock_producer.send = mock.Mock()
    return res


TEST_RESP1 = (200, 'aaa')
TEST_RESP2 = (201, 'pp23ss')
TEST_RESP3 = (500, 'internal server error')


@mark.parametrize("check_name, req_id, req_dt, status, regexp, res_status, res_text, expected", [
    param('test1', '6301e00b-65b3-411d-97c8-3ec1da1e5eb5', datetime(2023, 1, 1), 200, None, *TEST_RESP1,
          {'check_name': 'test1', 'id': '6301e00b-65b3-411d-97c8-3ec1da1e5eb5', 'dt': '2023-01-01T00:00:00',
           'health': True, 'status': 200, 'duration': 1.5, 'length': 3, 'sample': None}),
    param('test2', '94e0c1ac-8644-4ceb-86a3-560fde1ad2c0', datetime(2023, 1, 1), None, r'(?<=pp)\d+(?=ss)', *TEST_RESP2,
          {'check_name': 'test2', 'id': '94e0c1ac-8644-4ceb-86a3-560fde1ad2c0', 'dt': '2023-01-01T00:00:00',
           'health': True, 'status': 201, 'duration': 0.1, 'length': 6, 'sample': '23'}),
    param('test3', '824ae7a8-28a2-41c5-89ae-5f9912abc7a4', datetime(2023, 1, 1), 200, None, *TEST_RESP3,
          {'check_name': 'test3', 'id': '824ae7a8-28a2-41c5-89ae-5f9912abc7a4', 'dt': '2023-01-01T00:00:00',
           'health': False, 'status': 500, 'duration': 0.5, 'length': 21, 'sample': None}),
])
def test_check_response(check_name: str, req_id: str, req_dt: datetime, status: int, regexp: str,
                        res_status: int, res_text: str, expected: dict):
    actual = check_resp(check_name=check_name, req_id=req_id, req_dt=req_dt,
                        res_status=res_status, res_text=res_text, exp_status=status, regexp=regexp)
    expected.pop('duration')
    actual.pop('duration')
    assert actual == expected


def set_mock_request(mock_obj, status, text):
    mock_obj.return_value.__aenter__.return_value.status = status

    async def text_f():
        return text

    mock_obj.return_value.__aenter__.return_value.text = text_f


@mark.parametrize("name, url, method, status, regexp, res_status, res_text, expected_call, expected", [
    param('test1', 'https://google.com', 'GET', 200, None, *TEST_RESP1,
          (('GET', 'https://google.com'), {'timeout': ClientTimeout(total=3)}),
          {'check_name': 'test1', 'health': True, 'status': 200, 'duration': 1.5, 'length': 3, 'sample': None}),
    param('test2', 'https://stackoverflow.com', 'POST', None, r'(?<=pp)\d+(?=ss)', *TEST_RESP2,
          (('POST', 'https://stackoverflow.com'), {'timeout': ClientTimeout(3)}),
          {'check_name': 'test2', 'health': True, 'status': 201, 'duration': 0.1, 'length': 6, 'sample': '23'}),
    param('test3', 'http://localhost', None, 200, None, *TEST_RESP3,
          (('GET', 'http://localhost'), {'timeout': ClientTimeout(3)}),
          {'check_name': 'test3', 'health': False, 'status': 500, 'duration': 0.5, 'length': 21, 'sample': None}),
])
@mock.patch('aiohttp.request')
def test_do_check(mock_request, name: str, url: str, method: str, status: int, regexp: str,
                  res_status: int, res_text: str, expected_call, expected: dict):

    set_mock_request(mock_request, res_status, text=res_text)
    resp = asyncio.run(do_check(name, url, method=method, status=status, regexp=regexp))

    actual_args, actual_kwargs = mock_request.call_args
    assert (actual_args, actual_kwargs) == expected_call

    expected.pop('duration')
    resp.pop('duration')
    assert UUID(resp.pop('id'))
    assert datetime.fromisoformat(resp.pop('dt'))
    assert resp == expected


@mark.parametrize("name, url, method, status, regexp, expected_call_do_check, do_check_res", [
    param('test1', 'https://google.com', 'GET', 200, None,
          (('test1', 'https://google.com'), {'method': 'GET', 'status': 200, 'regexp': None, 'timeout': None}),
          {'check_name': 'test1', 'health': True, 'status': 200, 'duration': 1.5, 'length': 3, 'sample': None}),
    param('test2', 'https://stackoverflow.com', 'POST', None, r'(?<=pp)\d+(?=ss)',
          (('test2', 'https://stackoverflow.com'), {'method': 'POST', 'status': None, 'regexp': r'(?<=pp)\d+(?=ss)',
                                                    'timeout': None}),
          {'check_name': 'test2', 'health': True, 'status': 201, 'duration': 0.1, 'length': 6, 'sample': '23'}),
    param('test3', 'http://localhost', None, 200, None,
          (('test3', 'http://localhost'), {'method': 'GET', 'status': 200, 'regexp': None, 'timeout': None}),
          {'check_name': 'test3', 'health': False, 'status': 500, 'duration': 0.5, 'length': 21, 'sample': None}),
])
@mock.patch('health_checker.do_check')
def test_check_website(mock_do_check, mock_producer, name: str, url: str, method: str, status: int, regexp: str,
                       expected_call_do_check, do_check_res: dict):

    mock_do_check.return_value = do_check_res

    asyncio.run(check_website(
        mock_producer,
        check_name=name,
        url=url,
        method=method,
        status=status,
        regexp=regexp,
    ))

    actual_args, actual_kwargs = mock_do_check.call_args
    assert (actual_args, actual_kwargs) == expected_call_do_check

    expected_call_provider_send = ((broker.TOPIC_NAME, do_check_res), {})
    actual_args, actual_kwargs = mock_producer.send.call_args
    assert (actual_args, actual_kwargs) == expected_call_provider_send
