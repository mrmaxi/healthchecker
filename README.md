# Health-checker

Tool for checking website availability and store results in pg-database.

### Key features

- asynchronous checking of multiple sites the same time
- passing results to pg-db through kafka
- logging website response latency
- defining regex for checking website response and store result 

## Using

Install the dependencies:
```bash
pip install -r requirements.txt
```

Setup kafka and pg endpoints in [conf/local_settings.py](conf/local_settings.py) file
```python
LOG_LEVEL = 'DEBUG'
SITES_FILE = 'conf/sites.json'

TOPIC_NAME = "health_checker"
KAFKA_BOOTSTRAP_SERVERS = f"127.0.0.1:9092"
KAFKA_CLIENT_ID = "CONSUMER_CLIENT_ID1"
KAFKA_GROUP_ID = "CONSUMER_GROUP_ID"

PG_CONNECTION_STR = 'postgres://postgres:mysecretpassword@127.0.0.1:5432/postgres'
```

Kafka topic and database table will be created if not exists.

Put your kafka connection key and certs into [conf](conf) directory:
```
└── conf/
    ├── service.key
    ├── service.crt
    └── ca.pem   
```

Define your sites list [conf/sites.json](conf/sites.json):
```json
{
  "stackoverflow": {
    "url": "https://stackoverflow.com/",
    "seconds": 5,
    "method": "GET",
    "auth": ["user", "pass"],
    "headers": {
      "X-API-KEY": "123456789AAA"
    },
    "timeout": 3,
    "regexp": "(?<=Explore technical topics and other disciplines across )\\d+(?=\\+ Q&A)"
  },
  "google": {
    "url": "https://google.com/",
    "seconds": 5,
    "status": null
  }
}
```

## Parameters description:

### Trigger settings - check every N:
- `weeks`
- `days`
- `hours` 
- `minutes`
- `seconds`

### Request settings:
- `url` - url for checking 
- `method` - optional request method: `GET` (default), `POST`, `PUT`, ...
- `headers` - optional dict of headers
- `auth` - optional basic auth as ("user", "pass")
- `data` - optional str body in utf-8 
- `json` - optional json body
- `timeout` - optional timeout for request default 3s

### Check settings:
- `status` - optional expected response status, 200 - by default, null - do not check
- `regexp` - regex for searching in response content and storing into database 

Run health_checker:
```bash
python health_checker.py
```

Run db_writer:
```bash
python db_writer.py
```


## Testing
### Unit tests
Execute:
```bash
pytest tests/unit
```

### Integration tests
***Be careful, all data in temporary database and broker may be lost***

Configure temporary pg-db and kafka topic for run integration tests [tests/integration/local_settings.py](tests/integration/local_settings.py).

```python
PG_CONNECTION_STR = 'postgres://postgres:mysecretpassword@127.0.0.1:5432/postgres'

TOPIC_NAME = "test_topic"

KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
KAFKA_CLIENT_ID = "CONSUMER_CLIENT_ID1"
KAFKA_GROUP_ID = "CONSUMER_GROUP_ID"
```

Execute:
```bash
pytest tests/integration
```
