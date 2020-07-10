import time

from prometheus_client import Gauge

METRICS_PREFIX = 'hitcounter'

cached_data = None
last_fetched = 0


def init_metrics(db_connection):
    g = Gauge(f'{METRICS_PREFIX}_hits_total', 'Total number of hits', ['site', 'path'])

    url_counts = db_connection.getTopUrls(db_connection.get_connection(), -1)
    for url in url_counts['urls']:
        site, path = _split_url(url)
        g.labels(site, path).set_function(_get_resolver(db_connection, site, path))


def resolve_label_count(db, site, path):
    global cached_data, last_fetched

    if time.monotonic() - last_fetched >= 5:
        cached_data = db.getTopUrls(db.get_connection(), -1)
        last_fetched = time.monotonic()

    url = f'{site}/{path}'
    if url not in cached_data['values']:
        return 0
    return cached_data['values'][url]


def _split_url(url):
    split = url.split('/')
    return split[0], '/'.join(split[1:])


def _get_resolver(db, for_site, for_path):
    def call():
        return resolve_label_count(db, for_site, for_path)

    return call
