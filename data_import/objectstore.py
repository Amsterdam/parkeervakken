import logging
import os

from swiftclient.client import Connection

from dateutil import parser

logging.basicConfig(level=logging.DEBUG)

log = logging.getLogger(__name__)


logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("swiftclient").setLevel(logging.WARNING)

destination_dir = "data/"

assert os.getenv('PARKEERVAKKEN_OS_PASSWORD')

global container

container = 'parkeren'

object_store = {
    'auth_version': '2.0',
    'authurl': 'https://identity.stack.cloudvps.com/v2.0',
    'user': os.getenv('OBJECTSTORE_USER', 'parkeren'),
    'key': os.getenv('PARKEERVAKKEN_OS_PASSWORD', 'insecure'),
    'tenant_name': 'BGE000081_Parkeren',
    'os_options': {
        'tenant_id': 'fd380ccb48444960837008800a453122',
        'region_name': 'NL',
    }
}

parkeren_conn = Connection(**object_store)


def get_store_object(object_meta_data):
    """ get a single file from object store """
    return parkeren_conn.get_object(
        container, object_meta_data['name'])[1]


def save_file(time, object_meta_data):
    zipname = object_meta_data['name'].split('/')[-1]
    log.info('Downloading latest: %s %s', time, zipname)
    latest_zip = get_store_object(object_meta_data)

    # create the directory inclusive nonexisting path
    os.makedirs('data/parkeren/', exist_ok=True)

    # save output to file!
    with open('data/parkeren/{}'.format(zipname), 'wb') as outputzip:
        outputzip.write(latest_zip)


def get_latest_zipfile():
    """
    Get latest zipfile uploaded by mks
    """
    niet_fiscaal = []
    zip_list = []

    meta_data = get_full_container_list(
        parkeren_conn, container)

    for o_info in meta_data:
        if o_info['content_type'] in [
                'application/zip', 'application/octet-stream']:
            dt = parser.parse(o_info['last_modified'])
            if 'nietfiscaal' in o_info['name']:
                niet_fiscaal.append((dt, o_info))
                continue
            zip_list.append((dt, o_info))

    zips_sorted_by_time = sorted(zip_list)
    nf_zips_sorted_by_time = sorted(niet_fiscaal)

    log.info('Available files..')

    for time, meta in zips_sorted_by_time:
        log.info('%s %s', time, meta['name'])

    for time, meta in nf_zips_sorted_by_time:
        log.info('%s %s', time, meta['name'])

    if not zips_sorted_by_time:
        raise ValueError

    if not nf_zips_sorted_by_time:
        raise ValueError

    time, object_meta_data = zips_sorted_by_time[-1]

    # Download the latest data
    save_file(time, object_meta_data)

    # Download laatste niet fiscaal data
    time, object_meta_data = nf_zips_sorted_by_time[-1]
    save_file(time, object_meta_data)


def get_full_container_list(conn, container, **kwargs):
    limit = 10000
    kwargs['limit'] = limit
    page = []

    seed = []

    _, page = conn.get_container(container, **kwargs)
    seed.extend(page)

    while len(page) == limit:
        # keep getting pages..
        kwargs['marker'] = seed[-1]['name']
        _, page = conn.get_container(container, **kwargs)
        seed.extend(page)

    return seed


if __name__ == "__main__":
    # Download files from objectstore
    get_latest_zipfile()
