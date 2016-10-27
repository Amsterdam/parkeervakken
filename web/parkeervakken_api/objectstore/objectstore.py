import logging
import os
from swiftclient.client import Connection
from django.conf import settings

log = logging.getLogger(__name__)

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("swiftclient").setLevel(logging.WARNING)

data_dir = settings.DIVA_DIR
bag_connection = Connection(**settings.OBJECTSTORE)


def fetch_importfiles():
    """
    Fetches all files mentioned in prefixes and writes them to `data_dir`/prefixes[1]
    :return:
    """
    store = ObjectStore('BAG')
    prefixes = [ ('parkeren', {'dir_name': '', 'prefix': []})]

    for s, f in prefixes:
        os.makedirs(os.path.join(data_dir, f['dir_name']), exist_ok=True)
        numfiles = len(f['prefix'])
        if numfiles > 0:
            # get the latest modified files, by reverse sorting then on `last_modified` and take the number of
            # unique prefixes.
            container_list = sorted(store._get_full_container_list([], prefix='{}/'.format(s)),
                                    key=lambda l: l['last_modified'], reverse=True)[:numfiles]
        else:
            container_list = store._get_full_container_list([], prefix='{}/'.format(s))

        for ob in container_list:
            fname = os.path.join(data_dir, f['dir_name'], ob['name'].split('/')[-1])
            newfile = open(fname, 'wb')
            newfile.write(store.get_store_object(ob['name']))
            newfile.close()


class ObjectStore():
    RESP_LIMIT = 10000  # serverside limit of the response

    def __init__(self, container):
        self.conn = Connection(**settings.OBJECTSTORE)
        self.container = container

    def get_store_object(self, name):
        """
        Returns the object store
        :param object_meta_data:
        :return:
        """
        return self.conn.get_object(self.container, name)[1]

    def get_store_objects(self, path):
        return self._get_full_container_list([], prefix=path)

    def _get_full_container_list(self, seed, **kwargs):
        kwargs['limit'] = self.RESP_LIMIT
        if len(seed):
            kwargs['marker'] = seed[-1]['name']

        _, page = self.conn.get_container(self.container, **kwargs)
        seed.extend(page)
        return seed if len(page) < self.RESP_LIMIT else \
            self._get_full_container_list(seed, **kwargs)

    def folders(self, path):
        objects_from_store = self._get_full_container_list(
            [], delimiter='/', prefix=path
        )
        return [store_object['subdir'] for store_object in objects_from_store if 'subdir' in store_object]

    def files(self, path, file_id):
        file_list = self._get_full_container_list(
            [], delimiter='/', prefix=path + file_id)

        for file_object in file_list:
            file_object['container'] = self.container
        return file_list

    def put_to_objectstore(self, object_name, object_content, content_type):
        return self.conn.put_object(self.container, object_name, contents=object_content, content_type=content_type)

    def delete_from_objectstore(self, object_name):
        return self.conn.delete_object(self.container, object_name)
