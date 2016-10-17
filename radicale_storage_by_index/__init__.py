import os
import sqlite3
from collections import OrderedDict
from datetime import date, datetime, time, timezone
from logging import INFO, getLogger

from radicale.storage import Collection as FileSystemCollection
from radicale.xmlutils import _tag

log = getLogger('radicale.storage.by_index')
log.setLevel(INFO)


class Not(str):
    pass


class Db(object):
    __version__ = '1'

    def __init__(self, folder, fields, collection,
                 file_name=".Radicale.index.db"):
        self._connection = None
        self.fields = fields
        self.collection = collection
        self.db_path = os.path.join(folder, file_name)

    @property
    def connection(self):
        if not self._connection:
            create = False
            if not os.path.exists(self.db_path):
                create = True
                os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self._connection = sqlite3.connect(self.db_path)
            if log.level <= INFO:
                self._connection.set_trace_callback(log.info)

            if not create:
                try:
                    self._connection.cursor().execute(
                        'SELECT href FROM by_index_events').fetchone()
                except sqlite3.Error:
                    create = True
            if not create:
                try:
                    version = self._connection.cursor().execute(
                        'SELECT version FROM by_index_version').fetchone()[0]
                except sqlite3.Error:
                    version = None
                create = version != self.__version__
            if not create:
                try:
                    fields = self._connection.cursor().execute(
                        'SELECT field FROM by_index_fields').fetchall()
                except sqlite3.Error:
                    raise
                    fields = None
                create = set(fields) == set(self.fields)

            if create:
                self.create_database(self._connection)
                if any(e for e in FileSystemCollection.list(self.collection)):
                    self.reindex(self._connection)
        return self._connection

    @property
    def cursor(self):
        return self.connection.cursor()

    @property
    def columns(self):
        return ', '.join(self.fields)

    @property
    def columns_placeholder(self):
        return ', '.join(['?' for _ in self.fields])

    def create_database(self, connection):
        log.info('Creating database %s' % self.db_path)

        cursor = connection.cursor()
        cursor.execute('DROP TABLE IF EXISTS by_index_events')
        cursor.execute('DROP TABLE IF EXISTS by_index_fields')
        cursor.execute('DROP TABLE IF EXISTS by_index_version')

        cursor.execute('CREATE TABLE by_index_version (version)')
        cursor.execute(
            'INSERT INTO by_index_version (version) VALUES (?)',
            self.__version__)
        cursor.execute('CREATE TABLE by_index_fields (field)')
        self.cursor.executemany(
            'INSERT INTO by_index_fields VALUES (?)',
            [(f, ) for f in self.fields])
        cursor.execute(
            'CREATE TABLE by_index_events '
            '(href PRIMARY KEY, recurrent, %s)' % self.columns)
        connection.commit()

    def reindex(self, connection):
        log.warn('Reindexing %s' % self.db_path)
        self.add_all([
            self.collection.get_db_params(FileSystemCollection.get(
                self.collection, href))
            for href in FileSystemCollection.list(self.collection)
        ])

    def upsert(self, href, recurrent, *fields):
        self.cursor.execute(
            'INSERT OR REPLACE INTO by_index_events (href, recurrent, %s) '
            'VALUES (?, ?, %s)' % (self.columns, self.columns_placeholder),
            (href, recurrent, *fields))
        self.connection.commit()

    def add_all(self, lines):
        self.cursor.executemany(
            'INSERT OR REPLACE INTO by_index_events (href, recurrent, %s) '
            'VALUES (?, ?, %s)' % (self.columns, self.columns_placeholder),
            lines)
        self.connection.commit()

    def list(self):
        try:
            for result in self.cursor.execute(
                'SELECT href, recurrent, %s FROM by_index_events' %
                self.columns
            ):
                yield result
        finally:
            self.connection.rollback()

    def search(self, **fields):
        fields = OrderedDict(fields)

        def get_comparator(name, value):
            # If the event finished before the start of the query,
            # There's a chance it will recur after
            if name == 'dtstart':
                return '? <= dtend or recurrent'  # We don't index recurrences
            # If the event start after the end of the query we don't care
            if name == 'dtend':
                return '? >= dtstart'
            comparator = ''
            if isinstance(value, Not):
                comparator = 'NOT '
            fields[name] = '%%%s%%' % value
            return comparator + '%s like ?' % name

        query = ') AND ('.join([
            get_comparator(name, value) for name, value in fields.items()
        ])

        try:
            for result in self.cursor.execute(
                    'SELECT href FROM by_index_events WHERE (%s)' %
                    query, list(fields.values())):
                yield result
        finally:
            self.connection.rollback()

    def delete(self, href):
        if href is not None:
            self.cursor.execute(
                'DELETE FROM by_index_events WHERE href = ?', (href,))
        else:
            self.cursor.execute('DELETE FROM by_index_events')
        self.connection.commit()


class Collection(FileSystemCollection):
    def __init__(self, path, principal=False, folder=None):
        super().__init__(path, principal, folder)
        self.fields = list(
            map(lambda x: x.strip(), self.configuration.get(
                'storage', 'radicale_storage_by_index_fields',
                fallback='dtstart, dtend, uid').split(',')))
        self.db = Db(self._filesystem_path, self.fields, self)

    def dt_to_timestamp(self, dt):
        if dt.tzinfo is None:
            # Naive dates to utc
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()

    def _fill_request(self, filters, request):
        for filter_ in filters:
            if filter_.tag in [_tag("C", "filter"), _tag("C", "comp-filter")]:
                self._fill_request(filter_, request)
            elif filter_.tag == _tag("C", "time-range"):
                request['dtstart'] = filter_.get('start')
                request['dtend'] = filter_.get('end')
            if filter_.tag == _tag("C", "prop-filter"):
                assert filter_[0].tag == _tag("C", "text-match")
                text = filter_[0].text
                if filter_[0].get('negate-condition') == 'yes':
                    text = Not(text)
                key = filter_.get('name').lower().replace('-', '_')
                request[key] = text

    def pre_filtered_list(self, filters):
        # Get request
        request = {}
        self._fill_request(filters, request)
        if not request:
            return super().pre_filtered_list(filters)
        if 'dtstart' in request:
            request['dtstart'] = self.dt_to_timestamp(
                datetime.strptime(request['dtstart'], "%Y%m%dT%H%M%SZ"))
        if 'dtend' in request:
            request['dtend'] = self.dt_to_timestamp(
                datetime.strptime(request['dtend'], "%Y%m%dT%H%M%SZ"))

        return [self.get(href) for href, in self.db.search(**request)]

    def get_db_params(self, item):
        if hasattr(item.item, 'vevent'):
            vobj = item.item.vevent
        elif hasattr(item.item, 'vtodo'):
            vobj = item.item.vtodo
        elif hasattr(item.item, 'vjournal'):
            vobj = item.item.vjournal

        recurrent = bool(getattr(vobj, 'rruleset', False))
        values = []
        for field in self.fields:
            value = None
            if hasattr(vobj, field):
                value = getattr(vobj, field).value
                if field in ['dtstart', 'dtend']:
                    if not isinstance(
                            value, datetime) and isinstance(value, date):
                        value = datetime.combine(
                            value, time.min if field == 'dtstart'
                            else time.max)
                    value = self.dt_to_timestamp(value)
            values.append(value)
        return (item.href, recurrent, *values)

    def upload(self, href, vobject_item):
        item = super().upload(href, vobject_item)
        if item:
            self.db.upsert(*self.get_db_params(item))
        return item

    def upload_all_nonatomic(self, collections):
        # TODO: See why super() does not work
        self.db.add_all([
            self.get_db_params(
                super(Collection, self).upload(href, vobject_item)
            ) for href, vobject_item in collections.items()
        ])

    def delete(self, href=None):
        self.db.delete(href)
        super().delete(href)
