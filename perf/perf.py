import os
import sys
import shutil
import xml.etree.ElementTree as ET
from radicale import Application
from radicale.config import load
from datetime import datetime, timedelta
from logging import getLogger
from time import time
from vobject import iCalendar

L = int(sys.argv[1]) if len(sys.argv) > 1 else 10000

collection_folder = os.path.join(
    os.path.dirname(__file__), 'collections')

environ = {
    'PATH_INFO': 'test',
    'CONTENT_TYPE': 'VADDRESSBOOK',
}

mkcal = '''<?xml version="1.0" encoding="utf-8" ?>
<C:mkcalendar xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
    <D:set>
        <D:prop>
            <D:displayname>Test calendar</D:displayname>
        </D:prop>
    </D:set>
</C:mkcalendar>
'''


def gen_events():
    cal = iCalendar()
    d0 = datetime(2016, 1, 1)

    for i in range(L):
        ds = d0 + timedelta(days=(365 * (i / L)))
        dt = ds + timedelta(hours=1)
        # dt = ds + timedelta(seconds=7 * 24 * 3600 * i / L)
        e = cal.add('vevent')
        e.add('uid').value = 'u%d' % ds.timestamp()
        e.add('dtstart').value = ds
        e.add('dtend').value = dt
        # print(ds, dt)
    return cal.serialize()

test_events = gen_events()


def get_col():
    return list(app.Collection.discover(environ['PATH_INFO'], '0'))

for type in ('radicale_storage_by_index', ):
    if len(sys.argv) > 2 and type not in sys.argv[2:]:
        continue
    shutil.rmtree(collection_folder, True)
    os.mkdir(collection_folder)
    app = Application(
        load(extra_config=dict(
            storage=dict(
                type=type,
                filesystem_folder=collection_folder
            ))), getLogger('rt'))

    collections = get_col()
    app.do_MKCALENDAR(environ, collections, collections, mkcal, None)

    collections = get_col()
    t0 = time()
    app.do_PUT(environ, collections, collections, test_events, None)
    print('PUT using %s %f' % (type, time() - t0))

    def f(y):
        return y['y'], y['m'], y['d'], y['h']

    ymdh_start = dict(y=2016, m=1, d=1, h=15)
    for scale in ('h', ): #'d', 'w', 'm', 'y'):
        i = 1
        scale_ = scale
        if scale == 'w':
            scale_ = 'd'
            i = 7

        ymdh_end = dict(ymdh_start)
        ymdh_end[scale_] += i
        report = '''<?xml version="1.0" encoding="utf-8" ?>
        <C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav">
            <D:prop xmlns:D="DAV:">
              <D:getetag/>
              <C:calendar-data/>
            </D:prop>
            <C:filter>
              <C:comp-filter name="VCALENDAR">
                  <C:comp-filter name="VEVENT">
                    <C:time-range
                        start="%d%02d%02dT%02d0000Z"
                        end="%d%02d%02dT%02d0000Z"/>
                  </C:comp-filter>
              </C:comp-filter>
            </C:filter>
        </C:calendar-query>
        ''' % (f(ymdh_start) + f(ymdh_end))
        collections = get_col()
        t1 = time()
        r = app.do_REPORT(
            environ, collections, collections, report, None)
        t2 = time()
        root = ET.fromstring(r[2])
        node_name = ".//{urn:ietf:params:xml:ns:caldav}calendar-data"
        calendars = root.findall(node_name)
        print('REPORT using %s for %s found [%d] %f' % (
            type, scale, len(calendars), t2 - t1))

# shutil.rmtree(collection_folder, True)
