import re
import shutil
import tempfile
from datetime import datetime, timezone

from radicale import Application
from radicale.tests.test_base import TestCustomStorageSystem, get_file_content


def ts(dt):
    return dt.replace(tzinfo=timezone.utc).timestamp()


class TestStorageByIndex(TestCustomStorageSystem):
    """Base class for custom backend tests."""
    storage_type = "radicale_storage_by_index"

    def setup(self):
        super().setup()
        self.colpath = tempfile.mkdtemp()
        self.configuration.set("storage", "filesystem_folder", self.colpath)
        self.configuration.set("storage", 'radicale_storage_by_index_fields',
                               'dtstart, dtend, uid, summary, organizer')
        self.application = Application(self.configuration, self.logger)

    @property
    def db(self):
        return self.application.Collection('calendar.ics').db

    def teardown(self):
        shutil.rmtree(self.colpath)

    def test_index_add_event(self):
        """Add an event."""
        self.request("MKCOL", "/calendar.ics/")
        assert len(list(self.db.list())) == 0

        self.request(
            "PUT", "/calendar.ics/", "BEGIN:VCALENDAR\r\nEND:VCALENDAR")

        assert len(list(self.db.list())) == 0

        event = get_file_content("event1.ics")
        path = "/calendar.ics/event1.ics"
        status, headers, answer = self.request("PUT", path, event)

        index = list(self.db.list())
        assert len(index) == 1
        assert index[0] == (
            'event1.ics',
            0,
            ts(datetime(2013, 9, 1, 16, 0, 0)),
            ts(datetime(2013, 9, 1, 17, 0, 0)),
            'event1',
            'Event',
            'mailto:unclesam@example.com')

    def test_index_multiple_events_with_same_uid(self):
        """Add two events with the same UID."""
        self.request("MKCOL", "/calendar.ics/")

        self.request("PUT", "/calendar.ics/", get_file_content("event2.ics"))
        status, headers, answer = self.request(
            "REPORT", "/calendar.ics/",
            '<?xml version="1.0" encoding="utf-8" ?>'
            '<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav">'
            '</C:calendar-query>')

        uids = re.findall(r'<href>/calendar.ics/(.+)</href>', answer)

        index = list(self.db.list())
        assert len(index) == 1
        assert index[0] == (
            uids[0],
            1,
            ts(datetime(2013, 9, 2, 16, 0, 0)),
            ts(datetime(2013, 9, 2, 17, 0, 0)),
            'event2',
            'Event2',
            None)

    def test_index_update(self):
        """Update an event."""
        self.request("MKCOL", "/calendar.ics/")

        assert len(list(self.db.list())) == 0

        self.request(
            "PUT", "/calendar.ics/", "BEGIN:VCALENDAR\r\nEND:VCALENDAR")
        event = get_file_content("event1.ics")
        path = "/calendar.ics/event1.ics"
        status, headers, answer = self.request("PUT", path, event)
        assert status == 201

        index = list(self.db.list())
        assert len(index) == 1
        assert index[0] == (
            'event1.ics',
            0,
            ts(datetime(2013, 9, 1, 16, 0, 0)),
            ts(datetime(2013, 9, 1, 17, 0, 0)),
            'event1',
            'Event',
            'mailto:unclesam@example.com')

        # Then we send another PUT request
        event = get_file_content("event1-prime.ics")
        status, headers, answer = self.request("PUT", path, event)
        assert status == 201

        index = list(self.db.list())
        assert len(index) == 1
        assert index[0] == (
            'event1.ics',
            0,
            ts(datetime(2014, 9, 1, 16, 0, 0)),
            ts(datetime(2014, 9, 1, 19, 0, 0)),
            'event1',
            'Event',
            'mailto:unclesam@example.com')

    def test_index_delete(self):
        """Delete an event."""
        self.request("MKCOL", "/calendar.ics/")

        assert len(list(self.db.list())) == 0

        self.request(
            "PUT", "/calendar.ics/", "BEGIN:VCALENDAR\r\nEND:VCALENDAR")
        event = get_file_content("event1.ics")
        path = "/calendar.ics/event1.ics"
        status, headers, answer = self.request("PUT", path, event)

        assert len(list(self.db.list())) == 1

        # Then we send a DELETE request
        status, headers, answer = self.request("DELETE", path)

        assert len(list(self.db.list())) == 0

    def test_reindex(self):
        self.request("MKCOL", "/calendar.ics/")
        assert len(list(self.db.list())) == 0
        self.request(
            "PUT", "/calendar.ics/", "BEGIN:VCALENDAR\r\nEND:VCALENDAR")
        for i in range(1, 6):
            e = 'event%d.ics' % i
            event = get_file_content(e)
            path = "/calendar.ics/%s" % e
            status, headers, answer = self.request("PUT", path, event)
            assert status == 201

        index = set(self.db.list())
        db = self.db
        assert len(list(db.list())) == 5
        db.cursor.execute('DELETE FROM by_index_events')
        db.connection.commit()

        assert len(list(db.list())) == 0
        assert len(list(self.db.list())) == 0
        db.cursor.execute('UPDATE by_index_version SET version = 0')
        db.connection.commit()
        assert len(list(db.list())) == 0
        # Reindexing
        assert len(list(self.db.list())) == 5

        assert index == set(self.db.list())

    def test_complex_report(self):
        self.request("MKCOL", "/calendar.ics/")
        assert len(list(self.db.list())) == 0
        self.request(
            "PUT", "/calendar.ics/", "BEGIN:VCALENDAR\r\nEND:VCALENDAR")
        for i in range(1, 6):
            e = 'event%d.ics' % i
            event = get_file_content(e)
            path = "/calendar.ics/%s" % e
            status, headers, answer = self.request("PUT", path, event)
            assert status == 201

        status, headers, answer = self.request(
            "REPORT", "/calendar.ics/",
            '<?xml version="1.0" encoding="utf-8" ?>'
            '<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav">'
            '<C:filter>'
            '   <C:comp-filter name="VCALENDAR">'
            '     <C:comp-filter name="VEVENT">'
            '       <C:prop-filter name="UID">'
            '         <C:text-match>event</C:text-match>'
            '       </C:prop-filter>'
            '       <C:prop-filter name="SUMMARY">'
            '         <C:text-match'
            '            negate-condition="yes">Event4</C:text-match>'
            '       </C:prop-filter>'
            '     </C:comp-filter>'
            '   </C:comp-filter>'
            ' </C:filter>'
            '</C:calendar-query>')

        uids = re.findall(r'<href>/calendar.ics/(.+)</href>', answer)
        assert uids == ['event1.ics', 'event2.ics', 'event3.ics', 'event5.ics']
