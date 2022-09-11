# messybrainz-server - Server for the MessyBrainz project
#
# Copyright (C) 2017 MetaBrainz Foundation Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA)

from listenbrainz import messybrainz
from listenbrainz.db import timescale
from listenbrainz.db.testing import TimescaleTestCase


recording = {
    'artist': 'Frank Ocean',
    'release': 'Blond',
    'title': 'Pretty Sweet',
    'additional_info': {
        'key1': 'Value1',
        'track_number': '5/12',
        'duration': 50000,
    },
    'recording_mbid': "5465ca86-3881-4349-81b2-6efbd3a59451"
}

recording2 = {
    'artist': 'FRANK OCEAN',
    'release': 'BLoNd',
    'title': 'PReTtY SWEET',
    'additional_info': {
        'key1': 'VaLue1',
        'track_number': '5/12',
        'duration': 50000
    },
    'recording_mbid': "5465ca86-3881-4349-81b2-6efbd3a59451"
}


class DataTestCase(TimescaleTestCase):

    def test_submit_recording(self):
        with timescale.engine.begin() as connection:
            title, artist, release, track_number, duration = \
                recording["title"], recording["artist"], recording["release"],\
                recording["additional_info"]["track_number"], recording["additional_info"]["duration"]
            recording_msid = messybrainz.submit_recording(connection, title, artist, release, track_number, duration)
            received_msid_1 = messybrainz.get_msid(connection, title, artist, release, track_number, duration)
            self.assertEqual(recording_msid, received_msid_1)

            results = connection.execute("SELECT * FROM messybrainz.submissions")
            for row in results.fetchall():
                print(row)

            recording_msid_2 = messybrainz.submit_recording(connection, title, artist, release)
            received_msid_2 = messybrainz.get_msid(connection, title, artist, release)
            results = connection.execute("SELECT * FROM messybrainz.submissions")
            for row in results.fetchall():
                print(row)
            self.assertEqual(recording_msid_2, received_msid_2)

            self.assertNotEqual(received_msid_1, received_msid_2)

    def test_add_recording_different_cases(self):
        """ Tests that recordings with only case differences get the same MessyBrainz ID.
        """
        with timescale.engine.begin() as connection:
            title1, artist1, release1, track_number1, duration1 = \
                recording["title"], recording["artist"], recording["release"], \
                recording["additional_info"]["track_number"], recording["additional_info"]["duration"]
            title2, artist2, release2, track_number2, duration2 = \
                recording2["title"], recording2["artist"], recording2["release"], \
                recording2["additional_info"]["track_number"], recording2["additional_info"]["duration"]
            msid1 = messybrainz.submit_recording(connection, title1, artist1, release1, track_number1, duration1)
            msid2 = messybrainz.get_msid(connection, title2, artist2, release2, track_number2, duration2)
            self.assertEqual(msid1, msid2)

    def test_load_recordings_from_msids(self):
        with timescale.engine.begin() as connection:
            title, artist, release = recording["title"], recording["artist"], recording["release"]
            recording_msid = messybrainz.submit_recording(connection, title, artist, release)
            result = messybrainz.load_recordings_from_msids(connection, [recording_msid])[0]
            self.assertDictEqual(result, {
                "msid": recording_msid,
                "title": title,
                "artist": artist,
                "release": release
            })
