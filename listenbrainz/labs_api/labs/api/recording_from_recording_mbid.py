import psycopg2.extras
from datasethoster import Query
from flask import current_app

from listenbrainz.labs_api.labs.api.utils import get_recordings_from_mbids

psycopg2.extras.register_uuid()


class RecordingFromRecordingMBIDQuery(Query):
    """ Look up a musicbrainz data for a list of recordings, based on MBID. """

    def names(self):
        return "recording-mbid-lookup", "MusicBrainz Recording by MBID Lookup"

    def inputs(self):
        return ['[recording_mbid]']

    def introduction(self):
        return """Look up recording and artist information given a recording MBID"""

    def outputs(self):
        return ['recording_mbid', 'recording_name', 'length', 'comment', 'artist_credit_id',
                'artist_credit_name', '[artist_credit_mbids]', 'original_recording_mbid']

    def fetch(self, params, offset=-1, count=-1):
        if not current_app.config["MB_DATABASE_URI"]:
            return []

        mbids = [p['[recording_mbid]'] for p in params]
        output = get_recordings_from_mbids(mbids)

        # Ideally offset and count should be handled by the postgres query itself, but the 1:1 relationship
        # of what the user requests and what we need to fetch is no longer true, so we can't easily use LIMIT/OFFSET.
        # We might be able to use a RIGHT JOIN to fix this, but for now I'm happy to leave this as it. We need to
        # revisit this when we get closer to pushing recommendation tools to production.
        if offset > 0 and count > 0:
            return output[offset:offset+count]

        if offset > 0 and count < 0:
            return output[offset:]

        if offset < 0 and count > 0:
            return output[:count]

        return output
