MBID Mapping
============

For a background on how the mapping works, see :ref:`developers-mapping`

Debugging lookups
^^^^^^^^^^^^^^^^^

If a listen isn't showing up as mapped on ListenBrainz, one of the following might be true:

* The item wasn't in musicbrainz at the time that the lookup was made
* There is a bug in the mapping algorithm

If the recording doesn't exist in MusicBrainz during mapping, a row will be added to the ``mbid_mapping`` table
with the MSID and a ``match_type`` of ``no_match``. Currently no_match values aren't looked up again automatically.

You can test the results of a lookup by using https://labs.api.listenbrainz.org/explain-mbid-mapping
This uses the same lookup process that the mapper uses. If this returns a result, but there is no mapping present
it could be due to data being recently added to MusicBrainz or improvements to the mapping algorithm.

In this case you can retrigger a lookup by seting the ``mbid_mapping.last_updated`` field to '1970-01-01'. 
The mapper will pick up these items and put them on the queue again.

.. code:: sql

    UPDATE mbid_mapping SET last_updated = 'yyyy-mm-dd' WHERE recording_msid = '00000737-3a59-4499-b30a-31fe2464555d';
    UPDATE mbid_mapping SET last_updated = 'yyyy-mm-dd' WHERE match_type = 'no_match' AND last_updated = now() - interval '1 day';
