from brainzutils.ratelimit import ratelimit
from flask import Blueprint, request, jsonify, current_app

from listenbrainz.db.metadata import get_metadata_for_recording
from listenbrainz.mbid_mapping_writer.matcher import lookup_listens
from listenbrainz.webserver.decorators import crossdomain
from listenbrainz.webserver.errors import APIBadRequest
from listenbrainz.webserver.views.api_tools import is_valid_uuid

metadata_bp = Blueprint('metadata', __name__)


@metadata_bp.route("/recording/", methods=["GET", "OPTIONS"])
@crossdomain
@ratelimit()
def metadata_recording():
    """
    This endpoint takes in a list of recording_mbids and returns an array of dicts that contain
    recording metadata suitable for showing in a context that requires as much detail about
    a recording and the artist.

    TODO: Add a sample entry and document inc argument

    :param recording_mbids: A comma separated list of recording_mbids
    :type recording_mbids: ``str``
    :statuscode 200: playlist generated
    :statuscode 400: invalid recording_mbid arguments
    """

    allowed_incs = ("artist", "tag")

    recordings = request.args.get("recording_mbids", default=None)
    if recordings is None:
        raise APIBadRequest("recording_mbids argument must be present and contain a comma separated list of recording_mbids")

    incs = request.args.get("inc", default="")
    incs = incs.split()
    for inc in incs:
        if inc not in allowed_incs:
            raise APIBadRequest("invalid inc argument '%s'. Must be one of %s." % (inc, ",".join(allowed_incs)))

    recording_mbids = []
    for mbid in recordings.split(","):
        mbid_clean = mbid.strip()
        if not is_valid_uuid(mbid_clean):
            raise APIBadRequest(f"Recording mbid {mbid} is not valid.")

        recording_mbids.append(mbid_clean)

    metadata = get_metadata_for_recording(recording_mbids)
    result = {}
    for entry in metadata:
        data = { "recording": entry.recording_data }
        if "artist" in incs:
            data["artist"] = entry.artist_data

        if "tag" in incs:
            data["tag"] = entry.tag_data

        result[str(entry.recording_mbid)] = data

    return jsonify(result)


@metadata_bp.route("/lookup", methods=["GET", "OPTIONS"])
def get_mbid_mapping():
    """
    This endpoint looks up mbid metadata for the given artist and recording name.

    :param artist_name: artist name of the listen
    :type artist_name: ``str``
    :param recording_name: track name of the listen
    :type artist_name: ``str``
    :statuscode 200: lookup succeeded, does not indicate match found or not
    :statuscode 400: invalid arguments
    """
    artist_name = request.args.get("artist_name")
    recording_name = request.args.get("recording_name")
    if not artist_name:
        raise APIBadRequest("artist_name is invalid or not present in arguments")
    if not recording_name:
        raise APIBadRequest("recording_name is invalid or not present in arguments")

    listen = {
        "data": {
            "artist_name": artist_name,
            "track_name": recording_name
        }
    }

    exact_lookup, _, _ = lookup_listens(current_app, [listen], {}, True, False)
    if exact_lookup:
        return jsonify(exact_lookup)

    fuzzy_lookup, _, _ = lookup_listens(current_app, [listen], {}, False, False)
    if fuzzy_lookup:
        return jsonify(fuzzy_lookup)

    return jsonify({})
