from collections import defaultdict

from brainzutils.ratelimit import ratelimit
from flask import Blueprint, request, jsonify, current_app

from listenbrainz.db.metadata import get_metadata_for_recording
from listenbrainz.labs_api.labs.api.artist_credit_recording_lookup import ArtistCreditRecordingLookupQuery
from listenbrainz.labs_api.labs.api.mbid_mapping import MBIDMappingQuery
from listenbrainz.mbid_mapping_writer.matcher import lookup_listens
from listenbrainz.webserver.decorators import crossdomain
from listenbrainz.webserver.errors import APIBadRequest
from listenbrainz.webserver.utils import parse_boolean_arg
from listenbrainz.webserver.views.api_tools import is_valid_uuid

metadata_bp = Blueprint('metadata', __name__)


def parse_incs():
    allowed_incs = ("artist", "tag")
    incs = request.args.get("inc", default="")
    incs = incs.split()
    for inc in incs:
        if inc not in allowed_incs:
            raise APIBadRequest("invalid inc argument '%s'. Must be one of %s." % (inc, ",".join(allowed_incs)))
    return incs


def fetch_metadata(recording_mbids, incs):
    metadata = get_metadata_for_recording(recording_mbids)
    result = {}
    for entry in metadata:
        data = {"recording": entry.recording_data}
        if "artist" in incs:
            data["artist"] = entry.artist_data

        if "tag" in incs:
            data["tag"] = entry.tag_data

        result[str(entry.recording_mbid)] = data
    return result


@metadata_bp.route("/recording/", methods=["GET", "OPTIONS"])
@crossdomain
@ratelimit()
def metadata_recording():
    """
    This endpoint takes in a list of recording_mbids and returns an array of dicts that contain
    recording metadata suitable for showing in a context that requires as much detail about
    a recording and the artist.

    TODO: Add a sample entry

    :param recording_mbids: A comma separated list of recording_mbids
    :type recording_mbids: ``str``
    :param incs: comma separated of additional attribute that should be also returned
    :type incs: ``str``
    :statuscode 200: metadata successfully fetched
    :statuscode 400: invalid recording_mbid or inc arguments
    """
    incs = parse_incs()

    recordings = request.args.get("recording_mbids", default=None)
    if recordings is None:
        raise APIBadRequest("recording_mbids argument must be present and contain a comma separated list of recording_mbids")

    recording_mbids = []
    for mbid in recordings.split(","):
        mbid_clean = mbid.strip()
        if not is_valid_uuid(mbid_clean):
            raise APIBadRequest(f"Recording mbid {mbid} is not valid.")

        recording_mbids.append(mbid_clean)

    result = fetch_metadata(recording_mbids, incs)
    return jsonify(result)


def process_results(match, metadata, incs):
    recording_mbid = match["recording_mbid"]
    result = {
        "recording_mbid": recording_mbid,
        "release_mbid": match["release_mbid"],
        "artist_mbids": match["artist_mbids"],
        "artist_credit_id": match["artist_credit_id"],
        "recording_name": match["recording_name"],
        "release_name": match["release_name"],
        "artist_credit_name": match["artist_credit_name"]
    }
    if metadata:
        extras = fetch_metadata([recording_mbid], incs)
        result["metadata"] = extras.get(recording_mbid, {})
    return result


@metadata_bp.route("/lookup", methods=["GET", "OPTIONS"])
@crossdomain
@ratelimit()
def get_mbid_mapping():
    """
    This endpoint looks up mbid metadata for the given artist and recording name.

    :param artist_name: artist name of the listen
    :type artist_name: ``str``
    :param recording_name: track name of the listen
    :type artist_name: ``str``
    :param metadata: should extra metadata be also returned if a match is found,
                     see /metadata/recording for details.
    :type metadata: ``bool``
    :param incs: same as /metadata/recording endpoint
    :type incs: ``str``
    :statuscode 200: lookup succeeded, does not indicate whether a match was found or not
    :statuscode 400: invalid arguments
    """
    artist_name = request.args.get("artist_name")
    recording_name = request.args.get("recording_name")
    if not artist_name:
        raise APIBadRequest("artist_name is invalid or not present in arguments")
    if not recording_name:
        raise APIBadRequest("recording_name is invalid or not present in arguments")

    metadata = parse_boolean_arg("metadata")
    incs = parse_incs() if metadata else []

    params = [
        {
            "[artist_credit_name]": artist_name,
            "[recording_name]": recording_name
        }
    ]

    q = ArtistCreditRecordingLookupQuery(debug=False)
    result = q.fetch(params)
    if result:
        return process_results(result[0], metadata, incs)

    q = MBIDMappingQuery(timeout=10, remove_stop_words=True, debug=False)
    result = q.fetch(params)
    if result:
        return process_results(result[0], metadata, incs)

    return jsonify({})
