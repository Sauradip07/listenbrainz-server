from flask import Blueprint, current_app, jsonify

import listenbrainz.db.user as db_user
import listenbrainz.db.user_relationship as db_user_relationship
from listenbrainz import db

from listenbrainz.webserver.decorators import crossdomain
from listenbrainz.webserver.errors import APINotFound, APIInternalServerError, APIBadRequest
from brainzutils.ratelimit import ratelimit
from listenbrainz.webserver.views.api_tools import validate_auth_header

social_api_bp = Blueprint('social_api_v1', __name__)


@social_api_bp.route("/user/<user_name>/followers", methods=["GET", "OPTIONS"])
@crossdomain
@ratelimit()
def get_followers(user_name: str):
    """
    Fetch the list of followers of the user ``user_name``. Returns a JSON with an array of user names like these:

    .. code-block:: json

        {
            "followers": ["rob", "mr_monkey", "..."],
            "user": "shivam-kapila"
        }

    :statuscode 200: Yay, you have data!
    :statuscode 404: User not found
    """
    user = db_user.get_by_mb_id(user_name)

    if not user:
        raise APINotFound("User %s not found" % user_name)

    with db.engine.connect() as connection:
        followers = db_user_relationship.get_followers_of_user(connection, user["id"])

    followers = [user["musicbrainz_id"] for user in followers]
    return jsonify({"followers": followers, "user": user["musicbrainz_id"]})


@social_api_bp.route("/user/<user_name>/following", methods=["GET", "OPTIONS"])
@crossdomain
@ratelimit()
def get_following(user_name: str):
    """
    Fetch the list of users followed by the user ``user_name``. Returns a JSON with an array of user names like these:

    .. code-block:: json

        {
            "followers": ["rob", "mr_monkey", "..."],
            "user": "shivam-kapila"
        }

    :statuscode 200: Yay, you have data!
    :statuscode 404: User not found
    """
    user = db_user.get_by_mb_id(user_name)

    if not user:
        raise APINotFound("User %s not found" % user_name)

    with db.engine.connect() as connection:
        following = db_user_relationship.get_following_for_user(connection, user["id"])

    following = [user["musicbrainz_id"] for user in following]
    return jsonify({"following": following, "user": user["musicbrainz_id"]})


@social_api_bp.route("/user/<user_name>/follow", methods=["POST", "OPTIONS"])
@crossdomain
@ratelimit()
def follow_user(user_name: str):
    """
    Follow the user ``user_name``. A user token (found on  https://listenbrainz.org/profile/ ) must
    be provided in the Authorization header!

    :reqheader Authorization: Token <user token>
    :reqheader Content-Type: *application/json*
    :statuscode 200: Successfully followed the user ``user_name``.
    :statuscode 400:
                    - Already following the user ``user_name``.
                    - Trying to follow yourself.
    :statuscode 401: invalid authorization. See error message for details.
    :resheader Content-Type: *application/json*
    """
    current_user = validate_auth_header()
    user = db_user.get_by_mb_id(user_name)

    if not user:
        raise APINotFound("User %s not found" % user_name)

    if user["musicbrainz_id"] == current_user["musicbrainz_id"]:
        raise APIBadRequest("Whoops, cannot follow yourself.")

    with db.engine.connect() as connection:
        if db_user_relationship.is_following_user(connection, current_user["id"], user["id"]):
            raise APIBadRequest(f'{current_user["musicbrainz_id"]} is already following user {user["musicbrainz_id"]}')

        db_user_relationship.insert(connection, current_user["id"], user["id"], "follow")

    return jsonify({"status": "ok"})


@social_api_bp.route("/user/<user_name>/unfollow", methods=["POST", "OPTIONS"])
@crossdomain
@ratelimit()
def unfollow_user(user_name: str):
    """
    Unfollow the user ``user_name``. A user token (found on  https://listenbrainz.org/profile/ ) must
    be provided in the Authorization header!

    :reqheader Authorization: Token <user token>
    :reqheader Content-Type: *application/json*
    :statuscode 200: Successfully unfollowed the user ``user_name``.
    :statuscode 401: invalid authorization. See error message for details.
    :resheader Content-Type: *application/json*
    """
    current_user = validate_auth_header()
    user = db_user.get_by_mb_id(user_name)

    if not user:
        raise APINotFound("User %s not found" % user_name)

    with db.engine.connect() as connection:
        db_user_relationship.delete(connection, current_user["id"], user["id"], "follow")

    return jsonify({"status": "ok"})
