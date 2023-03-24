from pydantic import BaseModel, NonNegativeInt, validator, constr

from data.model.validators import check_valid_uuid


class UserListenRecord(BaseModel):
    """ Each individual record for a top listener of an entity

    Contains the ListenBrainz user ID and listen count.
    """
    user_id: NonNegativeInt
    listen_count: NonNegativeInt


class ArtistListenerRecord(BaseModel):
    """ Each individual record for top listeners of any given artist

    Contains the artist name, ListenBrainz user IDs and listen count.
    """
    artist_mbid: str
    artist_name: constr(min_length=1)
    total_listen_count: NonNegativeInt
    users: list[UserListenRecord]

    _validate_uuids: classmethod = validator("artist_mbid", allow_reuse=True)(check_valid_uuid)


EntityListenerRecord = ArtistListenerRecord
