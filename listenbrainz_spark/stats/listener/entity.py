import json
import logging
from datetime import datetime
from typing import Iterator, Optional, Dict

from more_itertools import chunked
from pydantic import ValidationError

from data.model.entity_listener_stat import EntityListenerRecord, ArtistListenerRecord
from data.model.user_entity import UserEntityStatMessage
from listenbrainz_spark.path import RELEASE_METADATA_CACHE_DATAFRAME, ARTIST_COUNTRY_CODE_DATAFRAME
from listenbrainz_spark.stats import get_dates_for_stats_range
from listenbrainz_spark.stats.user import USERS_PER_MESSAGE
from listenbrainz_spark.stats.user.artist import get_artists
from listenbrainz_spark.utils import get_listens_from_dump, read_files_from_HDFS

logger = logging.getLogger(__name__)

entity_handler_map = {
    "artists": get_artists,
}

entity_model_map = {
    "artists": ArtistListenerRecord,
}

entity_cache_map = {
    "artists": ARTIST_COUNTRY_CODE_DATAFRAME,
}

NUMBER_OF_TOP_LISTENERS = 10  # number of top listeners to retain for user stats


def get_listener_stats(entity: str, stats_range: str, database: str = None) -> Iterator[Optional[Dict]]:
    """ Get the top listeners for all entity for specified stats_range """
    logger.debug(f"Calculating {entity}_listener_{stats_range}...")

    from_date, to_date = get_dates_for_stats_range(stats_range)
    listens_df = get_listens_from_dump(from_date, to_date)
    table = f"{entity}_listener_{stats_range}"
    listens_df.createOrReplaceTempView(table)

    df_name = "entity_data_cache"
    cache_table_path = entity_cache_map.get(entity)
    if cache_table_path:
        read_files_from_HDFS(cache_table_path).createOrReplaceTempView(df_name)

    messages = calculate_entity_stats(
        from_date, to_date, table, df_name, entity, stats_range, database
    )

    logger.debug("Done!")

    return messages


def calculate_entity_stats(from_date: datetime, to_date: datetime, table: str, cache_table: str,
                           entity: str, stats_range: str, database: str = None):
    handler = entity_handler_map[entity]
    data = handler(table, cache_table, NUMBER_OF_TOP_LISTENERS)
    return create_messages(data=data, entity=entity, stats_range=stats_range, from_date=from_date,
                           to_date=to_date, database=database)


def parse_one_entity_stats(entry, entity: str, stats_range: str) \
        -> Optional[EntityListenerRecord]:
    try:
        data = entry.asDict(recursive=True)
        return entity_model_map[stats_range](**data)
    except ValidationError:
        logger.error(f"""ValidationError while calculating {stats_range} top {entity} for user: 
        {data["user_id"]}. Data: {json.dumps(data, indent=2)}""", exc_info=True)
        return None


def create_messages(data, entity: str, stats_range: str, from_date: datetime, to_date: datetime, database: str = None) \
        -> Iterator[Optional[Dict]]:
    """
    Create messages to send the data to the webserver via RabbitMQ

    Args:
        data: Data to sent to the webserver
        entity: The entity for which statistics are calculated, i.e 'artists',
            'releases' or 'recordings'
        stats_range: The range for which the statistics have been calculated
        from_date: The start time of the stats
        to_date: The end time of the stats
        database: the name of the database in which the webserver should store the data

    Returns:
        messages: A list of messages to be sent via RabbitMQ
    """
    if database is None:
        database = f"{entity}_{stats_range}"

    yield {
        "type": "couchdb_data_start",
        "database": database
    }

    from_ts = int(from_date.timestamp())
    to_ts = int(to_date.timestamp())

    for entries in chunked(data, USERS_PER_MESSAGE):
        multiple_entity_stats = []
        for entry in entries:
            processed_stat = parse_one_entity_stats(entry, entity, stats_range)
            multiple_entity_stats.append(processed_stat)

        try:
            model = UserEntityStatMessage(**{
                "type": "entity_listener",
                "stats_range": stats_range,
                "from_ts": from_ts,
                "to_ts": to_ts,
                "entity": entity,
                "data": multiple_entity_stats,
                "database": database
            })
            result = model.dict(exclude_none=True)
            yield result
        except ValidationError:
            logger.error(f"ValidationError while calculating {stats_range} top {entity}:", exc_info=True)
            yield None

    yield {
        "type": "couchdb_data_end",
        "database": database
    }
