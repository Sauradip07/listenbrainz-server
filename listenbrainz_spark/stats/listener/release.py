from typing import Iterator

from data.model.entity_listener_stat import ArtistListenerRecord
from listenbrainz_spark.stats import run_query


def get_listeners(table: str, cache_table: str, number_of_results: int) -> Iterator[ArtistListenerRecord]:
    """ Get information about top listeners of a release.

        Args:
            table: name of the temporary table having listens.
            cache_table: release data table
            number_of_results: number of top results to keep per user.

        Returns:
            iterator (iter): an iterator over result

            {
                "release-mbid-1": {
                    "count": total listen count,
                    "top_listeners": [list of user ids of top listeners]
                },
                // ...
            }

    """
    result = run_query(f"""
        WITH listens_with_mb_data AS (
            SELECT user_id
                 , release_mbid
                 , ct.release_name
              FROM {table}
              JOIN {cache_table} ct
             USING (release_mbid)
        ), intermediate_table AS (
            SELECT user_id
                 , release_mbid
                 , release_name
                 , count(*) AS listen_count
              FROM listens_with_mb_data
          GROUP BY release_mbid
                 , release_name
                 , user_id
        ), entity_count as (
            SELECT release_mbid
                 , SUM(listen_count) as total_listen_count
              FROM intermediate_table
          GROUP BY release_mbid      
        ), ranked_stats as (
            SELECT release_mbid
                 , release_name
                 , user_id 
                 , listen_count
                 , row_number() OVER (PARTITION BY user_id ORDER BY listen_count DESC) AS rank
              FROM intermediate_table
        ), grouped_stats AS (
            SELECT release_mbid
                 , release_name
                 , sort_array(
                        collect_list(
                            struct(
                                listen_count
                              , user_id 
                            )
                        )
                        , false
                   ) as listeners
              FROM ranked_stats
             WHERE rank <= {number_of_results}
          GROUP BY release_mbid
                 , release_name
        )
            SELECT release_mbid
                 , release_name
                 , listeners
                 , total_listen_count
              FROM grouped_stats
              JOIN entity_count
             USING (release_mbid)
    """)

    return result.toLocalIterator()
