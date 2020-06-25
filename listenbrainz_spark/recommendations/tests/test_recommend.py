import os
from datetime import datetime
from unittest.mock import patch, call, MagicMock

from listenbrainz_spark.tests import SparkTestCase
from listenbrainz_spark.recommendations import recommend
from listenbrainz_spark.recommendations import train_models
from listenbrainz_spark import schema, utils, config, path, stats
from listenbrainz_spark.exceptions import RecommendationsNotGeneratedException

from pyspark.sql import Row
import pyspark.sql.functions as f
from pyspark.rdd import RDD
from pyspark.mllib.recommendation import Rating

# for test data/dataframes refer to listenbrainzspark/tests/__init__.py
MODEL_PATH = '/test/model'

class RecommendTestClass(SparkTestCase):

    model_save_path = None
    recommendation_top_artist_limit = 10
    recommendation_similar_artist_limit = 10

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        super().upload_test_playcounts()
        cls.upload_test_model()

    @classmethod
    def tearDownClass(cls):
        super().delete_dir()
        super().tearDownClass()

    @classmethod
    def upload_test_model(cls):
        training_data, validation_data, test_data = super().split_playcounts()

        best_model, _, best_model_metadata = train_models.train(
            training_data, validation_data, validation_data.count(), cls.ranks,
            cls.lambdas, cls.iterations, cls.alpha
        )
        cls.model_save_path = os.path.join(MODEL_PATH, best_model_metadata['model_id'])
        train_models.save_model(cls.model_save_path, best_model_metadata['model_id'], best_model)
    '''
    def test_recommendation_params_init(self):
        recordings = utils.create_dataframe(Row(col1=3, col2=9), schema=None)
        model = recommend.load_model(config.HDFS_CLUSTER_URI + self.model_save_path)
        top_artist_candidate_set = utils.create_dataframe(Row(col1=4, col2=5, col3=5), schema=None)
        similar_artist_candidate_set = utils.create_dataframe(Row(col1=1), schema=None)
        recommendation_top_artist_limit = 20
        recommendation_similar_artist_limit = 40

        params = recommend.RecommendationParams(recordings, model, top_artist_candidate_set,
                                                similar_artist_candidate_set,
                                                recommendation_top_artist_limit,
                                                recommendation_similar_artist_limit)

        self.assertEqual(sorted(params.recordings.columns), sorted(recordings.columns))
        self.assertEqual(params.model, model)
        self.assertEqual(sorted(params.top_artist_candidate_set.columns), sorted(top_artist_candidate_set.columns))
        self.assertEqual(sorted(params.similar_artist_candidate_set.columns), sorted(similar_artist_candidate_set.columns))
        self.assertEqual(params.recommendation_top_artist_limit, recommendation_top_artist_limit)
        self.assertEqual(params.recommendation_similar_artist_limit, recommendation_similar_artist_limit)

    def test_load_model(self):
        model = recommend.load_model(config.HDFS_CLUSTER_URI + self.model_save_path)
        self.assertTrue(model)

    def test_get_recording_mbids(self):
        params = self.get_recommendation_params()
        recommended_recording_ids = [1]
        recording_mbids = recommend.get_recording_mbids(params, recommended_recording_ids)

        self.assertEqual(recording_mbids.count(), 1)
        self.assertEqual(recording_mbids.collect()[0].mb_recording_mbid, "3acb406f-c716-45f8-a8bd-96ca3939c2e5")

    @patch('listenbrainz_spark.recommendations.recommend.get_recording_mbids')
    @patch('listenbrainz_spark.recommendations.recommend.generate_recommendations')
    def test_get_recommended_mbids(self, mock_gen_rec, mock_get_mbids):
        candidate_set = self.get_candidate_set()
        params = self.get_recommendation_params()
        limit = 2

        rdd = candidate_set.rdd.map(lambda r: (r['user_id'], r['recording_id']))

        mock_gen_rec.return_value = [Rating(user=2, product=2, rating=1.2)]
        mock_get_mbids.return_value = utils.create_dataframe(Row(mb_recording_mbid='xxxx'), schema=None)
        recommended_mbids = recommend.get_recommended_mbids(rdd, params, limit)

        mock_gen_rec.assert_called_once_with(rdd, params, limit)
        mock_get_mbids.assert_called_once_with(params, [2])

        self.assertEqual(recommended_mbids, ['xxxx'])

        mock_gen_rec.return_value = []

        with self.assertRaises(RecommendationsNotGeneratedException):
            _ = recommend.get_recommended_mbids(rdd, params, limit)

    def test_generate_recommendations(self):
        params = self.get_recommendation_params()
        limit = 1
        mock_candidate_set = MagicMock()

        mock_model = MagicMock()
        params.model = mock_model

        _ = recommend.generate_recommendations(mock_candidate_set, params, limit)

        mock_predict = mock_model.predictAll
        mock_predict.assert_called_once_with(mock_candidate_set)

        mock_take_ordered = mock_predict.return_value.takeOrdered
        self.assertEqual(mock_take_ordered.call_args[0][0], limit)

    def test_get_candidate_set_rdd_for_user(self):
        candidate_set = self.get_candidate_set()
        user_id = 1

        candidate_set_rdd = recommend.get_candidate_set_rdd_for_user(candidate_set, user_id)

        self.assertTrue(isinstance(candidate_set_rdd, RDD))
        self.assertEqual(candidate_set_rdd.collect()[0][0], user_id)
        self.assertEqual(candidate_set_rdd.collect()[0][1], 1)

        user_id = 10
        with self.assertRaises(IndexError):
            candidate_set_rdd = recommend.get_candidate_set_rdd_for_user(candidate_set, user_id)

    @patch('listenbrainz_spark.recommendations.recommend.get_recommended_mbids')
    @patch('listenbrainz_spark.recommendations.recommend.get_candidate_set_rdd_for_user')
    def test_get_recommendations_for_user(self, mock_candidate_set, mock_mbids):
        params = self.get_recommendation_params()
        user_id = 1
        user_name = 'vansika'

        _, _ = recommend.get_recommendations_for_user(user_id, user_name, params)

        mock_candidate_set.assert_has_calls([
            call(params.top_artist_candidate_set, user_id),
            call(params.similar_artist_candidate_set, user_id)
        ])

        mock_mbids.assert_has_calls([
            call(mock_candidate_set.return_value, params, params.recommendation_top_artist_limit),
            call(mock_candidate_set.return_value, params, params.recommendation_similar_artist_limit)
        ])
    '''
    def test_get_users(self):
        params = self.get_recommendation_params()
        df = utils.create_dataframe(
            Row(
                user_id=1,
                user_name='vansika',
                recording_id=1
            ),
            schema=None
        )

        df = df.union(utils.create_dataframe(
            Row(
                user_id=1,
                user_name='vansika',
                recording_id=2
            ),
            schema=None
        ))

        df = df.union(utils.create_dataframe(
            Row(
                user_id=2,
                user_name='rob',
                recording_id=1
            ),
            schema=None
        ))

        params.top_artist_candidate_set = df

        users = recommend.get_users(params)
        users.show()
        self.assertEqual(users.count(), 2)
        self.assertEqual(sorted(users.columns), sorted(['user_id', 'user_name']))

    @patch('listenbrainz_spark.recommendations.recommend.get_recommendations_for_user')
    @patch('listenbrainz_spark.recommendations.recommend.get_users')
    def test_get_recommendations_for_all(self, mock_users, mock_rec_user):
        mock_users.return_value = utils.create_dataframe(
            Row(
                user_id=1,
                user_name='vansika',
                recording_id=1
            ),
            schema=None
        )
        params = self.get_recommendation_params()

        mock_rec_user.return_value = 'recording_mbid_1', 'recording_mbid_2'
        messages = recommend.get_recommendations_for_all(params)

        mock_rec_user.assert_called_once_with(1, 'vansika', params)

        message = messages[0]
        self.assertEqual(message['musicbrainz_id'], 'vansika')
        self.assertTrue(message['type'], 'cf_recording_recommendations')
        self.assertTrue(message['top_artist'], 'recording_mbid_1')
        self.assertTrue(message['similar_artist'], 'recording_mbid_2')

    def get_recommendation_params(self):
        recordings = self.get_recordings_df()
        model = recommend.load_model(config.HDFS_CLUSTER_URI + self.model_save_path)
        top_artist_candidate_set = self.get_candidate_set()
        similar_artist_candidate_set = self.get_candidate_set()
        recommendation_top_artist_limit = 2
        recommendation_similar_artist_limit = 1

        params = recommend.RecommendationParams(recordings, model, top_artist_candidate_set,
                                                similar_artist_candidate_set,
                                                recommendation_top_artist_limit,
                                                recommendation_similar_artist_limit)
        return params
