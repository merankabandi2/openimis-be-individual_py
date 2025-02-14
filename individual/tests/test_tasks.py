from core.test_helpers import create_test_interactive_user
from django.test import TestCase
from individual.models import Individual, IndividualDataSource, IndividualDataSourceUpload
from individual.tasks import sync_individuals_to_opensearch
from individual.tests.test_helpers import create_individual
from unittest.mock import patch


class SyncIndividualsToOpenSearchTest(TestCase):

    def setUp(self):
        self.user = create_test_interactive_user(username="admin")
        self.upload = IndividualDataSourceUpload(
            source_name="Test Upload.csv",
            source_type="individual import",
        )
        self.upload.save(user=self.user)

        self.empty_upload = IndividualDataSourceUpload(
            source_name="no-data.csv",
            source_type="individual import",
        )
        self.empty_upload.save(user=self.user)

        self.individual1 = create_individual(self.user.username)
        self.individual2 = create_individual(self.user.username)

        source1 = IndividualDataSource(individual=self.individual1, upload=self.upload)
        source1.save(user=self.user)
        source2 = IndividualDataSource(individual=self.individual2, upload=self.upload)
        source2.save(user=self.user)

    @patch("individual.tasks.registry.update")
    def test_sync_individuals_to_opensearch(self, mock_update):
        sync_individuals_to_opensearch(self.upload.id)

        mock_update.assert_called_once()
        model_arg, kwargs = mock_update.call_args
        self.assertEqual(model_arg[0], Individual)
        self.assertCountEqual(kwargs["instances"], [self.individual1, self.individual2])

    @patch("individual.tasks.registry.update")
    def test_sync_no_individuals(self, mock_update):
        sync_individuals_to_opensearch(self.empty_upload.id)
        mock_update.assert_not_called()

