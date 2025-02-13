import logging

from celery import shared_task
from django_opensearch_dsl.registries import registry
from individual.models import Individual

logger = logging.getLogger(__name__)


@shared_task
def sync_individuals_to_opensearch(upload_id):
    individuals = list(Individual.objects.filter(
        individualdatasource__upload=upload_id
    ))
    if individuals:
        registry.update(Individual, instances=individuals)
