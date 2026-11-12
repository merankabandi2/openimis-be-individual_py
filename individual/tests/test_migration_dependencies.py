from django.db.migrations.loader import MigrationLoader
from django.test import SimpleTestCase


class MigrationDependenciesTest(SimpleTestCase):
    def test_admin_rights_migration_runs_after_role_right_validity_from_default(self):
        # 0002 inserts RoleRight rows without ValidityFrom; core 0020 gives the column its default.
        graph = MigrationLoader(None, ignore_no_migrations=True).graph

        plan = graph.forwards_plan(('individual', '0002_add_individual_rigts_for_admin'))

        self.assertIn(('core', '0020_add_missing_fields_to_django_scheme'), plan)
