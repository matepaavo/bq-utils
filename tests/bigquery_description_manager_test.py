import unittest
from unittest import mock

from google.cloud import bigquery

from bq_utils.bigquery_description_manager import BigQueryDescriptionManager


class DescriptionManagerTestCase(unittest.TestCase):
    source_table_id = 'project_name.dataset_name.source_table'
    target_table_id = 'other_project_name.other_dataset_name.target_table'

    def patched_get_table(self, table_id):
        if table_id == self.source_table_id:
            return bigquery.Table(
                bigquery.TableReference(bigquery.DatasetReference('project_name', 'dataset_name'), 'source_table'),
                schema=[bigquery.SchemaField('clientId', 'STRING', 'NULLABLE',
                                             ('Unhashed version of the Client ID for a'
                                              ' given user associated with any given visit/session.')),
                        bigquery.SchemaField('fullVisitorId', 'STRING', 'NULLABLE',
                                             'The unique visitor ID (also known as client ID).'),
                        bigquery.SchemaField('visitNumber', 'STRING', 'NULLABLE',
                                             ('The session number for this user. '
                                              'If this is the first session, then this is set to 1.')),
                        bigquery.SchemaField('totals', 'RECORD', 'NULLABLE',
                                             'This section contains aggregate values across the session.',
                                             [bigquery.SchemaField('hits', 'INTEGER', 'NULLABLE',
                                                                   'Total number of hits within the session.'),
                                              bigquery.SchemaField('pageviews', 'INTEGER', 'NULLABLE',
                                                                   'Total number of pageviews within the session.'),
                                              bigquery.SchemaField('screenviews', 'INTEGER', 'NULLABLE',
                                                                   'Total number of screenviews within the session.')])
                        ])
        elif table_id == self.target_table_id:
            return bigquery.Table(
                bigquery.TableReference(bigquery.DatasetReference('other_project_name', 'other_dataset_name'),
                                        'target_table'),
                schema=[bigquery.SchemaField('clientId', 'STRING', 'NULLABLE'),
                        bigquery.SchemaField('fullVisitorId', 'STRING', 'NULLABLE'),
                        bigquery.SchemaField('totals', 'RECORD', 'NULLABLE',
                                             fields=[bigquery.SchemaField('hits', 'INTEGER', 'NULLABLE'),
                                                     bigquery.SchemaField('pageviews', 'INTEGER', 'NULLABLE')])
                        ])

    def test_get_descriptions(self):
        mock_bq_client = mock.create_autospec(bigquery.Client, instance=True)
        reference = BigQueryDescriptionManager(mock_bq_client)
        mock_bq_client.get_table = self.patched_get_table
        reference.copy_field_descriptions(self.source_table_id, self.target_table_id)
        self.maxDiff = None
        self.assertEqual(mock_bq_client.update_table.call_count, 1)
        self.assertEqual(mock_bq_client.update_table.call_args[1], {})
        self.assertEqual(len(mock_bq_client.update_table.call_args[0]), 2)
        table_arg, params_arg = mock_bq_client.update_table.call_args[0]
        self.assertEqual(params_arg, ['schema'])
        self.assertEqual(table_arg.project, 'other_project_name')
        self.assertEqual(table_arg.dataset_id, 'other_dataset_name')
        self.assertEqual(table_arg.table_id, 'target_table')
        expected_schema = [bigquery.SchemaField('clientId', 'STRING', 'NULLABLE',
                                                ('Unhashed version of the Client ID for a'
                                                 ' given user associated with any given visit/session.')),
                           bigquery.SchemaField('fullVisitorId', 'STRING', 'NULLABLE',
                                                'The unique visitor ID (also known as client ID).'),
                           bigquery.SchemaField('totals', 'RECORD', 'NULLABLE',
                                                'This section contains aggregate values across the session.',
                                                [bigquery.SchemaField('hits', 'INTEGER',
                                                                      'NULLABLE',
                                                                      'Total number of hits within the session.'),
                                                 bigquery.SchemaField('pageviews', 'INTEGER',
                                                                      'NULLABLE',
                                                                      'Total number of pageviews within the session.')])
                           ]
        self.assertListEqual(table_arg.schema, expected_schema)
