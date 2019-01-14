"""
Utility to manage BigQuery fields descriptions.
"""
import csv
import logging
import pprint

from google.cloud import bigquery


class BigQueryDescriptionManager:
    """
        Manages table/view descriptions.
    """

    def __init__(self, bq_client):
        """
        Class init.
        :param `google.cloud.bigquery.Client` bq_client: BigQuery client
        """
        self.bq_client = bq_client

    def copy_field_descriptions(self, source_full_table_id, target_full_table_id):
        """
        Copy field descriptions from one table/view to another.
        :param str source_full_table_id: fully-qualified source table ID
        :param str target_full_table_id: fully-qualified target table ID
        """
        source_table = self.bq_client.get_table(source_full_table_id)
        descriptions = self._get_descriptions_from_schema(source_table.schema)
        logging.debug('Source descriptions processed.')
        logging.debug('Source descriptions: %s', pprint.pformat(descriptions))
        self._update_table(target_full_table_id, descriptions)

    def _update_table(self, target_full_table_id, descriptions):
        """
        Updates table with field descriptions.
        :param str target_full_table_id: fully-qualified target table ID
        :param dict descriptions: dictionary of fully-qualified  field name and description pairs
        """
        target_table = self.bq_client.get_table(target_full_table_id)
        target_table.schema = self._get_new_schema(target_table.schema, descriptions)
        logging.debug('Updating target table...')
        self.bq_client.update_table(target_table, ['schema'])
        logging.debug('Successful update')

    def _get_descriptions_from_schema(self, schema):
        """
        Returns a dictionary of fully-qualified field names and descriptions.
        :param list of `google.cloud.bigquery.schema.SchemaField` schema: table schema
        :return dict: dictionary of fully-qualified  field name and description pairs
        """
        fields_to_process = [(column.name, column) for column in schema]
        descriptions = {}
        while fields_to_process:
            field = fields_to_process.pop()
            descriptions[field[0]] = field[1].description
            fields_to_process += [('{}.{}'.format(field[0], nested_field.name), nested_field)
                                  for nested_field in field[1].fields]
        return descriptions

    def _update_field(self, field, name, descriptions):
        """
        Updates the current field and all of its nested fields with the appropriate descriptions.
        :param dict field: dictionary representation of a SchemaField
        :param str name: fully-qualified field name
        :param dict descriptions: dictionary of fully-qualified field names and descriptions
        :return:
        """
        if name in descriptions and descriptions[name]:
            field['description'] = descriptions[name]
        if 'fields' in field:
            for nested_field in field['fields']:
                self._update_field(nested_field,
                                   '{}.{}'.format(name, nested_field['name']), descriptions)

    def _get_new_schema(self, schema, descriptions):
        """
        Creates new schema based on the original and the available descriptions.
        :param list of `google.cloud.bigquery.schema.SchemaField` schema: original table schema
        :param dict descriptions: dictionary of fully-qualified  field names and descriptions
        :return list of `google.cloud.bigquery.schema.SchemaField`: updated table schema
        """
        temp_schema = [field.to_api_repr() for field in schema]
        for field in temp_schema:
            self._update_field(field, field['name'], descriptions)
        updated_schema = [bigquery.schema.SchemaField.from_api_repr(field) for field in temp_schema]
        return updated_schema

    def upload_field_descriptions(self, csv_path, target_full_table_id):
        """
        Uploads field descriptions from a csv file to a table/view.
        :param str csv_path: path to the csv file (header row should be emitted)
        :param str target_full_table_id: fully-qualified target table ID
        """
        with open(csv_path) as input_file:
            csv_reader = csv.reader(input_file)
            descriptions = {row[0]: row[1] for row in csv_reader}
            self._update_table(target_full_table_id, descriptions)
