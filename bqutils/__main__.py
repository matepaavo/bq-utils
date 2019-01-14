import argparse
import logging
import sys

from google.cloud import bigquery

from bqutils.bigquery_description_manager import BigQueryDescriptionManager


def main():
    """If used as the main module, this method parses the arguments and calls copy or upload"""
    parser = argparse.ArgumentParser(
        description='Copy or upload field descriptions for BigQuery tables/views')
    parser.add_argument('mode', type=str, choices=['desccopy', 'descupload'])
    parser.add_argument('--source',
                        action='store',
                        help='fully-qualified source table ID')
    parser.add_argument('--target',
                        action='store',
                        help='fully-qualified target table ID',
                        required=True)
    parser.add_argument('--csv_path',
                        action='store',
                        help='path for the csv file')
    parser.add_argument('--debug',
                        action='store_true',
                        help='set debug mode on, default is false')

    args = parser.parse_args()
    if args.mode == 'copy' and not args.source:
        parser.error('source table id is missing for copy')
    elif args.mode == 'upload' and not args.csv_path:
        parser.error('csv path is missing for upload')

    log_level = logging.DEBUG if args.debug else logging.INFO

    logging.basicConfig(stream=sys.stdout, level=log_level,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    client = bigquery.Client()
    description_manager = BigQueryDescriptionManager(client)
    if args.mode == 'desccopy':
        description_manager.copy_field_descriptions(args.source, args.target)
    elif args.mode == 'descupload':
        description_manager.upload_field_descriptions(args.csv_path, args.target)


if __name__ == '__main__':
    main()
