import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import datetime
import csv
import json

class FilterTransactions(beam.DoFn):
    """Filter transactions based on amount and date."""

    def process(self, element):
        """
        Process each element and filter transactions.

        Args:
            element: A dictionary representing a transaction.

        Yields:
            The filtered transactions.
        """
        try:
            transaction_amount = float(element['transaction_amount'])
            transaction_date = datetime.datetime.strptime(element['timestamp'], '%Y-%m-%d %H:%M:%S %Z')
            if transaction_amount > 20 and transaction_date.year >= 2010:
                yield element
        except ValueError:
            # Skip rows with invalid data
            pass


class SumTotalAmount(beam.DoFn):
    """Calculate the sum of total amounts by date."""

    def process(self, element):
        """
        Process each element and calculate the sum of total amounts by date.

        Args:
            element: A dictionary representing a transaction.

        Yields:
            A tuple of date and total amount.
        """
        yield (element['timestamp'].split()[0], float(element['transaction_amount']))


class ProcessTransactions(beam.PTransform):
    """Process the transactions and calculate the total amount by date."""

    def expand(self, pcoll):
        """
        Expand the PTransform.

        Args:
            pcoll: A PCollection representing the input data.

        Returns:
            A PCollection representing the output data.
        """
        return (
            pcoll
            | 'ParseCSV' >> beam.Map(lambda line: next(csv.reader([line])))
            | 'FormatAsDict' >> beam.Map(lambda fields: {
                'timestamp': fields[0],
                'origin': fields[1],
                'destination': fields[2],
                'transaction_amount': fields[3]
            })
            | 'FilterTransactions' >> beam.ParDo(FilterTransactions())
            | 'SumTotalAmount' >> beam.ParDo(SumTotalAmount())
            | 'GroupByDate' >> beam.GroupByKey()
            | 'SumAmount' >> beam.Map(lambda kv: {
                'date': kv[0],
                'total_amount': sum(kv[1])
            })
        )


class BeamUnitTest(unittest.TestCase):

    def test_pipeline(self):
        """Unit test for the Beam pipeline."""
        input_data = [
            'timestamp,origin,destination,transaction_amount',
            '2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99',
            '2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95',
            '2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22',
            '2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,1.00030',
            '2017-08-31 17:00:09 UTC,wallet00001e494c12b3083634,wallet00005f83196ec58e4ffe,13700000023.08',
            '2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,129.12',
        ]

        expected_output = [
            '{"date": "2017-03-18", "total_amount": 2102.22}',
            '{"date": "2017-08-31", "total_amount": 13700000023.08}',
            '{"date": "2018-02-27", "total_amount": 129.12}',
        ]

        with TestPipeline() as p:
            input_lines = p | beam.Create(input_data)
            output = (
                input_lines
                | 'ProcessTransactions' >> ProcessTransactions()
                | 'FormatOutput' >> beam.Map(json.dumps)
            )

            assert_that(output, equal_to(expected_output))


if __name__ == '__main__':
    unittest.main()
