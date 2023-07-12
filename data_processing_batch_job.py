import os
import apache_beam as beam
import datetime
import csv
import json
from typing import Tuple, List



def process_transactions(row: List[str]) -> List[Tuple[str, float]]:
    """
    Process transactions and filter based on criteria.

    Args:
        row (List[str]): A list representing a row of transaction data.

    Returns:
        List[Tuple[str, float]]: A list of tuples containing the date and transaction amount.

    """
    date = datetime.datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S UTC').strftime('%Y-%m-%d')
    if float(row[3]) > 20 and date >= '2010-01-01':
        return [(date, float(row[3]))]
    else:
        return []


def format_output(element: Tuple[str, float]) -> str:
    """
    Format the output element as JSON string.

    Args:
        element (Tuple[str, float]): A tuple containing the date and total amount.

    Returns:
        str: A JSON string representing the output element.

    """
    date, total = element
    return json.dumps({'date': date, 'total': total})


## Posible alternative solution 

# def write_jsonl(element: str, output_folder: str) -> None:
#     """
#     Write the JSON string element to a JSONL file.

#     Args:
#         element (str): A JSON string.
#         output_folder (str): The output folder path.

#     """
#     with gzip.open(os.path.join(output_folder, 'results.jsonl.gz'), 'at') as f:
#         f.write(element + '\n')



# def write_csv(elements: List[str], output_folder: str) -> None:
#     """
#     Write the elements to a CSV file.

#     Args:
#         elements (List[str]): A list of JSON strings.
#         output_folder (str): The output folder path.

#     """
#     csv_rows = []
#     for element in elements:
#         json_data = json.loads(element)
#         csv_rows.append(f"{json_data['date']},{json_data['total']}")
    
#     with open(os.path.join(output_folder, 'results.csv'), "w", newline="") as f:
#         f.write("date,total_amount\n")
#         f.write("\n".join(csv_rows))


def run_pipeline() -> None:
    """
    Run the Apache Beam pipeline to process transactions.

    """
    output_folder = 'output'
    os.makedirs(output_folder, exist_ok=True)  # Create the output folder if it doesn't exist

    
    with beam.Pipeline() as p:
        transactions = (
            p
            | 'Read CSV' >> beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv', skip_header_lines=1)
            | 'Parse CSV' >> beam.Map(lambda line: next(csv.reader([line])))
            | 'Process Transactions' >> beam.FlatMap(process_transactions)
            | 'Sum by Date' >> beam.CombinePerKey(sum)
            | 'Format Output' >> beam.Map(format_output)
        )
        #transactions | 'Print Output' >> beam.Map(print)  # Add this line to print the output
        
        transactions | 'Write JSONL' >> beam.io.WriteToText(
            os.path.join(output_folder, 'results'),
            file_name_suffix='.jsonl.gz',
            coder=beam.coders.StrUtf8Coder(),
            num_shards=1,
            shard_name_template='')
       # transactions | 'Write CSV' >> beam.combiners.ToList() | 'Write Files' >> beam.ParDo(write_csv, output_folder=output_folder)

if __name__ == '__main__':
    run_pipeline()
