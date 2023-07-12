
# Data Processing with Apache Beam

This project, developed by Mihir Shah, demonstrates data processing using Apache Beam in Python. The project reads transaction data from a CSV file, processes it to calculate the daily sum of transactions over a certain amount, and writes the results into a JSON Lines (JSONL) file.

## Project Structure

The project comprises two main Python files:

1.  `data_processing_batch_job.py`: This script contains the main Apache Beam pipeline that performs the data processing tasks.
2.  `data_processing_unittest.py`: This script contains unit tests for the transformations used in the Apache Beam pipeline.

## How It Works

1.  The Apache Beam pipeline reads the input data from a CSV file located at `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`.
2.  It filters out transactions that have a `transaction_amount` of less than or equal to 20.
3.  It also filters out transactions that were made before the year 2010.
4.  The pipeline then sums the total transaction amount by date.
5.  The results are saved into a JSON Lines file named `results.jsonl.gz` in the `output` directory.

## Installation

To set up and run the data processing pipeline, follow these steps:

1.  Clone the GitHub repository:
    
    `git clone https://github.com/Mir955/Data-processing-with-Apache-Beam.git` 
    
2.  Navigate to the cloned project directory:
        
    `cd Data-processing-with-Apache-Beam` 
    
3. Create and activate virtual environment
	
    `python3 -m venv venv`

	`venv\Scripts\activate` 
 
4. Install the required dependencies:
    
    `pip install -r requirements.txt` 
    

## Running the Project


You can run the Apache Beam pipeline with the following command:

`python data_processing_batch_job.py` 

## Unit Tests

To run the unit tests, execute the following command:

`python -m unittest data_processing_unittest.py` 

## Notes

 - [**Note 1:** ] One possible solution to improve the `write_jsonl` function is to avoid opening the output file for each element that is written. This approach could impact the pipeline's performance if there are a large number of elements. Instead, consider accumulating the results into a PCollection and utilizing a built-in Apache Beam transform like `beam.io.WriteToText` to write them in a single operation. This can help optimize the pipeline's efficiency.
 
 - [ **Note 2:** ] The write_csv function converts the JSON strings back
 into CSV format. This step might be unnecessary since you are already processing the data into a desirable format before writing it as a JSON Lines file.

## Contact

For any questions or feedback, please reach out to Mihir Shah at [mihirshah1328@gmail.com](mailto:mihirshah1328@gmail.com).

This README provides a concise explanation of your project, its structure, and how to run the pipeline and the unit tests. It also includes license and contact sections to provide relevant information to users and collaborators.