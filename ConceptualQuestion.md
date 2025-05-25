**Create a proposal on how to handle new batches of input data, so that existing transactions
created in previous batches are not overridden. You don’t need to provide an implementation,
just write down a short proposal which can be discussed at the interview.**

- Save each transaction output file with date for logging and backup purposes (es. transactions_20250526.csv)
Following steps change according to requirements.

If all the transactions are required to be stored and unique transactions are extracted with post processing logics:

- Write CSV file in append mode with most recent file

If we do not need any duplicate transaction: 

- Load the last written transaction file in a "target" DataFrame - write a code snippet which loads the file with the most recent timestamp in filename
- Write a Merge condition in PySpark between targetDataFrame and updates DataFrame (transactions created in current processing batch)
- Merge condition must be based on the match between primary composite key fields to keep records uniqueness. We could dynamycally extract merge key fields reading fields metadata
- WhenNotMatched: InsertAll
- It could be thought about a SCD2 logic to keep history of updated transaction but schema requirements does not have an active flag so I suppose it's not really important
