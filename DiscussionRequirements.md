** Issue A **

Input CSV columns and input schema table requirements do not match.
To overcome the issue I have taken these steps:

- Initallize empty Contract and Claim dataframes with columns names as defined in the requirements
- Loaded CSV file content within temporary dataframes
- Forced temporary dataframe column names to match main dataframe with PySpark union command

Otherwise, I could renamed input CSV columns with mapping dictionary defined in the file config.yml

** Issue B **

The field CLAIM_ID is defined as INTEGER in the requirements chema but it is a STRING
As a result, the CLAIM_ID value converted as an INTEGER was NULL in the CLAIM table
This is a constraining issue because in the transaction table many fields are derived from CLAIM_ID field
I changed schema requirements for CLAIM_ID field from NUMBER to STRING to test the join