# cox_automotive

Welcome to the report engine

This engine is implemented a framework you to generate a file and send to FTP/sFTP by configuration.

# ReportConfiguration

you can perdine the report ID ,output format and the API endpoint

# Report

It provided few functions for your work to generate a report

Upload_report: upload the report to FTP or sFTP
column_to_cte: generate a CTE(common table expression) by pandas dataframe
pysqldf: globalize the pandasql.sqldf within the engine
convert_column: globally replace a string to the pandas data frame
fix_column_length: to fix the length of a column in the pandas data frame
