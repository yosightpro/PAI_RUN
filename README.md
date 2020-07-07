# PAI_RUN
 PAI

NOTE: To start service run the Manage.py script file. Manage.py is the main file that starts and stops the service. 

The system is designed to read a CSV file sent in on a monthly basis. The file is intended to be loaded in a pre-existing database with multiple tables. Python being the key programming language for this solution, the file is expected to be broken down and loaded in multiple tables under one schema IBRD_UG. 

After the file loading process is done and the file is complete, a number of results set in Excel workbooks are created in the directory “OUTPUT” in the home path of the source code.


TOOLS AND RESOURCES USED:
•	Python 3.8
•	PG Admin 4
•	PostgresSQL
•	SQL WorkBench

Additional notes:

Raw file is maintained in the home directory
Use email Logins of SMTP to access sample emails sent out after the file is loaded into the database
Notice the powershell results to monitor service progress