# ImportCsvToSqlDb
csv load to sql


usage: ```python csv_to_sqlserver.py <csv_filename> <schema> dbconfig.json```

Config:
```json
{
    "dbconfig": [
     
       {
          "conn_name":"",
          "server": "",
          "database": "",
          "username": "",
          "password": "",
          "server_type": "sql_server"
       }

    ]
 }
```


sql support:
SQL Server: ```sql_server```
