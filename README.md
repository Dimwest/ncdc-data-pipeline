# ncdc-data-pipeline

Fetches, stores, and processes National Climatic Data Center dataset

## Description

The following diagram describes the pipeline's components and actions:

![alt text](diagram.png)

## How to

The components can be accessed and used easily locally with the following commands.

### Create containers

`docker-compose up`

### Enter containers

Python container: `docker exec -it python-etl /bin/bash`
Postgres container: `docker exec -it results-db /bin/bash`

### Run Python ETL in container

`python3 main.py`

As described on the diagram, the Python ETL performs the following actions:

#### extract.py

- Parallel download and extraction into .txt of .zip files
- Deletion of .zip files after use

#### transform.py

- Header and Data columns parsing into dictionary records
- Records transformation: 
    - drop columns
    - split datetime strings
    - empty values handling
    - type conversion
 - Deletion of .txt files after use

Results are yielded to limit memory usage.

#### load.py

- Records insertion
- Generation of COPY queries to an SQL file 

Note: the COPY queries are stored in a .sql file in the Python container but have to be applied manually,
due to an issue I encountered with files creation when running the COPY TO command 
from the Python container to Postgres.

### Connect to Postgres database in container

`psql -h localhost -p 5432 ncdc testuser`

### Check database records from psql

`SELECT * FROM ncdc_data.results LIMIT 100;`

## Next steps: 

- [ ] Unit, integration and end-to-end testing
- [ ] Refactoring, better exception handling, corrupted or errored data handling
- [ ] Find a way to execute the COPY TO directly from the Python container
- [ ] Replacement of null values (e.g. -9999, -8888)

