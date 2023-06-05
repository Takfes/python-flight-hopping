
# WORKFLOW

# JUPYTER NOTEBOOK CONTAINER
### Access token
docker exec -it jupyter /bin/sh  
jupyter notebook list  
exit  



# POSTGRES CONTAINER
# http://www.postgresqltutorial.com/psql-commands/
docker exec -it postgres /bin/bash
psql -d postgres -U postgres -W
SELECT version();
CREATE DATABASE manolisdb;
\l
\c postgres

# run the following query to see HOME DIRECTORY FOR POSTGRES
select setting from pg_settings where name = 'data_directory';
SHOW data_directory;
-- HOME DIRECTORY    : /var/lib/postgresql/data 
-- MOUNTED DIRECTORY : /var/lib/postgresql/DockerShare
\COPY flights FROM '/var/lib/postgresql/DockerShare/flights_17122019.csv' DELIMITER ',' CSV HEADER;



## COMMANDS 
docker-compose up -d  
docker-compose stop  
docker container prune -f
docker-compose rm -f  
docker-compose stop && docker-compose rm -f  

docker ps -a

> https://www.thegeekstuff.com/2016/04/docker-compose-up-stop-rm/

## DEBUGGING
docker logs container_name

## ACCESS CONTAINER
docker exec -it neo4j /bin/sh
