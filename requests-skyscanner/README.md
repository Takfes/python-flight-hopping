# CREATE NEW ENVIRONMENT
conda create --name py2 python=2.7
conda info --env
activate py2
conda install pip
pip install --upgrade pip
pip install unirest
pycharm > conda environments > add > existing > c/users/.../anaconda3/envs/envName/python.exe

# COMMANDS 
docker-compose up -d  
docker-compose stop  
docker container prune -f
docker-compose rm -f  
docker-compose stop && docker-compose rm -f  

> https://www.thegeekstuff.com/2016/04/docker-compose-up-stop-rm/

# DEBUGGING
docker logs container_name

# ACCESS CONTAINER
docker exec -it neo4j /bin/sh
docker exec -it minio /bin/sh