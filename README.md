# tt-dgaln
Data Engineer technical test with the objectif of providing an environment with
a data flow from data collection to analysis using a visualization tool.

## Execution
docker compose up
 - In some case, you have to set rigths to execute the superset init file:
 - chmod u+x docker/superset-ini.sh

## Airflow
 - [localhost:8080](http://localhost:8080)
 - username: airflow
 - password: airflow



## Superset
 - [localhost:8088](http://localhost:8088)
 - username: admin
 - password: admin


## Adminer
 - [localhost:9090](http://localhost:9090)
 - Systeme: PostgreSQL
 - Serveur: postgres
 - Utilisateur: airflow
 - Mot de passe: airflow
