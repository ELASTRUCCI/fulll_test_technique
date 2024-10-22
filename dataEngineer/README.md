# Intermediate Data Engineer test

## Contexte
A supermarket stores its transaction data in a relational database for further analysis. The transaction data is uploaded to the system as a CSV file multiple times per day. Each transaction has a unique id. As a data engineer, your task is to implement and maintain a pipeline to store the transaction data in the database.


## ETL PART
# MISE EN PLACE DU PROJET
- Installer venv si pas déjà installé :
```shell script
sudo apt-get install python3.12-venv
```

- Créer un environnement virtuel :
```shell script
python3.12 -m venv .penv 
```

- Activer l'environnement virtuel :
```shell script
source .penv/bin/activate
```

- Installer les paquettages Python à l'aide de pip3 :
```shell script
pip3 install -r requirements.txt
```
# General information
Input file should be place into data/input_data directory

# run the script
```bash
python3 etl.py
```

## EXPLORE DATABASE
The sql request are present in explore.sql file

