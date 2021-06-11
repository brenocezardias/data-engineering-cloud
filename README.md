# Data Ingest Challenge
This repository contains a project for the technical challenge of creating a data ingestion process with Google Cloud tools (GPC).

# Infrastructure and tools used
  1. Google Cloud (GPC): 
  
    - Data Flow (Tool for ingesting data in the database), 
    - Storage (Repository of CSV files), 
    - Big Query (Database), 
    - Data Studio (Tool for creating reports).
  2. Python.
  3. Apache Beam.

## Setting up the environment
  1. You must have a GPC account, and terminal access.
     For this challenge, I installed some tools in the environment, which are in [Config](https://github.com/brenocezardias/dotz-desafio-dados/tree/main/Config)
  >  This code has been tested on Python 3.7.3.
  
   If it is necessary to install the tools, it can be done through the following command (with the file on your machine):
  
     pip install -r ferramentas_instaladas.txt
  
  2. Before starting the data ingestion process in the database, it is necessary to create the 3 tables in BigQuery (GPC). For this, look at the schema of each table in
     [Config](https://github.com/brenocezardias/dotz-desafio-dados/tree/main/Config)
  
  3. Create a bucket on Storage and include the CSV files, present in [Arquivos](https://github.com/brenocezardias/dotz-desafio-dados/tree/main/Arquivos)
  
  4. Start the Python environment, which can be done using the commands below (in terminal):
     
      ```pip3 install --upgrade virtualenv --user
      python3 -m virtualenv env
      source env/bin/activate
      pip3 install --quiet apache-beam[gcp]
  
## Modeling
  To understand a little better the data that will be worked in this process, it is possible to see a conceptual modeling in
  [Modelagem](https://github.com/brenocezardias/dotz-desafio-dados/tree/main/Modelagem)
  
## Running scripts
  
  1. With the environment started, just use the python script, so that the ingestion process starts, and the Job is sent to Data Flow.
  See the script at [Script](https://github.com/brenocezardias/dotz-desafio-dados/tree/main/Script)
  
  2.Before execution, it is necessary to configure the target data in the script, which correspond to your project in the GPC, below are the excerpts:
  lines 66, 74, 80, 81, 82 ,83 ,84, 158, 166, 172, 173, 174, 175, 176, 247, 255, 261, 262, 263, 264, 265
  
  **Look for these lines in the code, and change as per your project data !**
  
  3. To run the script, just enter the command line below in the terminal:
  `python processo_ingestao.py`
  
  4. Wait for the script execution, and that's it! The data will be ingested in the BigQuery.
  
  
## Data Viz
  To view the data entered in the database, you can see the reports in 
  [Visualização](https://github.com/brenocezardias/dotz-desafio-dados/tree/main/Visualiza%C3%A7%C3%A3o)
  
  or just go through the links below.:
  
  **PRICE QUOTE:**

  https://datastudio.google.com/s/ls7PUQUezgA

  **COMP BOSS:**

  https://datastudio.google.com/s/snonobAbXco

  **BILL OF MATERIALS:**

  https://datastudio.google.com/s/pXNiaxpq_tg
  
  
  **Thanks !**
