#IMPORT DE LIBS DO PYTHON PARA EXECUÇÃO
import argparse
import csv
import logging
import os
import re 

import apache_beam as beam
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import AsDict
from datetime import datetime

#SCHEMA DA TABELA NO BANCO
SCHEMA = "component_id:STRING,component_type_id:STRING,type:STRING,connection_type_id:STRING,outside_shape:STRING,base_type:STRING,height_over_tube:NUMERIC,bolt_pattern_long:NUMERIC,bolt_pattern_wide:NUMERIC,groove:BOOLEAN,base_diameter:NUMERIC,shoulder_diameter:NUMERIC,unique_feature:BOOLEAN,orientation:BOOLEAN,weight:NUMERIC"

#PERCORRE O ARQUIVO CSV, FAZENDO OS TRATAMENTOS NECESSÁRIOS PARA O PROCESSO DE INGESTÃO
class DataIngestion():
    def parse_method(self, string_input):
        import csv
        row = list(csv.reader([string_input]))[0]
        for r in range(len(row)):
            if row[r]=='' or row[r]=='NA':
                if r in (6,7,8,10,11,14):
                    row[r]=0
                else:
                    row[r]='NULL'
            if r in (9,12,13):
                if row[r] == 'Yes':
                    row[r] = True
                else:
                    row[r] = False
        return row
        
#RECEBE OS CAMPOS DO ARQUIVO E CONVERTE PARA A TIPAGEM CORRETA
def trata(row):
    from datetime import datetime
    x = {
    "component_id": str(row[0]),
    "component_type_id": str(row[1]),
    "type": str(row[2]),
    "connection_type_id": str(row[3]),
    "outside_shape": str(row[4]),
    "base_type": str(row[5]),
    "height_over_tube": float(row[6]),
    "bolt_pattern_long": float(row[7]),
    "bolt_pattern_wide": float(row[8]),
    "groove": bool(row[9]),
    "base_diameter": float(row[10]),
    "shoulder_diameter": float(row[11]),
    "unique_feature": bool(row[12]),
    "orientation": bool(row[13]),
    "weight": float(row[14])
    }
    return x
    
#SESSÃO DE CONFIGURAÇÃO DO PROCESSO DE INGESTÃO: ARQUIVO, TABELA, PROJETO
def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input',
        default='gs://dotz-project-test/Fontes/comp_boss.csv' #ENDEREÇO DO ARQUIVO NO BUCKET

    )

    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        default='dotz_project_test.comp_boss' #SCHEMA E TABELA NO BIGQUERY
    )

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--runner=DataflowRunner',
        '--project=optimum-nebula-291101', #NOME DO PROJETO 
        '--staging_location=gs://dotz-project-test/Stage', #ENDEREÇO DA STAGE NO BUCKET
        '--temp_location=gs://dotz-project-test/Temp', #ENDEREÇO DA PASTA TEMPORÁRIA NO BUCKET
        '--job_name=dotz-project-test2', #NOME DO JOB QUE IRÁ PARA O DATA FLOW
        '--region=us-central1' #REGIÃO DO SERVIDOR DO BUCKET NO STORAGE
    ])
    data_ingestion = DataIngestion()

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    row = (
        p
        |'Read from text' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
        |'String to Bigquery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s))
        |'Tratamento dos dados' >> beam.Map(trata)
        |'Ingestao no BigQuery' >> beam.io.WriteToBigQuery(
            known_args.output,
            schema=SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        ))
    p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()


#SCHEMA DA TABELA NO BANCO
SCHEMA = "tube_assembly_id:STRING,component_id_1:STRING,quantity_1:INTEGER,component_id_2:STRING,quantity_2:INTEGER,component_id_3:STRING,quantity_3:INTEGER,component_id_4:STRING,quantity_4:INTEGER,component_id_5:STRING,quantity_5:INTEGER,component_id_6:STRING,quantity_6:INTEGER,component_id_7:STRING,quantity_7:INTEGER,component_id_8:STRING,quantity_8:INTEGER"

#PERCORRE O ARQUIVO CSV, FAZENDO OS TRATAMENTOS NECESSÁRIOS PARA O PROCESSO DE INGESTÃO
class DataIngestion2():
    def parse_method(self, string_input):
        import csv
        row = list(csv.reader([string_input]))[0]
        for r in range(len(row)):
            if row[r]=='' or row[r]=='NA':
                if r in (0,1,3,5,7,9,11,13,15):
                    row[r]= 'NULL'
                else:
                    row[r]=0
        return row

#RECEBE OS CAMPOS DO ARQUIVO E CONVERTE PARA A TIPAGEM CORRETA
def trata(row):
    if len(row) > 0:
        x = {
        "tube_assembly_id": str(row[0]),
        "component_id_1": str(row[1]),
        "quantity_1": int(row[2]),
        "component_id_2": str(row[3]),
        "quantity_2": int(row[4]),
        "component_id_3": str(row[5]),
        "quantity_3": int(row[6]),
        "component_id_4": str(row[7]),
        "quantity_4": int(row[8]),
        "component_id_5": str(row[9]),
        "quantity_5": int(row[10]),
        "component_id_6": str(row[11]),
        "quantity_6": int(row[12]),
        "component_id_7": str(row[13]),
        "quantity_7": int(row[14]),
        "component_id_8": str(row[15]),
        "quantity_8": int(row[16])
        }
    else:
        x = {}
    return x

#SESSÃO DE CONFIGURAÇÃO DO PROCESSO DE INGESTÃO: ARQUIVO, TABELA, PROJETO 
def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input',
        default='gs://dotz-project-test/Fontes/bill_of_materials.csv' #ENDEREÇO DO ARQUIVO NO BUCKET

    )

    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        default='dotz_project_test.bill_of_materials' #SCHEMA E TABELA NO BIGQUERY
    )

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--runner=DataflowRunner',
        '--project=optimum-nebula-291101', #NOME DO PROJETO
        '--staging_location=gs://dotz-project-test/Stage', #ENDEREÇO DA STAGE NO BUCKET
        '--temp_location=gs://dotz-project-test/Temp', #ENDEREÇO DA PASTA TEMPORÁRIA NO BUCKET
        '--job_name=dotz-project-test2', #NOME DO JOB QUE IRÁ PARA O DATA FLOW
        '--region=us-central1' #REGIÃO DO SERVIDOR DO BUCKET NO STORAGE
    ])
    data_ingestion = DataIngestion2()

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    row = (
        p
        |'Read from text' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
        |'String to Bigquery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s))
        |'Tratamento dos dados' >> beam.Map(trata)
        |'Ingestao no BigQuery' >> beam.io.WriteToBigQuery(
            known_args.output,
            schema=SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        ))
    p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
    
#SCHEMA DA TABELA NO BANCO
SCHEMA = "tube_assembly_id:STRING,supplier:STRING,quote_date:DATE,annual_usage:INTEGER,min_order_quantity:INTEGER,bracket_pricing:BOOLEAN,quantity:INTEGER,cost:FLOAT"

#PERCORRE O ARQUIVO CSV, FAZENDO OS TRATAMENTOS NECESSÁRIOS PARA O PROCESSO DE INGESTÃO
class DataIngestion3():
    def parse_method(self, string_input):
        import csv
        row = list(csv.reader([string_input]))[0]
        for r in range(len(row)):
            if row[r]=='' or row[r]=='NA':
                if r in (0,1,2,5):
                    row[r]='NULL'
                else:
                    row[r]=0
            if r == 7:
                stage = str(row[r])
                row[r] = float(stage.split('.')[0])
            if r == 5:
                if row[r] == 'Yes':
                    row[r] = True
                else:
                    row[r] = False

        return row

#RECEBE OS CAMPOS DO ARQUIVO E CONVERTE PARA A TIPAGEM CORRETA
def trata(row):
    from datetime import datetime
    x = {
    "tube_assembly_id": str(row[0]),
    "supplier": str(row[1]),
    "quote_date": str(datetime.strptime(row[2], '%Y-%m-%d').date()),
    "annual_usage": int(row[3]),
    "min_order_quantity": int(row[4]),
    "bracket_pricing": bool(row[5]),
    "quantity": int(row[6]),
    "cost": float(row[7])
    }
    return x

#SESSÃO DE CONFIGURAÇÃO DO PROCESSO DE INGESTÃO: ARQUIVO, TABELA, PROJETO
def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input',
        default='gs://dotz-project-test/Fontes/price_quote.csv' #ENDEREÇO DO ARQUIVO NO BUCKET

    )

    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        default='dotz_project_test.price_quote' #SCHEMA E TABELA NO BIGQUERY
    )

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--runner=DataflowRunner',
        '--project=optimum-nebula-291101', #NOME DO PROJETO
        '--staging_location=gs://dotz-project-test/Stage', #ENDEREÇO DA STAGE NO BUCKET
        '--temp_location=gs://dotz-project-test/Temp', #ENDEREÇO DA PASTA TEMPORÁRIA NO BUCKET
        '--job_name=dotz-project-test', #NOME DO JOB QUE IRÁ PARA O DATA FLOW
        '--region=us-central1' #REGIÃO DO SERVIDOR DO BUCKET NO STORAGE
    ])
    data_ingestion = DataIngestion3()

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    row = (
        p
        |'Read from text' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
        |'String to Bigquery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s))
        |'Tratamento dos dados' >> beam.Map(trata)
        |'Ingestao no BigQuery' >> beam.io.WriteToBigQuery(
            known_args.output,
            schema=SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        ))
    p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()