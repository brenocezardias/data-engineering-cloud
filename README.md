# Desafio ingestão de dados - Dotz
Este repositório contém um projeto para o desafio técnico de criar um processo de ingestão de dados com as ferramentas do Google Cloud (GPC).

# Infraestrutura e ferramentas utilizadas
  1. Google Cloud (GPC): 
  
    - Data Flow (Ferramenta para ingestão dos dados no banco de dados), 
    - Storage (Repositório dos arquivos CSV), 
    - Big Query (Banco de dados), 
    - Data Studio (Ferramenta para criação de relatórios).
  2. Python.
  3. Apache Beam.

## Configurando o ambiente
  1. É necessário ter uma conta no GPC, e acesso ao terminal.
     Para esse desafio, instalei algumas ferramentas no ambiente, que estão em [Config](https://github.com/brenocezardias/dotz-desafio-dados/tree/main/Config)
  >  Esse código foi testado no Python 3.7.3.
  
   Se for necessário instalar as ferramentas, pode ser feito através do seguinte comando (com o arquivo em sua máquina):
  
     pip install -r ferramentas_instaladas.txt
  
  2. Antes de iniciar o processo de ingestão dos dados no banco, é necessário criar as 3 tabelas no BigQuery (GPC). Para isso, olhar o schema de cada tabela em
     [Config](https://github.com/brenocezardias/dotz-desafio-dados/tree/main/Config)
  
  3. Criar um bucket no Storage e incluir os arquivos CSV, presente em [Arquivos](https://github.com/brenocezardias/dotz-desafio-dados/tree/main/Arquivos)
  
  4. Iniciar o ambiente do Python, que pode ser feito através dos comandos abaixo (no terminal):
     
      ```pip3 install --upgrade virtualenv --user
      python3 -m virtualenv env
      source env/bin/activate
      pip3 install --quiet apache-beam[gcp]
  
## Modelagem
  Para entender um pouco melhor os dados que serão trabalhados nesse processo, é possível ver uma modelagem conceitual em
  [Modelagem](https://github.com/brenocezardias/dotz-desafio-dados/tree/main/Modelagem)
  
## Rodando os scripts
  
  1. Com o ambiente inciado, basta utilizar o script python, para que o processo de ingestão inicie, e o Job seja enviado para o Data Flow.
  Veja o scritp em [Script](https://github.com/brenocezardias/dotz-desafio-dados/tree/main/Script)
  
  2. Antes da execução, é necessário configurar os dados de destino no script, que correspondem ao seu projeto no GPC, segue abaixo os trechos:
  Linhas 66, 74, 80, 81, 82 ,83 ,84, 158, 166, 172, 173, 174, 175, 176, 247, 255, 261, 262, 263, 264, 265
  
  **Procure essas linhas no código, e altere conforme os dados do seu projeto !**
  
  3. Para rodar o script, basta inserir a linha de comando abaixo, no terminal:
  `python processo_ingestao.py`
  
  4. Aguardar a execução script, e pronto ! Os dados serão ingeridos no BigQuery.
  
  
## Visualização
  Para visualização dos dados inseridos no banco, é possível ver os relatórios em 
  [Visualização](https://github.com/brenocezardias/dotz-desafio-dados/tree/main/Visualiza%C3%A7%C3%A3o)
  
  
  ou apenas acesse através dos links abaixo:
  
  **PRICE QUOTE:**

  https://datastudio.google.com/s/ls7PUQUezgA

  **COMP BOSS:**

  https://datastudio.google.com/s/snonobAbXco

  **BILL OF MATERIALS:**

  https://datastudio.google.com/s/pXNiaxpq_tg
  
  
  **Obrigado !**
