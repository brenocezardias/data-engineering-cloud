# Desafio ingestão de dados - Dotz
Este repositório contém um projeto para o desafio técnico de criar um processo de ingestão de dados com as ferramentas do Google Cloud (GPC).

## Configurando o ambiente
  1. É necessário ter uma conta no GPC, e acesso ao terminal.
     Para esse desafio, instalei algumas ferramentas no ambiente, que estão em [Config](https://github.com/brenocezardias/dotz-desafio-dados/tree/main/Config)
  >  Esse código foi testado no Python 3.7.3
  
     Se for necessário instalar as ferramentas, pode ser feito através do seguinte comando (com o arquivo em sua máquina):
  
     `pip install -r ferramentas_instaladas.txt`
  
  2. Antes de iniciar o processo de ingestão dos dados no banco, é necessário criar as 3 tabelas no BigQuery (GPC). Para isso, olhar o schema de cada tabela em
     [Config](https://github.com/brenocezardias/dotz-desafio-dados/tree/main/Config)
  
  3. Criar um bucket no Storage e incluir os arquivos CSV, presente em [Arquivos](https://github.com/brenocezardias/dotz-desafio-dados/tree/main/Arquivos)
  
## Modelagem
  Para entender um pouco melhor os dados que serão trabalhados nesse processo, é possível ver uma modelagem conceitual em
  [Modelagem]()
  
## Rodando os scripts
  
  Com o ambiente inciado, basta utilizar os scripts python, para que o processo de ingestão inicie, e o Job seja enviado para o Data Flow.
  Veja os scritps em [Scripts]()
  
  
## Visualização
  Para visualização dos dados inseridos no banco, é possível ver um report em [Visualização]()
  
  
  **Obrigado !
