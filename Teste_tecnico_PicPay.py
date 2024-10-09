# Databricks notebook source
import requests
from urllib.parse import urljoin
from datetime import datetime
from dateutil.relativedelta import relativedelta
import zipfile
import os
import pandas as pd
import io

from pyspark.sql.functions import (
    isnan,
    when,
    count,
    col,
    input_file_name,
    element_at,
    split,
    regexp_replace,
)
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Raw data

# COMMAND ----------

def download_and_extract_zip(data: datetime, destination_path: str):
    data_formatada = data.strftime("%Y%m")
    url = f"https://portaldatransparencia.gov.br/download-de-dados/servidores/{data_formatada}_Aposentados_SIAPE"

    try:
        response = requests.get(url)
        response.raise_for_status()
        zip_content = response.content

        with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
            for file_info in zip_file.infolist():
                with zip_file.open(file_info) as extracted_file:
                    file_content = extracted_file.read()

                    try:
                        file_text = file_content.decode('iso-8859-1')
                        dest_file_path = f"dbfs:{destination_path}{file_info.filename}"
                        dbutils.fs.put(dest_file_path, file_text, overwrite=True)
                        print(f"Arquivo '{file_info.filename}' extraído para '{destination_path}'")
                    except UnicodeDecodeError:
                        print(f"Arquivo '{file_info.filename}' não é um arquivo de texto e foi ignorado.")
        print(f"Todos os arquivos extraídos para '{destination_path}'")
    except Exception as e:
        print(f"Erro: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvando os dados históricos

# COMMAND ----------

# Caso tenhamos novos arquivos eles serão carregados automaticamente na camada Landing após a execução, estamos limitando a um historico de 3 meses a partir do ultimo arquivo M-2

end_date = datetime.today() - relativedelta(months=1) # 1 mes pois assim será incluido o ultimo mes M-2 (no caso atual agosto/2024)
start_date = end_date - relativedelta(months=3)

for file_date in pd.date_range(start_date, end_date, freq="M"):
    landing_path = f"/FileStore/landing/siape/{file_date.strftime('%Y')}/{file_date.strftime('%m')}/"
    print("Downloading and extracting the file:", file_date.strftime("%Y%m"))
    download_and_extract_zip(file_date, landing_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando o schema

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS public_informations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando a tabela de Cadastros

# COMMAND ----------

df_cadastro_raw = spark.read.csv("dbfs:/FileStore/landing/siape/*/*/*_Cadastro.csv", sep=";", encoding="UTF-8", header=True)

rename_dict = {
    'Id_SERVIDOR_PORTAL': 'id_servidor_portal',
    'NOME': 'nome',
    'CPF': 'cpf',
    'MATRICULA': 'matricula',
    'COD_TIPO_APOSENTADORIA': 'codigo_tipo_aposentadoria',
    'TIPO_APOSENTADORIA': 'tipo_aposentadoria',
    'DATA_APOSENTADORIA': 'data_aposentadoria',
    'DESCRICAO_CARGO': 'descricao_cargo',
    'COD_UORG_LOTACAO': 'codigo_uorg_lotacao',
    'UORG_LOTACAO': 'uorg_lotacao',
    'COD_ORG_LOTACAO': 'codigo_org_lotacao',
    'ORG_LOTACAO': 'org_lotacao',
    'COD_ORGSUP_LOTACAO': 'codigo_orgsup_lotacao',
    'ORGSUP_LOTACAO': 'orgsup_lotacao',
    'COD_TIPO_VINCULO': 'codigo_tipo_vinculo',
    'TIPO_VINCULO': 'tipo_vinculo',
    'SITUACAO_VINCULO': 'situacao_vinculo',
    'REGIME_JURIDICO': 'regime_juridico',
    'JORNADA_DE_TRABALHO': 'jornada_de_trabalho',
    'DATA_INGRESSO_CARGOFUNCAO': 'data_ingresso_cargofuncao',
    'DATA_NOMEACAO_CARGOFUNCAO': 'data_nomeacao_cargofuncao',
    'DATA_INGRESSO_ORGAO': 'data_ingresso_orgao',
    'DOCUMENTO_INGRESSO_SERVICOPUBLICO': 'documento_ingresso_servico_publico',
    'DATA_DIPLOMA_INGRESSO_SERVICOPUBLICO': 'data_diploma_ingresso_servico_publico',
    'DIPLOMA_INGRESSO_CARGOFUNCAO': 'diploma_ingresso_cargofuncao',
    'DIPLOMA_INGRESSO_ORGAO': 'diploma_ingresso_orgao',
    'DIPLOMA_INGRESSO_SERVICOPUBLICO': 'diploma_ingresso_servico_publico'
}

df_cadastro = df_cadastro_raw.select([
    F.col(c).alias(rename_dict.get(c, c)) for c in df_cadastro_raw.columns
])

df_cadastro = (
    df_cadastro
    .dropna(subset=["cpf"])
    .withColumn("cpf", F.regexp_replace("cpf", r"[^\d]", ""))
    .withColumn("matricula", F.regexp_replace("matricula", r"[^\d]", ""))
    .withColumn('nome_arquivo', F.element_at(F.split(F.input_file_name(), '/'), -1))
    .withColumn("mes_referencia", F.element_at(F.split("nome_arquivo", "_"), 1))
    .withColumn("ano", F.substring("mes_referencia", 1, 4))
    .withColumn("mes", F.substring("mes_referencia", 5, 2))
    .drop("mes_referencia")
)

df_cadastro.write \
    .partitionBy("ano", "mes") \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("public_informations.cadastros")

spark.sql("OPTIMIZE public_informations.cadastros ZORDER BY cpf")
# display(spark.sql("SELECT * FROM public_informations.cadastros LIMIT 100"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando a tabela de Observações

# COMMAND ----------

df_observacoes_raw = spark.read.csv("dbfs:/FileStore/landing/siape/*/*/*_Observacoes.csv", sep=";", encoding="UTF-8", header=True)

rename_dict = {
    'ANO': 'ano',
    'MES': 'mes',
    'Id_SERVIDOR_PORTAL': 'id_servidor_portal',
    'NOME': 'nome',
    'CPF': 'cpf',
    'OBSERVACAO': 'observacao',
    'filename': 'nome_arquivo',
}

df_observacoes = df_observacoes_raw.select([
    F.col(c).alias(rename_dict.get(c, c)) for c in df_observacoes_raw.columns
])

df_observacoes = (
    df_observacoes.dropna(subset=["cpf"])
    .withColumn("cpf", regexp_replace(col("cpf"), r"[^\d]", ""))
    .withColumn("nome_arquivo", element_at(split(input_file_name(), "/"), -1))
)

df_observacoes.write \
    .partitionBy("ano", "mes") \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("public_informations.observacoes")

spark.sql("OPTIMIZE public_informations.observacoes ZORDER BY cpf")
display(spark.sql("select * FROM public_informations.observacoes limit 100"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando a tabela de Remuneração

# COMMAND ----------

from pyspark.sql.types import DecimalType

decimal_precision = 15
decimal_scale = 2

df_remuneracao_raw = spark.read.csv("dbfs:/FileStore/landing/siape/*/*/*_Remuneracao.csv", sep=";", encoding="UTF-8", header=True, inferSchema=True)

rename_dict = {
    "ANO": "ano",
    "MES": "mes",
    "Id_SERVIDOR_PORTAL": "id_servidor_portal",
    "CPF": "cpf",
    "NOME": "nome",
    "REMUNERAÇÃO BÁSICA BRUTA (R$)": "remuneracao_basica_bruta_brl",
    "REMUNERAÇÃO BÁSICA BRUTA (U$)": "remuneracao_basica_bruta_usd",
    "ABATE-TETO (R$)": "abate_teto_brl",
    "ABATE-TETO (U$)": "abate_teto_usd",
    "GRATIFICAÇÃO NATALINA (R$)": "gratificacao_natalina_brl",
    "GRATIFICAÇÃO NATALINA (U$)": "gratificacao_natalina_usd",
    "ABATE-TETO DA GRATIFICAÇÃO NATALINA (R$)": "abate_teto_gratificacao_natalina_brl",
    "ABATE-TETO DA GRATIFICAÇÃO NATALINA (U$)": "abate_teto_gratificacao_natalina_usd",
    "FÉRIAS (R$)": "ferias_brl",
    "FÉRIAS (U$)": "ferias_usd",
    "OUTRAS REMUNERAÇÕES EVENTUAIS (R$)": "outras_remuneracoes_eventuais_brl",
    "OUTRAS REMUNERAÇÕES EVENTUAIS (U$)": "outras_remuneracoes_eventuais_usd",
    "IRRF (R$)": "irrf_brl",
    "IRRF (U$)": "irrf_usd",
    "PSS/RPGS (R$)": "pss_rpgs_brl",
    "PSS/RPGS (U$)": "pss_rpgs_usd",
    "DEMAIS DEDUÇÕES (R$)": "demais_deducoes_brl",
    "DEMAIS DEDUÇÕES (U$)": "demais_deducoes_usd",
    "PENSÃO MILITAR (R$)": "pensao_militar_brl",
    "PENSÃO MILITAR (U$)": "pensao_militar_usd",
    "FUNDO DE SAÚDE (R$)": "fundo_saude_brl",
    "FUNDO DE SAÚDE (U$)": "fundo_saude_usd",
    "TAXA DE OCUPAÇÃO IMÓVEL FUNCIONAL (R$)": "taxa_ocupacao_imovel_funcional_brl",
    "TAXA DE OCUPAÇÃO IMÓVEL FUNCIONAL (U$)": "taxa_ocupacao_imovel_funcional_usd",
    "REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (R$)": "remuneracao_apos_deducoes_obrigatorias_brl",
    "REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (U$)": "remuneracao_apos_deducoes_obrigatorias_usd",
    "VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - CIVIL (R$)(*)": "verbas_indenizatorias_pessoal_civil_brl",
    "VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - CIVIL (U$)(*)": "verbas_indenizatorias_pessoal_civil_usd",
    "VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - MILITAR (R$)(*)": "verbas_indenizatorias_pessoal_militar_brl",
    "VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - MILITAR (U$)(*)": "verbas_indenizatorias_pessoal_militar_usd",
    "VERBAS INDENIZATÓRIAS PROGRAMA DESLIGAMENTO VOLUNTÁRIO  MP 792/2017 (R$)": "verbas_indenizatorias_desligamento_voluntario_brl",
    "VERBAS INDENIZATÓRIAS PROGRAMA DESLIGAMENTO VOLUNTÁRIO  MP 792/2017 (U$)": "verbas_indenizatorias_desligamento_voluntario_usd",
    "TOTAL DE VERBAS INDENIZATÓRIAS (R$)(*)": "total_verbas_indenizatorias_brl",
    "TOTAL DE VERBAS INDENIZATÓRIAS (U$)(*)": "total_verbas_indenizatorias_usd",
}

decimal_cols = [
    "remuneracao_basica_bruta_brl",
    "remuneracao_basica_bruta_usd",
    "abate_teto_brl",
    "abate_teto_usd",
    "gratificacao_natalina_brl",
    "gratificacao_natalina_usd",
    "abate_teto_gratificacao_natalina_brl",
    "abate_teto_gratificacao_natalina_usd",
    "ferias_brl",
    "ferias_usd",
    "outras_remuneracoes_eventuais_brl",
    "outras_remuneracoes_eventuais_usd",
    "irrf_brl",
    "irrf_usd",
    "pss_rpgs_brl",
    "pss_rpgs_usd",
    "demais_deducoes_brl",
    "demais_deducoes_usd",
    "pensao_militar_brl",
    "pensao_militar_usd",
    "fundo_saude_brl",
    "fundo_saude_usd",
    "taxa_ocupacao_imovel_funcional_brl",
    "taxa_ocupacao_imovel_funcional_usd",
    "remuneracao_apos_deducoes_obrigatorias_brl",
    "remuneracao_apos_deducoes_obrigatorias_usd",
    "verbas_indenizatorias_pessoal_civil_brl",
    "verbas_indenizatorias_pessoal_civil_usd",
    "verbas_indenizatorias_pessoal_militar_brl",
    "verbas_indenizatorias_pessoal_militar_usd",
    "verbas_indenizatorias_desligamento_voluntario_brl",
    "verbas_indenizatorias_desligamento_voluntario_usd",
    "total_verbas_indenizatorias_brl",
    "total_verbas_indenizatorias_usd",
]

df_remuneracao = df_remuneracao_raw.select([
    F.col(c).alias(rename_dict.get(c, c)) for c in df_remuneracao_raw.columns
])

for col_name in decimal_cols:
    df_remuneracao = df_remuneracao.withColumn(col_name, regexp_replace(col(col_name), ",", ".").cast(DecimalType(decimal_precision, decimal_scale)))

df_remuneracao = (
    df_remuneracao.dropna(subset=["cpf"])
    .withColumn("cpf", regexp_replace(col("cpf"), r"[^\d]", ""))
    .withColumn("nome_arquivo", element_at(split(input_file_name(), "/"), -1))
)

df_remuneracao.write \
    .partitionBy("ano", "mes") \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("public_informations.remuneracoes")

spark.sql("OPTIMIZE public_informations.remuneracoes ZORDER BY cpf")
display(spark.sql("select * FROM public_informations.remuneracoes limit 100"))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Estatisticas descritivas das tabelas

# COMMAND ----------

from pyspark.sql.functions import count, col
from pyspark.sql import DataFrame

# Função para calcular estatísticas gerais
def get_dataframe_statistics(df: DataFrame, df_name: str):
    print(f"Estatísticas do DataFrame: {df_name}")
    
    # Contar número de linhas por ano e mes
    df_grouped = df.groupBy("ano", "mes").agg(count("*").alias("num_linhas"))
    print("Contagem de linhas por ano e mes:")
    df_grouped.show()
    
    # Contar número total de linhas
    total_rows = df.count()
    print(f"Número total de linhas: {total_rows}")
    
    # Contar número total de colunas
    total_cols = len(df.columns)
    print(f"Número total de colunas: {total_cols}")

# COMMAND ----------

get_dataframe_statistics(df_cadastro, "df_cadastro")

# COMMAND ----------

get_dataframe_statistics(df_observacoes, "df_observacoes")

# COMMAND ----------

get_dataframe_statistics(df_remuneracao, "df_remuneracao")
display(df_remuneracao.describe())

# COMMAND ----------


