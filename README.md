# Teste Técnico - Analytics Engineer & Data Engineer

## Requisitos

Cluster padrão da Databricks Community

![Cluster](imgs/db_cluster.png)

## Instruções de Execução

- Importe o script Python `Teste_tecnico_PicPay.py` ou o notebook `Teste_tecnico_PicPay.ipynb` para o workspace do Databricks.
- Execute o script em um cluster padrão da versão Community do Databricks ou em outro ambiente que suporte PySpark, com acesso ao DBFS e Delta Lake.

## Resultados finais

Tabelas de `cadastros`, `observacoes` e `remuneração` particionadas pelo `ano` e `mês`.

Tabelas na UI do Databricks

![Tables](imgs/tables.png)

Tabela de Cadastro

![Table Cadastro](imgs/table_cadastro.png)

Tabela de Observações

![Table Observacoes](imgs/table_observacoes.png)

Tabela de Remunerações

![Table Remuneracoes](imgs/table_remuneracoes.png)

O código para a carga incremental e periódica das estatísticas descritivas das tabelas está presente no Jupyter Notebook.
