# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Tabelas [Silver] e [Gold]

# COMMAND ----------

display(spark.table("ifood_case.silver.yellow_trips"))

# COMMAND ----------

display(spark.table("ifood_case.gold.may_hourly_average_passengers"))

# COMMAND ----------

display(spark.table("ifood_case.gold.monthly_average_total_amount"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Valor total de viagens mensal

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ifood_case.gold.vw_monthly_amount AS
# MAGIC SELECT
# MAGIC   date_format(pickup_month, 'yyyy-MM') AS pickup_month,
# MAGIC   ROUND(average_total_amount, 2) AS average_total_amount,
# MAGIC   trip_count,
# MAGIC   negative_amount_count
# MAGIC FROM ifood_case.gold.monthly_average_total_amount;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Horária de passageiros em maio

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ifood_case.gold.vw_may_hourly_passengers AS
# MAGIC SELECT
# MAGIC   pickup_hour,
# MAGIC   ROUND(average_passenger_count, 2) AS average_passenger_count,
# MAGIC   trip_count,
# MAGIC   null_passenger_count
# MAGIC FROM ifood_case.gold.may_hourly_average_passengers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Volume total
# MAGIC
# MAGIC - Quantidade de corridas ;
# MAGIC - O volume total registrado na base é de 16 milhões de viagens no período analisado.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS total_trips FROM ifood_case.silver.yellow_trips;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Corridas por mês
# MAGIC
# MAGIC - Volume de corridas ao longo dos meses;
# MAGIC - Diferença de escala entre janeiro a maio;
# MAGIC
# MAGIC **Resumo**: Houve leve queda no volume em fevereiro, seguida por recuperação em março e crescimento até maio. Maio registrou o maior
# MAGIC   total de viagens do período, indicando tendência de alta na demanda ao longo dos meses analisados.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   pickup_month,
# MAGIC   trip_count
# MAGIC FROM ifood_case.gold.vw_monthly_amount
# MAGIC ORDER BY pickup_month;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Média mensal de total_amount
# MAGIC - Resposta direta para a primeira pergunta do case;
# MAGIC - Evolução do valor média por corrida ao longo dos meses.
# MAGIC
# MAGIC
# MAGIC **Resumo**: Houve uma leve queda em fevereiro de 2023, seguida por uma trajetória de crescimento contínuo de março a maio de 2023. No período inteiro, a média saiu de 27.02 em janeiro para 28.96 em maio, indicando aumento do valor médio por corrida.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   pickup_month,
# MAGIC   average_total_amount
# MAGIC FROM ifood_case.gold.vw_monthly_amount
# MAGIC ORDER BY pickup_month;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Média de passageiros por hora em maio
# MAGIC
# MAGIC - Resposta direta para a segunda pergunta do case;
# MAGIC - Distribuição do volume médio de passageiros ao longo das horas do dia em maio.
# MAGIC
# MAGIC ------------
# MAGIC
# MAGIC **Resumo**
# MAGIC
# MAGIC - Madrugada (0h–3h): pico mais alto, entre 1.41 e 1.44
# MAGIC - Queda forte entre 4h e 6h: o mínimo acontece por volta de 6h, com 1.23
# MAGIC - Recuperação gradual de 7h em diante
# MAGIC - Tarde/noite: estabiliza entre 1.36 e 1.41
# MAGIC - No geral, a variação existe, mas não é enorme: a média fica sempre perto de 1.2–1.4, então a maioria das corridas parece ter 1 passageiro, com parte menor tendo 2 ou mais

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   pickup_hour,
# MAGIC   average_passenger_count
# MAGIC FROM ifood_case.gold.vw_may_hourly_passengers
# MAGIC ORDER BY pickup_hour;
