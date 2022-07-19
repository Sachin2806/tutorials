// Databricks notebook source
dbutils.fs.unmount("/mnt/demo")

// COMMAND ----------

val containerName = dbutils.secrets.get(scope="myscope", key="containername")
val storageAccountName = dbutils.secrets.get(scope="myscope", key="accountname")
val sas = dbutils.secrets.get(scope="myscope", key="myKey")

val url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
var config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"

// COMMAND ----------

dbutils.fs.mount(
  source = url,
  mountPoint = "/mnt/demo",
  extraConfigs = Map(config -> sas))

//Check to display all files under the blob storage
display(dbutils.fs.ls("/mnt/demo"))

// COMMAND ----------

val df = spark.read.format("csv").option("header", "true").load("/mnt/demo/Cricket_data_set_odi.csv")
display(df)

// COMMAND ----------


