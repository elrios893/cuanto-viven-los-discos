"""Parte D (paso 1): agregados por modelo y ventana de 7 días sobre el trimestre completo.

Usa la misma ventana que la Parte C (window de 7 días, UTC) pero en modo batch y con los
91 días y todos los modelos. Escribe resultados/parte_d_ventanas.csv, que carga parte_d_mongo.py.

Corre dentro del cluster:
  docker exec discos-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
      --conf spark.jars.ivy=/tmp/ivy /opt/jobs/parte_d_agregados.py
"""
import csv
import os

from pyspark.sql import SparkSession, functions as F

DATOS = "/data/slim"
SALIDA = "/resultados/parte_d_ventanas.csv"


def main():
    rutas = [os.path.join(DATOS, f) for f in sorted(os.listdir(DATOS)) if f.endswith(".csv")]
    spark = (SparkSession.builder.appName("parte_d_agregados")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())

    filas = (spark.read.csv(rutas, header=True)
             .withColumn("fecha", F.to_timestamp("date"))
             .groupBy(F.window("fecha", "7 days").alias("w"), F.col("model").alias("modelo"))
             .agg(F.count("*").alias("dias_disco"),
                  F.sum(F.col("failure").cast("int")).alias("fallas"),
                  F.countDistinct("date").alias("dias_en_ventana"))
             .select(F.date_format("w.start", "yyyy-MM-dd").alias("inicio"),
                     F.date_format("w.end", "yyyy-MM-dd").alias("fin"),
                     "modelo", "dias_disco", "fallas", "dias_en_ventana")
             .orderBy("inicio", "modelo")
             .collect())

    with open(SALIDA, "w", newline="") as out:
        w = csv.writer(out)
        w.writerow(["inicio", "fin", "modelo", "dias_disco", "fallas", "dias_en_ventana"])
        w.writerows((r["inicio"], r["fin"], r["modelo"], r["dias_disco"], r["fallas"], r["dias_en_ventana"])
                    for r in filas)
    print("%d filas (ventana, modelo) escritas en %s" % (len(filas), SALIDA))
    spark.stop()


if __name__ == "__main__":
    main()
