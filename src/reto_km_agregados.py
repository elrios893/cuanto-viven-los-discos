"""Reto Kaplan-Meier (paso 1): tiempos de vida agregados por disco.

Por cada serial resume el trimestre completo: modelo, primer y último día con registro,
días con registro y si falló. Escribe resultados/km_vida_por_disco.csv, que consume el notebook
para estimar las curvas de supervivencia en Python.

Corre dentro del cluster:
  docker exec discos-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
      --conf spark.jars.ivy=/tmp/ivy /opt/jobs/reto_km_agregados.py
"""
import csv
import os

from pyspark.sql import SparkSession, functions as F

DATOS = "/data/slim"
SALIDA = "/resultados/km_vida_por_disco.csv"


def main():
    rutas = [os.path.join(DATOS, f) for f in sorted(os.listdir(DATOS)) if f.endswith(".csv")]
    spark = SparkSession.builder.appName("reto_km_agregados").getOrCreate()

    por_disco = (spark.read.csv(rutas, header=True)
                 .groupBy("serial_number")
                 .agg(F.first("model").alias("modelo"),
                      F.countDistinct("model").alias("n_modelos"),
                      F.min("date").alias("primer_dia"),
                      F.max("date").alias("ultimo_dia"),
                      F.count("*").alias("dias_registrados"),
                      F.max(F.col("failure").cast("int")).alias("fallo"))
                 .cache())

    multi = por_disco.filter(F.col("n_modelos") > 1).count()
    print("discos con más de un modelo asociado: %d" % multi)

    filas = por_disco.select("serial_number", "modelo", "primer_dia", "ultimo_dia",
                             "dias_registrados", "fallo").collect()
    with open(SALIDA, "w", newline="") as out:
        w = csv.writer(out)
        w.writerow(["serial_number", "modelo", "primer_dia", "ultimo_dia", "dias_registrados", "fallo"])
        w.writerows(tuple(r) for r in filas)
    print("%d discos escritos en %s (fallaron: %d)" % (len(filas), SALIDA, sum(r["fallo"] for r in filas)))
    spark.stop()


if __name__ == "__main__":
    main()
