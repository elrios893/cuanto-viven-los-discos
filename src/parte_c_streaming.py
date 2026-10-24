"""Parte C (consumidor): Structured Streaming sobre Kafka.

Agrupa por modelo en ventanas de 7 días (tiempo del evento = fecha de la fila) con watermark
de 2 días, y calcula días-disco, fallas y tasa anualizada por ventana.

Corre dentro del cluster (Python 3.8); el conector de Kafka se baja con --packages:
  docker exec discos-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 --conf spark.jars.ivy=/tmp/ivy \
      /opt/jobs/parte_c_streaming.py [segundos_maximos]
"""
import csv
import os
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

KAFKA = "kafka:19092"
TOPICO = "discos"
SALIDA = "/resultados/parte_c_ventanas.csv"
CHECKPOINT = "/tmp/checkpoint_parte_c"
TIMEOUT = float(sys.argv[1]) if len(sys.argv) > 1 else None

ESQUEMA = StructType([
    StructField("date", StringType()),
    StructField("serial_number", StringType()),
    StructField("model", StringType()),
    StructField("capacity_bytes", LongType()),
    StructField("failure", IntegerType()),
])

# Estado acumulado en el driver: (inicio de ventana, modelo) -> (fin, días-disco, fallas).
# En modo "update" cada microlote trae solo las ventanas que cambiaron.
estado = {}


def procesar(lote, id_lote):
    filas = lote.collect()
    for r in filas:
        estado[(r["inicio"], r["modelo"])] = (r["fin"], r["dias_disco"], r["fallas"])
    os.makedirs(os.path.dirname(SALIDA), exist_ok=True)
    with open(SALIDA, "w", newline="") as out:
        w = csv.writer(out)
        w.writerow(["inicio", "fin", "modelo", "dias_disco", "fallas", "afr_pct"])
        for (ini, modelo), (fin, dias, fallas) in sorted(estado.items(), key=lambda kv: (kv[0][0], kv[0][1])):
            w.writerow([ini, fin, modelo, dias, fallas, round(fallas / (dias / 365.0) * 100, 4)])
    print("lote %d: %d ventanas actualizadas, %d (ventana, modelo) acumuladas" % (id_lote, len(filas), len(estado)), flush=True)
    for r in sorted(filas, key=lambda r: (r["inicio"], r["modelo"])):
        print("   %s a %s  %-22s días-disco=%7d fallas=%3d  AFR=%6.2f %%" % (
            str(r["inicio"])[:10], str(r["fin"])[:10], r["modelo"], r["dias_disco"], r["fallas"], r["afr_pct"]), flush=True)


def main():
    spark = (SparkSession.builder.appName("parte_c_streaming")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.cores.max", "2")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    eventos = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", KAFKA)
               .option("subscribe", TOPICO)
               .option("startingOffsets", "earliest")
               .load()
               .select(F.from_json(F.col("value").cast("string"), ESQUEMA).alias("e"))
               .select("e.*")
               .withColumn("fecha", F.to_timestamp("date")))

    ventanas = (eventos
                .withWatermark("fecha", "2 days")
                .groupBy(F.window("fecha", "7 days").alias("w"), F.col("model").alias("modelo"))
                .agg(F.count("*").alias("dias_disco"), F.sum("failure").alias("fallas"))
                .select(F.col("w.start").alias("inicio"), F.col("w.end").alias("fin"), "modelo",
                        "dias_disco", F.col("fallas").cast("long").alias("fallas"),
                        (F.col("fallas") / (F.col("dias_disco") / 365.0) * 100).alias("afr_pct")))

    consulta = (ventanas.writeStream
                .outputMode("update")
                .foreachBatch(procesar)
                .option("checkpointLocation", CHECKPOINT)
                .trigger(processingTime="5 seconds")
                .start())
    consulta.awaitTermination(TIMEOUT)
    consulta.stop()
    spark.stop()


if __name__ == "__main__":
    main()
