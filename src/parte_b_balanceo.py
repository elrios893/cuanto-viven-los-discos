"""Parte B: balanceo de carga.

1) Tiempos: repite la agregación de la Parte A con 2, 8, 32 y 200 particiones, con reparto
   sin clave (repartition) y por modelo (partitionBy), varias repeticiones cada una.
2) Sesgo: cuenta filas por partición con spark_partition_id(), con y sin clave.

Corre dentro del cluster (Python 3.8):
  docker exec discos-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
      --conf spark.jars.ivy=/tmp/ivy /opt/jobs/parte_b_balanceo.py [n_dias] [repeticiones]
"""
import csv
import os
import statistics
import sys
import time

from pyspark.sql import SparkSession, functions as F

DATOS = "/data/slim"
SALIDA = "/resultados"
N_DIAS = int(sys.argv[1]) if len(sys.argv) > 1 else 14
REPS = int(sys.argv[2]) if len(sys.argv) > 2 else 5
PARTICIONES = [2, 8, 32, 200]
PARTICIONES_SESGO = PARTICIONES
SOLO_SESGO = len(sys.argv) > 3 and sys.argv[3] == "sesgo"  # omite la fase de tiempos


def parsear(linea):
    fecha, serial, modelo, _capacidad, falla = next(csv.reader([linea]))
    return modelo, (1, int(falla or 0))


def sumar(a, b):
    return (a[0] + b[0], a[1] + b[1])


def medir_tiempos(sc, rutas):
    pares = (sc.textFile(",".join(rutas))
               .filter(lambda l: not l.startswith("date,"))
               .map(parsear))

    def ejecutar(k, modo):
        t0 = time.time()
        base = pares.repartition(k) if modo == "sin_clave" else pares.partitionBy(k)
        n_modelos = base.reduceByKey(sumar).count()
        return time.time() - t0, n_modelos

    ejecutar(8, "sin_clave")  # calentamiento: JVM, caché de archivos y workers; no se registra
    filas = []
    for k in PARTICIONES:
        for modo in ("sin_clave", "por_modelo"):
            for rep in range(1, REPS + 1):
                seg, n = ejecutar(k, modo)
                filas.append((k, modo, rep, round(seg, 2)))
                print("  k=%-3d %-10s rep %d: %6.2f s (%d modelos)" % (k, modo, rep, seg, n), flush=True)

    print("\nResumen (segundos, %d repeticiones):" % REPS)
    print("%-10s %-11s %8s %8s %8s %8s" % ("particiones", "reparto", "media", "desv", "min", "max"))
    for k in PARTICIONES:
        for modo in ("sin_clave", "por_modelo"):
            t = [s for kk, m, _, s in filas if kk == k and m == modo]
            print("%-10d %-11s %8.2f %8.2f %8.2f %8.2f" % (
                k, modo, statistics.mean(t), statistics.pstdev(t), min(t), max(t)))
    return filas


def medir_sesgo(spark, rutas):
    df = spark.read.csv(rutas, header=True).select("model", "failure")
    total = df.count()

    top = df.groupBy("model").count().orderBy(F.desc("count")).collect()
    print("\nConcentración de filas por modelo (%d modelos, %d filas):" % (len(top), total))
    acum = 0
    for i, r in enumerate(top[:10], 1):
        acum += r["count"]
        print("  %2d. %-26s %9d filas  %5.1f %%  (acum. %5.1f %%)" % (
            i, r["model"], r["count"], 100.0 * r["count"] / total, 100.0 * acum / total))
    top5 = sum(r["count"] for r in top[:5])
    print("  Los 5 modelos mayores concentran el %.1f %% de las filas" % (100.0 * top5 / total))

    filas_csv = []
    for k in PARTICIONES_SESGO:
        for modo, repartido in (("sin_clave", df.repartition(k)),
                                ("por_modelo", df.repartition(k, "model"))):
            cuentas = (repartido.withColumn("particion", F.spark_partition_id())
                                .groupBy("particion").count().collect())
            n = {r["particion"]: r["count"] for r in cuentas}
            vals = [n.get(p, 0) for p in range(k)]
            media = total / float(k)
            print("k=%-3d %-10s min=%-8d max=%-8d media=%-8.0f max/media=%5.2f  vacías=%d de %d" % (
                k, modo, min(vals), max(vals), media, max(vals) / media, vals.count(0), k))
            for p, v in enumerate(vals):
                filas_csv.append((k, modo, p, v))
    return filas_csv


def main():
    archivos = sorted(f for f in os.listdir(DATOS) if f.endswith(".csv"))[:N_DIAS]
    rutas = [os.path.join(DATOS, f) for f in archivos]
    print("Archivos: %d (%s a %s)" % (len(rutas), archivos[0], archivos[-1]))

    spark = SparkSession.builder.appName("parte_b_balanceo").getOrCreate()
    os.makedirs(SALIDA, exist_ok=True)

    if not SOLO_SESGO:
        print("\n== Tiempos ==")
        tiempos = medir_tiempos(spark.sparkContext, rutas)
        with open(os.path.join(SALIDA, "parte_b_tiempos.csv"), "w", newline="") as out:
            w = csv.writer(out)
            w.writerow(["particiones", "reparto", "repeticion", "segundos"])
            w.writerows(tiempos)

    print("\n== Sesgo ==")
    sesgo = medir_sesgo(spark, rutas)
    with open(os.path.join(SALIDA, "parte_b_particiones.csv"), "w", newline="") as out:
        w = csv.writer(out)
        w.writerow(["particiones", "reparto", "particion", "filas"])
        w.writerows(sesgo)

    spark.stop()


if __name__ == "__main__":
    main()
