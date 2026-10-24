"""Parte A: MapReduce con RDD sobre Backblaze Drive Stats.

Calcula días-disco, fallas y AFR por modelo con reduceByKey, lo compara con un groupBy
de DataFrames y mide la censura (días observados por serial).

Corre dentro del cluster (Python 3.8):
  docker exec discos-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
      --conf spark.jars.ivy=/tmp/ivy /opt/jobs/parte_a_mapreduce.py [n_dias]
"""
import csv
import os
import sys

from pyspark.sql import SparkSession, functions as F

DATOS = "/data/slim"
SALIDA = "/resultados"
N_DIAS = int(sys.argv[1]) if len(sys.argv) > 1 else 14


def parsear(linea):
    """Una línea CSV -> (fecha, serial, modelo, falla)."""
    fecha, serial, modelo, _capacidad, falla = next(csv.reader([linea]))
    return fecha, serial, modelo, int(falla or 0)


def afr(fallas, dias_disco):
    return fallas / (dias_disco / 365.0) * 100 if dias_disco else 0.0


def main():
    archivos = sorted(f for f in os.listdir(DATOS) if f.endswith(".csv"))[:N_DIAS]
    rutas = [os.path.join(DATOS, f) for f in archivos]
    print("Archivos: %d (%s a %s)" % (len(rutas), archivos[0], archivos[-1]))

    spark = SparkSession.builder.appName("parte_a_mapreduce").getOrCreate()
    sc = spark.sparkContext

    filas = (sc.textFile(",".join(rutas))
               .filter(lambda l: not l.startswith("date,"))   # quita la cabecera de cada archivo
               .map(parsear)
               .cache())

    # --- MapReduce: (modelo, (1, falla)) -> sumar
    por_modelo = (filas.map(lambda r: (r[2], (1, r[3])))
                       .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])))
    rdd = {m: (d, f) for m, (d, f) in por_modelo.collect()}

    # --- Verificación con DataFrames
    df = spark.read.csv(rutas, header=True).groupBy("model").agg(
        F.count("*").alias("dias_disco"), F.sum(F.col("failure").cast("int")).alias("fallas"))
    dfd = {r["model"]: (r["dias_disco"], int(r["fallas"])) for r in df.collect()}
    coincide = rdd == dfd
    print("\nRDD == groupBy de DataFrames: %s (%d modelos)" % (coincide, len(rdd)))
    if not coincide:
        for m in sorted(set(rdd) | set(dfd)):
            if rdd.get(m) != dfd.get(m):
                print("  diferencia en %s: rdd=%s df=%s" % (m, rdd.get(m), dfd.get(m)))

    # --- Tabla de AFR por modelo
    tabla = sorted(((m, d, f, afr(f, d)) for m, (d, f) in rdd.items()), key=lambda t: -t[1])
    tot_d = sum(t[1] for t in tabla)
    tot_f = sum(t[2] for t in tabla)
    print("\nTop 10 modelos por días-disco:")
    print("%-34s %12s %7s %8s" % ("modelo", "dias_disco", "fallas", "AFR %"))
    for m, d, f, a in tabla[:10]:
        print("%-34s %12d %7d %8.2f" % (m, d, f, a))
    print("\nTOTAL: %d días-disco, %d fallas, AFR %.2f %%" % (tot_d, tot_f, afr(tot_f, tot_d)))

    os.makedirs(SALIDA, exist_ok=True)
    with open(os.path.join(SALIDA, "parte_a_afr_por_modelo.csv"), "w", newline="") as out:
        w = csv.writer(out)
        w.writerow(["modelo", "dias_disco", "fallas", "afr_pct"])
        w.writerows((m, d, f, round(a, 4)) for m, d, f, a in tabla)

    # --- Censura: días observados por serial
    por_serial = (filas.map(lambda r: (r[1], (1, r[3])))
                       .reduceByKey(lambda a, b: (a[0] + b[0], max(a[1], b[1]))))
    n_discos = por_serial.count()
    n_fallaron = por_serial.filter(lambda kv: kv[1][1] == 1).count()
    completos = por_serial.filter(lambda kv: kv[1][0] == len(rutas)).count()
    print("\nCensura (ventana de %d días):" % len(rutas))
    print("  discos distintos:            %d" % n_discos)
    print("  vistos los %d días completos: %d (%.1f %%)" % (len(rutas), completos, 100.0 * completos / n_discos))
    print("  fallaron en la ventana:      %d (%.3f %%)" % (n_fallaron, 100.0 * n_fallaron / n_discos))
    print("  censurados (siguen vivos):   %d (%.2f %%)" % (n_discos - n_fallaron, 100.0 * (n_discos - n_fallaron) / n_discos))

    spark.stop()


if __name__ == "__main__":
    main()
