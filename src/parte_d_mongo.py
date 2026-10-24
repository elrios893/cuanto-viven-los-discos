"""Parte D (paso 2): guarda los agregados en MongoDB y consulta los modelos con mayor tasa de falla.

Carga resultados/parte_d_ventanas.csv (trimestre completo) y resultados/parte_c_ventanas.csv
(salida del streaming) en la base "discos", y ejecuta las consultas del ejercicio.

Uso: python -I src/parte_d_mongo.py [--minimo 100000]
"""
import argparse
import csv
from pathlib import Path

from pymongo import ASCENDING, MongoClient

RESULTADOS = Path(__file__).resolve().parent.parent / "resultados"


def cargar(col, ruta, convertir):
    with open(ruta, newline="", encoding="utf-8") as f:
        docs = [convertir(r) for r in csv.DictReader(f)]
    col.delete_many({})
    col.insert_many(docs)
    col.create_index([("inicio", ASCENDING), ("modelo", ASCENDING)])
    return len(docs)


def doc_ventana(r):
    d = {"inicio": r["inicio"][:10], "modelo": r["modelo"],
         "dias_disco": int(r["dias_disco"]), "fallas": int(r["fallas"])}
    if "fin" in r:
        d["fin"] = r["fin"][:10]
    if "dias_en_ventana" in r:
        d["dias_en_ventana"] = int(r["dias_en_ventana"])
    return d


def top5(col, minimo, filtro=None):
    """Top 5 por AFR sumando todas las ventanas de cada modelo, con mínimo de días-disco."""
    pipeline = ([{"$match": filtro}] if filtro else []) + [
        {"$group": {"_id": "$modelo", "dias_disco": {"$sum": "$dias_disco"}, "fallas": {"$sum": "$fallas"}}},
        {"$match": {"dias_disco": {"$gte": minimo}}},
        {"$project": {"dias_disco": 1, "fallas": 1,
                      "afr_pct": {"$multiply": [{"$divide": ["$fallas", {"$divide": ["$dias_disco", 365]}]}, 100]}}},
        {"$sort": {"afr_pct": -1}},
        {"$limit": 5},
    ]
    return list(col.aggregate(pipeline))


def imprimir(titulo, filas):
    print("\n" + titulo)
    print("  %-26s %12s %7s %8s" % ("modelo", "dias_disco", "fallas", "AFR %"))
    for r in filas:
        print("  %-26s %12d %7d %8.2f" % (r["_id"], r["dias_disco"], r["fallas"], r["afr_pct"]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minimo", type=int, default=100000, help="días-disco mínimos para el trimestre")
    ap.add_argument("--uri", default="mongodb://localhost:27017")
    args = ap.parse_args()

    db = MongoClient(args.uri, serverSelectionTimeoutMS=5000)["discos"]
    n = cargar(db["ventanas"], RESULTADOS / "parte_d_ventanas.csv", doc_ventana)
    m = cargar(db["ventanas_streaming"], RESULTADOS / "parte_c_ventanas.csv", doc_ventana)
    print("Cargado: ventanas=%d documentos, ventanas_streaming=%d documentos" % (n, m))

    tot = list(db["ventanas"].aggregate([{"$group": {"_id": None, "d": {"$sum": "$dias_disco"}, "f": {"$sum": "$fallas"}}}]))[0]
    print("Control: %d días-disco, %d fallas, AFR %.2f %%" % (tot["d"], tot["f"], tot["f"] / (tot["d"] / 365.0) * 100))

    imprimir("Top 5 por AFR SIN mínimo (muestras pequeñas):", top5(db["ventanas"], 0))
    imprimir("Top 5 por AFR con mínimo de %d días-disco:" % args.minimo, top5(db["ventanas"], args.minimo))

    # Última ventana completa (7 días observados): mínimo proporcional al tamaño de la ventana.
    ultima = db["ventanas"].find_one({"dias_en_ventana": 7}, sort=[("inicio", -1)])["inicio"]
    minimo_v = max(1, args.minimo // 13)
    imprimir("Top 5 en la última ventana completa (%s) con mínimo de %d días-disco:" % (ultima, minimo_v),
             top5(db["ventanas"], minimo_v, {"inicio": ultima}))


if __name__ == "__main__":
    main()
