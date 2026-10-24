"""Parte C (producer): publica en Kafka los registros día por día con reloj acelerado.

Lee data/slim/<fecha>.csv en orden, filtra unos pocos modelos y envía cada fila como JSON
al tema "discos". Un día de datos se emite cada --segundos-por-dia segundos.

Uso: python -I src/parte_c_producer.py [--dias 14] [--segundos-por-dia 10]
"""
import argparse
import csv
import json
import time
from pathlib import Path

from kafka import KafkaProducer

DATOS = Path(__file__).resolve().parent.parent / "data" / "slim"
# Modelos con volumen y fallas suficientes en la Parte A, y pocos para que el producer aguante.
MODELOS = ["TOSHIBA MG07ACA14TA", "ST12000NM0008", "ST8000NM0055"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topico", default="discos")
    ap.add_argument("--modelos", nargs="+", default=MODELOS)
    ap.add_argument("--dias", type=int, default=14)
    ap.add_argument("--desde", default=None, help="fecha inicial AAAA-MM-DD (por defecto, el primer archivo)")
    ap.add_argument("--segundos-por-dia", type=float, default=10.0)
    args = ap.parse_args()

    archivos = sorted(DATOS.glob("*.csv"))
    if args.desde:
        archivos = [f for f in archivos if f.stem >= args.desde]
    archivos = archivos[: args.dias]
    modelos = set(args.modelos)
    print("Modelos: %s\nDías: %d (%s a %s), %.1f s por día" % (
        sorted(modelos), len(archivos), archivos[0].stem, archivos[-1].stem, args.segundos_por_dia), flush=True)

    productor = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
        linger_ms=50,
        batch_size=128 * 1024,
    )

    total = 0
    for archivo in archivos:
        inicio = time.time()
        enviadas = 0
        with open(archivo, newline="", encoding="utf-8") as f:
            for fila in csv.DictReader(f):
                if fila["model"] not in modelos:
                    continue
                fila["failure"] = int(fila["failure"] or 0)
                productor.send(args.topico, key=fila["serial_number"], value=fila)
                enviadas += 1
        productor.flush()
        total += enviadas
        gastado = time.time() - inicio
        pausa = args.segundos_por_dia - gastado
        print("%s: %d filas enviadas en %.1f s (total %d)%s" % (
            archivo.stem, enviadas, gastado, total, "" if pausa > 0 else "  ¡el producer no alcanza el ritmo!"), flush=True)
        if pausa > 0:
            time.sleep(pausa)
    productor.close()
    print("Listo: %d mensajes" % total)


if __name__ == "__main__":
    main()
