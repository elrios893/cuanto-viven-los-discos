"""Descarga Q2 2026 de Backblaze Drive Stats y deja solo las 5 columnas que usa el ejercicio.

Lee cada CSV diario directo del ZIP remoto (peticiones por rango, sin bajar los 2,1 GB)
y escribe data/slim/<fecha>.csv. Se puede relanzar: salta los días ya descargados.

Uso: python -I src/descargar_datos.py
"""
import csv
import io
import sys
import time
from pathlib import Path

from remotezip import RemoteZip

URL = "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q2_2026.zip"
COLUMNAS = ["date", "serial_number", "model", "capacity_bytes", "failure"]
DESTINO = Path(__file__).resolve().parent.parent / "data" / "slim"
REINTENTOS = 4


def bajar_dia(z, info):
    fecha = Path(info.filename).stem
    salida = DESTINO / f"{fecha}.csv"
    if salida.exists():
        return fecha, None
    parcial = salida.with_suffix(".part")
    filas = 0
    with z.open(info.filename) as crudo, open(parcial, "w", newline="", encoding="utf-8") as out:
        lector = csv.reader(io.TextIOWrapper(crudo, encoding="utf-8", newline=""))
        cabecera = next(lector)
        idx = [cabecera.index(c) for c in COLUMNAS]
        escritor = csv.writer(out)
        escritor.writerow(COLUMNAS)
        for fila in lector:
            escritor.writerow([fila[i] for i in idx])
            filas += 1
    parcial.replace(salida)
    return fecha, filas


def main():
    DESTINO.mkdir(parents=True, exist_ok=True)
    with RemoteZip(URL, timeout=60) as z:
        dias = sorted((i for i in z.infolist() if i.filename.endswith(".csv")), key=lambda i: i.filename)
        print(f"{len(dias)} archivos en el ZIP", flush=True)
        for n, info in enumerate(dias, 1):
            for intento in range(1, REINTENTOS + 1):
                try:
                    fecha, filas = bajar_dia(z, info)
                    estado = "ya existía" if filas is None else f"{filas} filas"
                    print(f"[{n}/{len(dias)}] {fecha}: {estado}", flush=True)
                    break
                except Exception as e:  # red intermitente: reintentar
                    print(f"[{n}/{len(dias)}] {info.filename} intento {intento} falló: {e}", flush=True)
                    time.sleep(5 * intento)
            else:
                sys.exit(f"no se pudo bajar {info.filename}")


if __name__ == "__main__":
    main()
