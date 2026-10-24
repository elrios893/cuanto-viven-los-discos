# ¿Cuánto viven los discos?

Ejercicio guiado de Big Data (IUE, 2026-2): MapReduce, balanceo de carga y Spark Streaming aplicados a datos reales de fallas de discos duros de [Backblaze Drive Stats](https://www.backblaze.com/drivestats) (trimestre **Q2 2026**), para estimar cuánto viven según su modelo.

El resultado completo, con tablas, gráficos y explicaciones de cada parte, está en [`notebooks/discos.ipynb`](notebooks/discos.ipynb). El notebook se abre con los resultados ya calculados y no necesita Spark ni Kafka para verse.

## Qué se hizo

| Parte | Tema | Scripts |
|---|---|---|
| Datos | Descarga del trimestre leyendo el ZIP remoto por rangos y recorte a 5 columnas | `src/descargar_datos.py` |
| A | MapReduce con RDD: días-disco, fallas y tasa de falla anualizada (AFR) por modelo | `src/parte_a_mapreduce.py` |
| B | Balanceo de carga: tiempos con 2, 8, 32 y 200 particiones y sesgo por modelo | `src/parte_b_balanceo.py` |
| C | Streaming: producer de Kafka con reloj acelerado y Structured Streaming con ventanas de 7 días y watermark de 2 días | `src/parte_c_producer.py`, `src/parte_c_streaming.py` |
| D | MongoDB: agregados por modelo y ventana, y top 5 de AFR con mínimo de días-disco | `src/parte_d_agregados.py`, `src/parte_d_mongo.py` |
| Reto | Análisis de supervivencia (Kaplan-Meier) por modelo | `src/reto_km_agregados.py` y el notebook |

La AFR se calcula como `fallas ÷ (días-disco ÷ 365) × 100`.

## Resultados principales

- **Parte A.** El RDD coincide con un `groupBy` en los 81 modelos. El trimestre suma 31.968.003 días-disco y 1.522 fallas, con una AFR de **1,74 %** frente al 1,73 % que reporta Backblaze. La diferencia en los totales (un 1,3 % en días-disco y un 1,6 % en fallas) no está explicada: una hipótesis es que Backblaze filtra algunos discos, pero no se comprobó. El 99,58 % de los discos sigue vivo al final del trimestre (censura).
- **Parte B.** Con 14 días de datos y un cluster local de 4 cores, 2 a 32 particiones tardan 11 a 15 s, y con 200 el reparto sin clave sube a 19 s. El reparto por modelo se desbalancea a medida que crecen las particiones (partición mayor / media: 1,00 con 2; 1,83 con 8; 4,86 con 32; 26,33 con 200); con 200, 131 particiones quedan vacías.
- **Parte C.** Se enviaron 960.203 mensajes de 3 modelos durante 14 días. Las 9 ventanas por modelo coinciden exactamente con un cálculo directo desde los CSV.
- **Parte D.** Sin un mínimo de días-disco, el top 5 lo copan modelos con una sola falla. Con un mínimo de 100.000, el ranking se apoya en decenas o cientos de fallas.
- **Reto.** A los 91 días, la probabilidad de haber fallado va de 0,14 % a 1,86 % según el modelo, y coincide con `AFR × 91 / 365` con diferencias relativas de 1 a 2 %. Un log-rank entre dos modelos da p ≈ 10⁻⁴⁰.

## Límites que conviene tener presentes

- **El tiempo del análisis de supervivencia no es la edad del disco.** El origen es el primer día que se ve al disco en el trimestre; el dataset recortado no trae la fecha de instalación ni las horas de encendido (`smart_9_raw`, que se descartó).
- **El watermark está configurado, pero no se probó con datos tardíos**: el producer envía los días en orden.
- **Los tiempos de la Parte B** se midieron con 14 días (4,8 millones de filas) en un solo cluster local; no son una regla general.
- **El salto de AFR de `ST8000NM0055`** entre ventanas consecutivas del streaming (3,20 % a 7,96 %) no se explica solo por tener pocos datos.

## Cómo reproducirlo

Requisitos: Docker (Spark 3.5.3, Kafka 3.8 en modo KRaft y MongoDB 7.0 se levantan con `docker-compose.yml`) y Python 3.10 o superior.

```powershell
pip install -r requirements.txt
docker compose up -d                                  # Spark (1 master + 2 workers), Kafka y MongoDB
python src/descargar_datos.py                         # los 91 CSV del trimestre a data/slim (~1,8 GB)

$spark = "docker exec discos-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/ivy"
Invoke-Expression "$spark /opt/jobs/parte_a_mapreduce.py 91"      # Parte A
Invoke-Expression "$spark /opt/jobs/parte_b_balanceo.py 14 5"     # Parte B

# Parte C: dejar el consumidor escuchando y, en otra terminal, lanzar el producer
Invoke-Expression "$spark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/jobs/parte_c_streaming.py 230"
python src/parte_c_producer.py --dias 14 --segundos-por-dia 10

Invoke-Expression "$spark /opt/jobs/parte_d_agregados.py"         # Parte D, paso 1
python src/parte_d_mongo.py                                       # Parte D, paso 2
Invoke-Expression "$spark /opt/jobs/reto_km_agregados.py"         # Reto: vida por disco
```

Antes de repetir la Parte C hay que vaciar el tema `discos` de Kafka y borrar el checkpoint (`/tmp/checkpoint_parte_c` en el contenedor), o las filas se contarían dos veces.

Los comandos de Spark usan Windows PowerShell. En otro sistema, adapta la variable `$spark`.

## Estructura

```
docker-compose.yml     Spark, Kafka (KRaft) y MongoDB
src/                   Scripts de cada parte
resultados/            CSV con los resultados que usa el notebook
notebooks/discos.ipynb Resultados, gráficos y explicaciones
data/                  (no incluida) CSV recortados de Backblaze
```

`resultados/km_vida_por_disco.csv` (resumen por disco, 19 MB) no está en el repositorio; se regenera con el último comando. El notebook conserva sus resultados ya calculados.

## Datos y licencia

Los datos son de Backblaze Drive Stats y **no se incluyen en este repositorio**. Se descargan desde la página oficial. Son gratuitos citando a Backblaze como fuente, y los datos en sí no se pueden vender. Fuente: <https://www.backblaze.com/drivestats>.
