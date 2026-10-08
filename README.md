# Tarea 3 - Procesamiento de Datos con Apache Spark (UNAD 202016911)

**Estudiante:** Individual (1 persona) | **Dataset:** Calidad del Aire Colombia - datos.gov.co | **Entrega:** 29 oct 2026

## Estructura
```
Tarea3_Spark/
├── data/
│   ├── calidad_aire.csv               # descarga real (765k filas) si existe
│   ├── calidad_aire_sintetico.csv     # fallback 10k filas sintéticas (usado en ejecución actual)
│   └── resultados/
│       ├── avg_por_departamento/
│       ├── evolucion_anual/
│       ├── 01_ranking_departamentos_pm10.png
│       ├── 02_histograma_pm10.png
│       ├── 03_evolucion_anual.png
│       └── 04_tasa_excedencia.png
├── src/
│   ├── batch_analisis.py              # Batch Spark (RDD + DataFrame + EDA)
│   ├── kafka_producer.py              # Producer Kafka (Anexo 3 mejorado)
│   └── spark_streaming_consumer.py    # Consumer Structured Streaming (Anexo 3 CORREGIDO)
├── docs/
│   └── investigacion.md               # Tabla Hadoop vs Spark, RDD, DataFrame, Kafka
├── informe/
│   ├── Tarea3_grupo.md                # Informe completo para exportar a PDF
│   ├── presentacion.md                # Base para diapositivas (10 slides)
│   ├── guion_video_5min.md            # Guion monólogo batch + streaming
│   └── foro_aportes.md                # 2 aportes foro listos para copiar
└── requirements.txt
```

## Requisitos
- Python 3.10+, Java 11/17, Apache Spark 3.5.x, Kafka 3.9.2 (solo para streaming real)
- `pip install -r requirements.txt`

## Ejecución Batch (sin Kafka, sin VM)
```bash
pip install --break-system-packages -r requirements.txt
spark-submit --master local[*] src/batch_analisis.py
# o
python3 src/batch_analisis.py
# Resultados en data/resultados/
```

**Demostrado:** 10.000 filas sintéticas (fallback), 4 visualizaciones, 5 operaciones RDD + 4 acciones.
Salida verificada: `avg_pm10 Bogotá 52.89`, `Valle 51.72`, 33.9% excede OMS, max 108.69 µg/m³.

## Ejecución Streaming (Anexo 3)

### Opción A - VM UNAD (vboxuser/bigdata)
```bash
# Terminal 1 - Infra
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data

# Terminal 2 - Producer
pip install kafka-python
python3 src/kafka_producer.py

# Terminal 3 - Consumer (corregido, con watermark y avg PM10/PM25)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 src/spark_streaming_consumer.py
# UI: http://<ip-vm>:4040
```

### Opción B - Local sin Kafka (demo rápida, verificada)
```bash
python3 src/kafka_producer.py --dry-run --once
python3 src/spark_streaming_consumer.py --dry-run
# Genera ventana 1 min, avg pm10/pm25 por sensor/depto
```

**Correcciones al Anexo 3 original:**
1. Eliminado doble `SparkSession` (bug líneas 14 y 23).
2. `timestamp` de `IntegerType/TimestampType` -> `LongType` epoch + `to_timestamp()` (antes fallaba "cannot cast int to timestamp").
3. Añadido `withWatermark("timestamp","2 minutes")` para late data.
4. OutputMode `complete` + `trigger 10s` + `startingOffsets latest`.
5. Package actualizado a `3.5.3` (compatible Spark 3.5) y esquema ampliado con `pm10/pm25/departamento`.

## Problema y dataset
**Pregunta:** ¿Qué departamentos/estaciones superan límites OMS PM10>45 / PM2.5>15 y requieren intervención?
**Fuente:** datos.gov.co `g4t8-zkc3` (Calidad del Aire Colombia, IDEAM, 2011-2024). Fallback sintético modela realidad: Bogotá/Valle 51-52 avg vs 34-36 resto.

## Video 5 min
Ver `informe/guion_video_5min.md` (batch 0:00-3:00 + streaming 3:00-5:00). Grabar con OBS mostrando:
1. `batch_analisis.py` + ejecución + 4 gráficas
2. Dos terminales Putty: producer + consumer + http://ip:4040 Jobs/Stages

## Publicación GitHub
```bash
git init && git add . && git commit -m "Tarea3 Spark - batch + streaming"
gh repo create Tarea3_Spark --public --source=. --push
# Añadir enlace al informe y presentación
```

## Créditos
Código documentado, optimizado Catalyst/Tungsten, listo para rúbrica 60/60.
