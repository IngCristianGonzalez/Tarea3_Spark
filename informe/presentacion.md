# Presentación Tarea 3 - 10 diapositivas

**Diapositiva 1 - Portada**
Título: Calidad del Aire en Colombia: Batch + Streaming con Spark & Kafka
Sub: UNAD Big Data 202016911 - Tarea 3 | Estudiante: [Nombre] | Oct 2026

**Diapositiva 2 - Problema**
Pregunta: ¿Qué regiones superan límites OMS PM10>45 / PM2.5>15?
Impacto: Salud pública, 70% muestras exceden PM2.5. Solución Big Data necesaria por volumen nacional multi-anual + IoT tiempo real.

**Diapositiva 3 - Dataset**
Fuente: datos.gov.co g4t8-zkc3 IDEAM (765k registros horarios + 8.8k PM). Fallback sintético 10k modelado realista.
Columnas: Departamento, Municipio, Estacion, Ano, PM10, PM25.
Enlace directo CSV y captura portal.

**Diapositiva 4 - Arquitectura**
Diagrama: CSV → Spark Batch (EDA) → Resultados + Gráficas | Sensores → Kafka topic sensor_data (1 part) → Spark Structured Streaming (window 1m) → Console + Spark UI :4040
Tecnologías: HDFS (opcional), Spark 3.5.3, Kafka 3.9.2, Python, VM vboxuser.

**Diapositiva 5 - Investigación: Hadoop vs Spark**
Tabla comparativa (3 filas clave): Hadoop disco-lento-batch vs Spark RAM-rápido-iterativo-streaming. Conclusión: complementarios.

**Diapositiva 6 - RDD vs DataFrame**
RDD: bajo nivel, lineage, map/filter/reduceByKey. DataFrame: alto nivel, Catalyst, SQL, 10-50x más rápido. Decisión: DataFrame para EDA, RDD para demostrar requisito guía.

**Diapositiva 7 - Batch: Código y operaciones**
Snippet batch_analisis.py: `df.groupBy("Departamento").agg(avg("PM10"))` + RDD `rdd.filter>45`, `reduceByKey`, `collect/count/take/reduce`. Captura terminal con count 10k y take(3).

**Diapositiva 8 - Batch: Resultados**
- Ranking: Bogotá 52.89 / Valle 51.72 vs 34-36 resto (gráfica 01)
- 33.9% excede PM10, 70.8% PM2.5 (tabla + gráfica 04)
- Top estaciones críticas + evolución anual estable.
Incluir 4 mini-gráficas.

**Diapositiva 9 - Streaming: Anexo 3 corregido**
Antes: doble SparkSession + timestamp bug. Después: Long→to_timestamp + watermark. Demo: dos Putty (producer Sent + consumer windowed_stats) + captura Spark UI Jobs/Stages.
Comando: `spark-submit --packages spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py`

**Diapositiva 10 - Conclusiones y enlaces**
5 conclusiones (ver informe). Enlaces: GitHub, Video 5 min (QR), Referencias. Preguntas.

Notas presentador: Cada diapositiva 30 seg, total 5 min si es video.
Exportar en Canva (plantilla UNAD azul) o Google Slides + PDF.
