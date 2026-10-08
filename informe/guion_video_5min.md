# Guion Video 5 min - Tarea 3 Spark (Monólogo)

**Duración total 5:00 - Grabar en OBS, 1080p, audio claro**

### 0:00-0:30 Introducción (30s)
"Hola, soy [Nombre] del curso Big Data 202016911 Tarea 3. El problema es la contaminación del aire en Colombia: ¿Qué departamentos superan los límites OMS? Usé el dataset oficial del IDEAM en datos.gov.co, con 765 mil registros horarios y un fallback sintético de 10 mil para demo offline. La solución combina Spark batch para histórico y Kafka + Spark Streaming para tiempo real."

### 0:30-3:00 Batch (2m30s) - Mostrar código y ejecución
[Compartir pantalla: VS Code con batch_analisis.py + terminal]
"En batch_analisis.py implementé DataFrame y RDD. Primero cargo el CSV con inferSchema y mapeo flexible de columnas. Limpieza: cast, nulos, excedencias OMS. Con DataFrame hago groupBy departamento para promedio PM10, evolución anual y top estaciones. Con RDD demuestro map, filter mayor a 45, flatMap, groupByKey y reduceByKey, más las acciones collect, count, take y reduce."
[Ejecutar: python3 src/batch_analisis.py - mostrar salida count 10000, avg Bogotá 52.89, max 108.69]
"Los resultados se guardan en data/resultados y generan cuatro visualizaciones: ranking por departamento, histograma PM10, evolución anual y tasa de excedencia. Como ven, Bogotá y Valle del Cauca con 52 PM10 superan 70% el límite OMS, mientras el resto está en 34. El 33% excede PM10 y 70% PM2.5."
[Mostrar 4 PNGs en carpeta]

### 3:00-5:00 Streaming (2m) - Dos terminales + Spark UI
[Cambiar a VM o local: dos terminales Putty]
"Para streaming seguí el Anexo 3 pero corregí tres bugs: eliminé doble SparkSession, cambié timestamp a Long con to_timestamp, y añadí watermark de 2 minutos con ventana de 1 minuto."
[Terminal 1: python3 src/kafka_producer.py --once - muestra Sent: sensor_id...]
[Terminal 2: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 src/spark_streaming_consumer.py --dry-run]
"El productor envía JSON cada segundo al topic sensor_data y el consumidor Structured Streaming calcula promedio de PM10, PM2.5, temperatura y humedad por ventana."
[Mostrar consola Batch 1 con window 16:01 y avg]
"Finalmente en el navegador abro el Spark UI en puerto 4040 para ver Jobs, Stages y Executors, como pide el anexo."
[Mostrar http://localhost:4040 o http://192.168.1.7:4040]
"Código en GitHub, informe y presentación en la descripción. Gracias."

**Tips grabación:**
- Usar `spark-submit --master local[*]` para batch local, y `--dry-run` para streaming sin Kafka si la VM falla.
- Incluir subtítulos automáticos de YouTube.
- Subir como No listado y copiar enlace al informe.

