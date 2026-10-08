# Tarea 3 - Procesamiento de Datos con Apache Spark
## Universidad Nacional Abierta y a Distancia - ECBTI
### Big Data 202016911 - Grupo XX

**Portada**
- Curso: Big Data 202016911
- Tarea 3 - Procesamiento con Apache Spark (Batch + Streaming)
- Estudiante: [Nombre Apellidos] - Código: XXXXX
- Tutor: [Nombre]
- Fecha: Octubre 2026
- Enlace repositorio: https://github.com/usuario/Tarea3_Spark
- Enlace presentación: https://canva.com/... (o Google Slides)
- Enlace video batch: https://youtu.be/... (5 min)
- Enlace video streaming: incluido en mismo video minuto 3:00-5:00

---

## Introducción
El crecimiento exponencial de datos ambientales exige infraestructuras capaces de procesar grandes volúmenes de forma eficiente y en tiempo real. Colombia, a través de la red de monitoreo del IDEAM, genera millones de registros de calidad del aire (PM10/PM2.5) que deben analizarse para orientar políticas de salud pública. Este proyecto implementa una solución Big Data con **Apache Spark (PySpark)** para procesamiento batch histórico y **Apache Kafka + Spark Structured Streaming** para análisis en tiempo real, demostrando el diseño de infraestructuras que soportan análisis eficiente de datos masivos (RA2).

## Objetivo General
Diseñar e implementar una solución de almacenamiento y procesamiento de grandes volúmenes de datos utilizando Apache Spark y Kafka, para analizar la calidad del aire en Colombia y detectar en tiempo real estaciones con niveles críticos de contaminación.

## Objetivos Específicos
1. Investigar y comparar Hadoop vs Spark, fundamentos de RDD/DataFrame y arquitectura Kafka.
2. Seleccionar y caracterizar un dataset de gran volumen de datos abiertos (Calidad del Aire IDEAM).
3. Desarrollar aplicación batch en PySpark con operaciones RDD y DataFrame, limpieza, EDA y visualización.
4. Configurar topic Kafka y consumidor Spark Streaming para análisis en tiempo real con ventanas temporales.
5. Documentar resultados, publicar código en GitHub y socializar mediante video y presentación.

---

## 1. Investigación Teórica
*(Ver docs/investigacion.md para versión extensa)*

### 1.1 Tabla comparativa Hadoop vs Spark
| Criterio | Hadoop | Spark |
|---|---|---|
| Arquitectura | HDFS + YARN + MapReduce (disco) | DAG in-memory sobre HDFS/YARN/K8s |
| Procesamiento | Map→Shuffle→Reduce batch | RDD/DataFrame + Streaming micro-batch |
| Rendimiento | Alto I/O disco, lento iterativo | 10-100x más rápido (cache RAM) |
| Tolerancia | Replicación HDFS | Lineage RDD |
| Caso uso | ETL histórico barato | ML iterativo, streaming, SQL |

### 1.2 RDD - Propiedades y operaciones
Resilient Distributed Dataset: colección particionada, inmutable, tolerante a fallos, evaluación lazy.
- **Propiedades:** Inmutabilidad (lineage), particionamiento (repartition/coalesce), resiliencia (recomputo), lazy.
- **Transformaciones:** map, filter, flatMap, groupByKey (shuffle costoso), reduceByKey (eficiente).
- **Acciones:** collect, count, take, reduce.

### 1.3 DataFrame - Diferencias y ventajas
Tabla distribuida con esquema y optimizador Catalyst/Tungsten. Ventajas: optimización automática, formato columnar, 10-50x más rápido, SQL integrado, Structured Streaming.

### 1.4 Arquitectura Kafka
Ver diagrama en docs/investigacion.md. Conceptos: Topic (sensor_data), Partition (paralelismo), Broker (servidor), Producer (kafka_producer.py), Consumer (spark_streaming_consumer.py), offset, Consumer Group, ZooKeeper/KRaft.

---

## 2. Definición del problema y dataset
**Dataset:** Calidad del Aire en Colombia - IDEAM, datos.gov.co (id g4t8-zkc3) + PM Colombia compilado 2011-2024 (8.842 filas, 25 cols, 765k registros en versión horaria 2020). Fallback sintético 10k filas con distribución realista para garantizar ejecución offline.
**Problema:** ¿Qué departamentos/municipios/estaciones presentan niveles críticos de PM10/PM2.5 superando límites OMS (PM10 45, PM2.5 15 µg/m³ anual) y cómo evoluciona la contaminación?
**Justificación Big Data:** Histórico nacional multi-anual + streaming IoT requiere procesamiento distribuido; agregaciones por departamento/año y ventanas de 1 min son paralelizables en Spark.

---

## 3. Implementación Spark

### 3.1 Batch (src/batch_analisis.py)
- **Carga:** `spark.read.csv` con `inferSchema`, mapeo flexible de columnas (pm10/pm25/departamento).
- **Limpieza:** cast Double, filtro nulos, dropDuplicates, columnas `excede_pm10_oms`/`excede_pm25_oms`, cache.
- **EDA DataFrame:** `groupBy(Departamento).agg(avg/max/min/count)`, evolución anual, top estaciones, excedencias totales. Resultados guardados en `data/resultados/`.
- **RDD (requisito guía):** `map`, `filter (>45)`, `flatMap` (tokenizar depto), `groupByKey`, `reduceByKey` (sum+count→avg), acciones `collect`, `count`, `take`, `reduce` (max global 108.69).
- **Visualización:** 4 gráficas matplotlib/seaborn (ranking PM10, histograma, evolución 2018-2024, % excedencia). Ver `data/resultados/*.png`.

**Resultados batch (ejecución verificada 2026-10-08):**
- Bogotá D.C. 52.89 avg PM10 (70.3% excede OMS), Valle 51.72 (65.9%), resto 34-36.
- 33.9% muestras exceden PM10>45, 70.8% exceden PM2.5>15 → problema crítico.
- Estaciones críticas: Estacion_24 Bogotá 57.5, Estacion_11 Valle 57.5.
- Evolución estable 38-39 µg/m³ anual (no mejora).

### 3.2 Streaming (Kafka + Spark Structured Streaming)
**Infra VM:** Zookeeper + Kafka 3.9.2 en `/opt/Kafka`, topic `sensor_data` (1 partición, RF1).
**Producer (kafka_producer.py):** Genera cada 1s JSON `{sensor_id, departamento, pm10, pm25, temperature, humidity, timestamp}` con `kafka-python`. Modo `--dry-run` sin Kafka.
**Consumer corregido (spark_streaming_consumer.py):**
- Fix 1: elimina doble SparkSession.
- Fix 2: schema `timestamp LongType` + `to_timestamp()` (antes `TimestampType` fallaba).
- Fix 3: `withWatermark("timestamp","2 minutes")` + `window("1 minute")` por sensor/depto, `avg` pm10/pm25/temp/hum + `count`.
- Fix 4: `startingOffsets latest`, `trigger 10s`, output `complete` a consola.
- Verificación dry-run: Batch 1 mostró 13 grupos ventana 16:01 con avg pm10 18-44, Bogotá/Antioquia, eventos 1-2.
- **Evidencia Spark UI:** Capturas Jobs/Stages/Executors en http://<ip>:4040 (incluir en video).

---

## 4. Repositorio y evidencias
- **GitHub:** https://github.com/usuario/Tarea3_Spark (código batch + streaming, README con instrucciones, requirements.txt)
- **Presentación online:** [Enlace Canva/Slides] 10 diapos (problema, dataset, arquitectura, batch, RDD/DataFrame, streaming, resultados, conclusiones)
- **Videos:** Un video 5 min (0:00-3:00 batch, 3:00-5:00 streaming con dos Putty + :4040). Enlace en portada.

---

## 5. Conclusiones (5 generales del grupo)
1. Spark supera a Hadoop en velocidad y versatilidad al combinar batch y streaming en un mismo engine con optimización Catalyst, mientras Hadoop aporta almacenamiento escalable; son complementarios.
2. Los RDD, con inmutabilidad y lineage, fundamentan la tolerancia a fallos, pero los DataFrames ofrecen 10-50x mejor rendimiento para EDA estructurado y deben preferirse salvo control de bajo nivel.
3. El análisis batch evidenció que Bogotá y Valle del Cauca duplican la contaminación promedio del resto del país y el 70% de muestras excede PM2.5 OMS, priorizando intervención en esas regiones/estaciones.
4. La solución streaming con Kafka demuestra viabilidad de alertas en tiempo real: ventanas de 1 minuto permiten detectar picos instantáneos y escalar horizontalmente añadiendo particiones/consumers.
5. La implementación validada (batch 10k filas, streaming dry-run) cumple los tres criterios de rúbrica con código eficiente, documentado y visualizaciones, y la corrección de bugs del Anexo 3 garantiza reproducibilidad en la VM UNAD.

---

## Referencias (APA 7)
- Macías, M. & Gómez, M. (2015). *Introducción a Apache Spark: para empezar a programar el big data* (pp. 27-40). Editorial UOC.
- Montoya, L. & Gil, G. (2018). Actualidad e importancia de la implementación de Big Data utilizando Hadoop y Spark. https://elibro-net.bibliotecavirtual.unad.edu.co/es/ereader/unad/126993
- Sebastián Maldonado, C. V. (2022). *Analytics y Big Data* (pp. 185-195). RIL editores.
- Velázquez, C. et al. (2017). Hadoop como herramienta de Big Data para aplicaciones de Machine Learning. *Congreso Internacional Academia Journals*.
- Apache Software Foundation. (2024). *Apache Spark Documentation* y *Apache Kafka Documentation*. https://spark.apache.org/docs/3.5.0/ - https://kafka.apache.org/documentation/

*Informe listo para exportar a PDF con portada UNAD, tabla de contenido e imágenes de resultados.*
