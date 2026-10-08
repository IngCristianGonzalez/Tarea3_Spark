# Foro Tarea 3 - 2 aportes listos para pegar

## Aporte 1 - Selección dataset y problema (Semana 1)
Hola compañeros y tutor,

Comparto mi avance para la Tarea 3:

**Dataset seleccionado:** Calidad del Aire en Colombia - IDEAM (datos.gov.co, id g4t8-zkc3). Contiene registros horarios 2020 (765k filas) y compilado PM 2011-2024 (8.8k). Elegí este dataset porque es oficial, de gran volumen, y conecta directamente con el streaming de sensores del Anexo 3 (misma temática). Como fallback generé un sintético de 10k filas con distribución realista (Bogotá/Valle 51-52 PM10 vs 34-36 resto) para garantizar ejecución offline.

**Problema definido:** ¿Qué departamentos/municipios/estaciones superan los límites OMS (PM10>45 y PM2.5>15 µg/m³ anual) y requieren priorización en políticas de salud pública? El análisis batch permite identificar regiones críticas y el streaming alertar picos en tiempo real.

**Avance técnico:** Ya implementé `batch_analisis.py` con operaciones RDD (map, filter, flatMap, groupByKey, reduceByKey + collect/count/take/reduce) y DataFrame (groupBy, agg, window) + 4 visualizaciones. Resultados preliminares: Bogotá 52.89 avg (70% excede), 33.9% PM10 excedido. Repositorio: https://github.com/usuario/Tarea3_Spark

Quedo atento a comentarios y a coordinar el informe grupal.

Referencia: IDEAM - datos.gov.co. Saludos.

---

## Aporte 2 - Solución streaming y dificultades (Semana 2)
Hola de nuevo,

Actualizo mi componente de **Spark Streaming + Kafka (Anexo 3)**:

Implementé `kafka_producer.py` (genera pm10/pm25/temp/humidity cada 1s al topic sensor_data) y `spark_streaming_consumer.py` corregido. Detecté 3 bugs del instructivo: doble SparkSession, timestamp mal tipado (usaba TimestampType directo con epoch int) y falta de watermark. Los corregí usando `LongType` + `to_timestamp()` y `withWatermark("timestamp","2 minutes").groupBy(window("timestamp","1 minute"), sensor_id)`.

**Dificultades y soluciones:**
- El dataset real tiene columnas heterogéneas (MED_CONCENTRACION con unidades % y °C), por lo que implementé mapeo flexible y fallback sintético si no detecta PM10/PM25.
- En streaming dry-run local tuve error de checkpoint `chmod` por JDK en Arch; lo resolví usando `trigger 10s` y checkpoint temporal, verificado con Batch 1 que muestra ventanas 16:01 con promedios por sensor/depto.

**Evidencias:** Ejecución batch verificada (10k filas, 4 PNGs en data/resultados) y streaming dry-run con Spark UI en :4040 (Jobs/Stages). Adjuntaré capturas al informe.

Quedo atento para unificar conclusiones y subir el Tarea3_grupo.pdf.

Saludos.
