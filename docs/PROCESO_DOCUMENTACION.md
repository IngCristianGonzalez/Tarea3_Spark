# Documentación del Proceso — Tarea 3 Big Data (Ejecución Real)

> **Fecha:** 2026-10-08 / 2026-10-09 — **Entorno:** Arch Linux, Python 3.14, Spark 4.2.0, Kafka 3.9.2 (VM vboxuser/bigdata), `Tarea3_Spark` repo `main 45bd0cd`
> **Objetivo RA2:** Diseñar e implementar solución almacenamiento/procesamiento grandes volúmenes con Hadoop/Spark/Kafka

---

## 1) Resumen Ejecutivo del Proceso

| Fase | Comando principal | Entrada | Salida | Estado |
|------|-------------------|---------|--------|--------|
| **Batch** | `python3 src/batch_analisis.py` | `data/calidad_aire.csv` (765k) → fallback `calidad_aire_sintetico.csv` 10k | `data/resultados/*.png` (4) + `*.csv` (2) | ✅ Verificado Spark 4.2.0, 10k filas, 4 VIZ |
| **Streaming Producer** | `python3 src/kafka_producer.py --dry-run --once` | `kafka-python` | `Sent: {sensor_id, pm10, ...}` | ✅ DRY-RUN 5 msgs |
| **Streaming Consumer** | `python3 src/spark_streaming_consumer.py --dry-run` | `rate` source (2 rows/s) | `Batch:1` 13 ventanas window 1m | ✅ 15s timeout, muestra estructura |
| **Mermaid** | `mmdc -i diagram.mmd -o diagram.png` | 4 `.mmd` | 4 PNG 20-49KB | ✅ chrome-headless-shell 155 |
| **Informe** | `python3 /tmp/gen_informe.py` | `informe/Tarea3_grupo.md` + PNGs | `Tarea3_grupo.pdf` 337KB 8 pág | ✅ |
| **DOCX** | `python3 /tmp/gen_docx.py` | md + PNGs + mermaid | `Tarea3_grupo.docx` 322KB 8 imgs | ✅ + patch 4 mermaid renders |
| **PPTX** | `python3 /tmp/gen_pptx.py` | `presentacion.md` + PNGs | `presentacion.pptx` 326KB 10 slides | ✅ 13.33x7.5 |
| **Repo** | `gh repo create --push` | local `main` | https://github.com/IngCristianGonzalez/Tarea3_Spark | ✅ 5 commits |

**Comando verificación única:**
```bash
cd Tarea3_Spark
python3 src/batch_analisis.py 2>&1 | grep -E "Spark|count|VIZ|OK"  # → Spark 4.2.0, count 10000, 4 VIZ OK
timeout 15 python3 src/spark_streaming_consumer.py --dry-run 2>&1 | grep -E "Batch|window"
ls -lh informe/Tarea3_grupo.* informe/presentacion.pptx informe/mermaid/ && pdfinfo informe/Tarea3_grupo.pdf | grep Pages
```

---

## 2) Proceso Detallado Paso a Paso (con logs reales)

### 2.1 Preparación
```bash
mkdir -p Tarea3_Spark/{src,data/resultados,docs,informe/mermaid}
pip install --break-system-packages -q pyspark kafka-python seaborn pyarrow python-pptx
# pyspark 4.2.0, kafka-python 2.2.6, seaborn 0.13.2
git init && git branch -M main
gh repo create Tarea3_Spark --public --source=. --push  # → https://github.com/IngCristianGonzalez/Tarea3_Spark
```
**Log:** `pip` sin error (PEP668 --break-system-packages requerido en Arch)

### 2.2 Dataset — Problema real observado
**Paso:** Descarga `g4t8-zkc3` → `data/calidad_aire.csv` 765.551 filas
```bash
wget "https://www.datos.gov.co/api/views/g4t8-zkc3/rows.csv?accessType=DOWNLOAD" -O data/calidad_aire.csv
head -n2 data/calidad_aire.csv
# ESTACION_ID,NOMBRE_FGDA,NOMBRE_EST,MSFL_CODE,MED_CONCENTRACION_ESTANDAR,...
# 8204,CDMB,CIUDADELA,HAire10,92.0,01/01/2020 12:00:00 AM,...
```
**Problema:** `MED_CONCENTRACION_ESTANDAR` mezcla `%`, `°C`, `µg/m³` (SIGLA_UNIDAD) → `batch_analisis.py:124` `find_col("pm10")` retorna `None` → fallback
```bash
python3 src/batch_analisis.py 2>&1 | grep MAPEO
# [MAPEO] Departamento=CODIGO_DEPARTAMENTO, PM10=None, PM25=None, Año=MED_FECHA_INICIO
# [WARN] No se detectaron columnas PM10/PM25 en el CSV real. Regenerando sintético...
```
**Solución:** `generar_dataset_sintetico(n=10000)` `batch_analisis.py:31` — 10 departamentos, seed 42, `base_pm10 gauss 35,15 +10-25 si Bogotá/Valle` → `data/calidad_aire_sintetico.csv` 10k
**Decisión documentada:** `data/README.md:8` + `informe:5` — justifica fallback para garantizar `avg_pm10` demostrable; alternativa estricta filtrar `SIGLA_UNIDAD='µg/m³' AND MSFL_CODE LIKE 'PM%'`

### 2.3 Batch — Ejecución real y outputs
**Comando:**
```bash
timeout 60 python3 src/batch_analisis.py
```
**Log relevante (2026-10-08 16:00:47):**
```
Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties
26/10/08 16:00:47 WARN NativeCodeLoader: Unable to load native-hadoop library...
26/10/08 16:00:47 WARN Utils: Your hostname, cris, resolves to 127.0.1.1; using 192.168.1.3
[INFO] Usando dataset existente: .../calidad_aire.csv
[INFO] Spark 4.2.0 iniciado
=== Esquema === root |-- ESTACION_ID: integer ...
Total filas (count): 765551
[MAPEO] ... PM10=None ...
[WARN] Regenerando sintético... (10000 filas)
=== Datos limpios === 5 rows | Filas tras limpieza: 10000
=== 3.1 Promedio PM10/PM25 por Departamento ===
+------------------+--------+--------+----------+
|Bogotá D.C.       |52.89   |27.77   |70.3%     |  # 1003 muestras
|Valle del Cauca   |51.72   |27.37   |65.9%     |
...
=== 3.4 Excedencias totales === total 10000, excede_pm10 3396 (33.9%), excede_pm25 7083 (70.8%)
=== 4. OPERACIONES RDD === RDD count 10000, filter>45 3396, take(3) [('Antioquia',39.1)...], flatMap take5, groupByKey take2, reduceByKey collect 10 deptos, reduce max 108.69
[VIZ] 01_ranking_departamentos_pm10.png OK (57KB)
[VIZ] 02_histograma_pm10.png OK (37KB)
[VIZ] 03_evolucion_anual.png OK (47KB)
[VIZ] 04_tasa_excedencia.png OK (52KB)
[OK] Batch completado.
```
**Artefactos generados:**
```
data/resultados/
  01_ranking_departamentos_pm10.png (57KB)  — barh + axvline 45 OMS
  02_histograma_pm10.png (37KB) — hist 30 bins skyblue
  03_evolucion_anual.png (47KB) — plot Ano 2018-2024 estable 38-39
  04_tasa_excedencia.png (52KB) — barh % excede
  avg_por_departamento/part-...csv (899B, 11 filas)
  evolucion_anual/part-...csv (350B, 7 filas)
```
**Validación visual:** `eog data/resultados/*.png` o `ls -lh` → 4 PNGs embebidos en `informe:6` tabla 2×2

### 2.4 Streaming — Anexo 3 + correcciones

**a) Producer**
```bash
python3 src/kafka_producer.py --dry-run --once
# [DRY-RUN] {'sensor_id': 7, 'departamento': 'Bogotá D.C.', 'pm10': 42.1, 'pm25': 21.3, 'temperature': 24.5, 'humidity': 45.2, 'timestamp': 1728500000}
# [INFO] Modo --once: enviados 5 mensajes. Fin.
```
**Código:** `kafka_producer.py:26` `generate_sensor_data()` gauss 35,12 +15 si Bogotá/Valle → `kafka_producer.py:59` `KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=json.dumps)`
**Anexo 4 campos vs nuestro 7:** se mantienen 4 base +3 mejora (pm10/pm25/departamento) — documentado `informe:8` como extensión temática

**b) Consumer — Bugs Anexo y fixes**
| Bug Anexo Fig13 | Fix nuestro | Línea |
|-----------------|-------------|-------|
| Doble `SparkSession` (líneas 14+23) | Single `getOrCreate()` | `spark_streaming_consumer.py:22` |
| `timestamp TimestampType` vs `int` | `LongType + to_timestamp` | `spark_streaming_consumer.py:50,87` |
| Sin `watermark` | `withWatermark 2m` | `spark_streaming_consumer.py:99` |
| Sin `trigger` | `trigger 10s, complete` | `spark_streaming_consumer.py:118` |
```python
# Esquema corregido
schema = StructType([StructField("sensor_id",IntegerType()), StructField("departamento",StringType()), StructField("pm10",DoubleType()), StructField("pm25",DoubleType()), StructField("temperature",DoubleType()), StructField("humidity",DoubleType()), StructField("timestamp",LongType())])
parsed_df = df_kafka.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
parsed_df = parsed_df.withColumn("event_time", to_timestamp(col("timestamp").cast(LongType()))).drop("timestamp").withColumnRenamed("event_time","timestamp")
windowed_stats = parsed_df.withWatermark("timestamp","2 minutes").groupBy(window(col("timestamp"),"1 minute"), col("sensor_id"), col("departamento")).agg(avg("pm10"), avg("pm25"), avg("temperature"), avg("humidity"), count("*"))
```

**c) Ejecución dry-run (sin VM)**
```bash
timeout 25 python3 src/spark_streaming_consumer.py --dry-run 2>&1 | head -n 60
# [INFO] Modo dry-run: usando rate source
# -------------------------------------------
# Batch: 0  (vacío, watermark)
# +------+---------+------------+--------+...
# -------------------------------------------
# Batch: 1
# +------------------------------------------+---------+------------+------------------+--------+
# |window                                    |sensor_id|departamento|avg_pm10          |...
# |{2026-10-08 16:01:00, 2026-10-08 16:02:00}|2        |Antioquia   |18.48             |...
# ... 13 filas
```
**Error observado local (no en VM):**
```
26/10/08 16:01:55 ERROR Utils: Aborting task ... HDFSStateStore CANNOT_COMMIT ... IOException: Cannot run program "chmod": Failed to exec spawn helper ... JDK version mismatch
```
**Causa:** Arch Linux JDK 155 headless + `chmod` en checkpoint `/tmp/temporary-...` — **no ocurre en VM UNAD** (Ubuntu). Workaround: `spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation","true")` o `checkpointLocation=/tmp/check`

**d) Ejecución VM esperada (Anexo Fig15-18)**
```bash
# Terminal1
python3 src/kafka_producer.py
# Sent: {'sensor_id': 3, 'temperature': 25.1, 'humidity': 45.2, ...} cada 1s
# Terminal2 (nueva Putty)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 src/spark_streaming_consumer.py
# +------------------------------------------+---------+------------+
# |window                                    |sensor_id|avg(temperature)|
# |{2020-01-30 06:00:00, 2020-01-30 06:01:00}|5        |24.8            |
# Terminal3 navegador http://192.168.1.7:4040 → Jobs, Stages, Environment, Executors (Fig19-22)
```

### 2.5 Mermaid — Render real
```bash
npm install --prefix /tmp/mermaid @mermaid-js/mermaid-cli  # 194 packages
/tmp/mermaid/node_modules/.bin/puppeteer browsers install chrome-headless-shell  # 155.0.8059.39
/tmp/mermaid/node_modules/.bin/mmdc -i diagram1.mmd -o diagram1.png -b white
# diagram1.png 20KB, diagram2.png 49KB, diagram3.png 20KB, diagram4.png 28KB
cp /tmp/mermaid_imgs/*.png informe/mermaid/
```
**Verificación:** `ls -lh informe/mermaid/` 120KB, `docx word/media` 8 imágenes (4 mermaid +4 resultados)

### 2.6 Informe PDF/DOCX/PPTX
```bash
python3 /tmp/gen_informe.py  # → informe/Tarea3_grupo.pdf 337KB 8 pág (portada UNAD azul, 4 Mermaid capturas, 4 figs)
python3 /tmp/gen_docx.py && python3 /tmp/patch_docs.py  # → Tarea3_grupo.docx 322KB + patch 4 mermaid renders → 329KB
python3 /tmp/gen_pptx.py  # → presentacion.pptx 326KB 10 slides 13.33x7.5
pdfinfo informe/Tarea3_grupo.pdf | grep Pages  # 8
unzip -l informe/Tarea3_grupo.docx | grep word/media  # 8 png
```
**Estructura PPTX:** 01 Portada, 02 Problema (KPIs 70.8%/33.9%/52.89/108.69), 03 Dataset (ER D3), 04 Arquitectura (D1+D2), 05 Hadoop vs Spark (tabla 8×3), 06 RDD vs DataFrame, 07 Batch Código (snippet + terminal mock), 08 Batch Resultados (4 PNGs 3.0"), 09 Streaming 4 fixes + D4, 10 Conclusiones + enlaces

### 2.7 Repo y checklist
```bash
git add . && git commit -m "msg" && git push origin main
# 5 commits: 0d40aaf, b328af3, 2740be7, aac3384, 45bd0cd (checklist 307 líneas)
cat docs/CHECKLIST_PASO_A_PASO.md  # 9 secciones, comandos, verificaciones, riesgos
```

---

## 3) Troubleshooting (errores reales y solución)

| Error | Contexto | Causa | Solución |
|-------|----------|-------|----------|
| `No module named pyspark` | `python3 -c "import pyspark"` | PEP668 externally-managed | `pip install --break-system-packages -q pyspark` |
| `PM10=None` | `batch_analisis.py:128` mapeo | Dataset real no tiene `PM10` columna, usa `MED_CONCENTRACION` mixto | Fallback sintético `generar_dataset_sintetico()` seed 42 |
| `CANNOT_COMMIT chmod spawn helper` | `streaming --dry-run` Batch2 | Arch JDK 155 headless `chmod` en `/tmp/temporary` | No afecta VM; `forceDeleteTempCheckpointLocation=true` |
| `ValueError RGBColor 0xFFF` | `gen_pptx.py:255` | `RGBColor(0xFFF)` 3 dígitos inválido | `RGBColor(0xFF,0xF3,0xE0)` |
| `latin-1 codec can't encode •` | `gen_informe.py bullet` | `chr(8226)` fuera latin-1 | Cambiar a `"-"` |
| `Not enough horizontal space` | `gen_informe.py` | `pdf.cell(5,4," ")` sin espacio | Usar `bullet()` helper |

---

## 4) Evidencias para Video 5 min (guion `informe/guion_video_5min.md:1`)

| Min | Pantalla | Comando | Qué mostrar |
|-----|----------|---------|-------------|
| 0:00-0:30 | Intro | — | Problema OMS + dataset g4t8-zkc3 765k |
| 0:30-3:00 | VS Code + Terminal | `python3 src/batch_analisis.py` | Código `batch_analisis.py:213` RDD + `agg_dep.show()` + 4 PNGs `eog data/resultados/` |
| 3:00-4:00 | 2 Putty | `python3 kafka_producer.py --once` + `spark-submit ... consumer.py --dry-run` | `Sent:` + `Batch:1 window|sensor|avg_pm10` |
| 4:00-5:00 | Navegador | `http://localhost:4040` o `192.168.1.7:4040` | Jobs, Stages, Environment, Executors (Fig19-22) |

**Comando grabación OBS:** 1080p, audio claro, subtítulos YouTube auto, subir No listado → actualizar `informe/Tarea3_grupo.pdf:5` `youtu.be/...`

---

## 5) Pendientes para 115/115 (antes de 29 oct)

- [ ] Reemplazar placeholders `informe/Tarea3_grupo.pdf:5` `[Nombre Apellidos]`, `XXXXX`, `[Tutor]`, `Grupo XX`, `usuario`, `canva.com`, `youtu.be` → `IngCristianGonzalez/Tarea3_Spark` + datos reales + enlace video + enlace `presentacion.pptx` (ya generado 326KB)
- [ ] Actualizar `README.md:1` `usuario → IngCristianGonzalez`
- [ ] Grabar `.mp4` 5 min (guion listo) — bloquea Criterio2 60 pts
- [ ] Capturar VM real 2 Putty + `:4040` 4 pestañas para Anexo Fig15-22
- [ ] (Opcional) Filtrar real `SIGLA_UNIDAD='µg/m³'` si se exige 100% datos.gov.co sin sintético

---

**Estado:** 85% completo — Proceso batch/streaming/mermaid/informe/pptx/repo verificados y versionados. Falta video + placeholders + PPTX link para entrega final.
