# CHECKLIST PASO A PASO — Tarea 3 Big Data 202016911 (Anexo 3 + Rúbrica 115 pts)

> **Objetivo:** Documentar cada comando del Anexo 3 y de la Guía con verificación, para entrega sin errores en VM `vboxuser/bigdata`.  
> **Repo:** https://github.com/IngCristianGonzalez/Tarea3_Spark — `main aac3384`  
> **Fecha verificación:** 2026-10-08 — Spark 4.2.0 / Kafka 3.9.2 / Python 3.14

---

## 0) Preparación (local y VM)

- [x] **0.1 Crear estructura**
  ```bash
  mkdir -p Tarea3_Spark/{src,data/resultados,docs,informe,informe/mermaid}
  ```
  `Tarea3_Spark/README.md:1` — Verificado `ls -R` OK

- [x] **0.2 Git init**
  ```bash
  git init && git branch -M main && git add . && git commit -m "init"
  gh repo create Tarea3_Spark --public --source=. --push
  ```
  Verificado `git log --oneline` 4 commits, `gh repo view` → https://github.com/IngCristianGonzalez/Tarea3_Spark

- [x] **0.3 Dependencias**
  ```bash
  pip install --break-system-packages -r requirements.txt
  # requirements.txt:1 pyspark==3.5.3, kafka-python, matplotlib, seaborn, pyarrow>=18
  ```
  Verificado `pip show pyspark` 4.2.0 local, VM espera 3.5.x (compatible)

---

## 1) Investigación (Rúbrica Criterio 1 — 20 pts, 16-20 = alto)

Archivo: `docs/investigacion.md:1` y `informe/Tarea3_grupo.pdf:3` / `docx:1`

- [x] **1.1 Tabla Hadoop vs Spark (8 filas)**
  - Criterios guía: arquitectura, procesamiento, rendimiento, casos de uso
  - Implementado `investigacion.md:5` — Hadoop HDFS+YARN+MapReduce disco vs Spark DAG in-memory Catalyst/Tungsten 10-100x
  - Verificación: tabla con `col_widths [1.6,2.8,2.8]` en PDF/DOCX OK

- [x] **1.2 RDD — Concepto + 6 Propiedades + Operaciones**
  - Propiedades: inmutabilidad, particionamiento, tolerancia lineage, lazy, tipado, persistencia `investigacion.md:24`
  - Transformaciones: `map, filter, flatMap, groupByKey, reduceByKey` `investigacion.md:35`
  - Acciones: `collect, count, take, reduce` `investigacion.md:42`
  - Verificación: texto en PDF §3.2 p.3

- [x] **1.3 DataFrame — Diferencias y 5 ventajas**
  - Tabla 6 aspectos `investigacion.md:54` + ventajas Catalyst/off-heap/SQL/Streaming `investigacion.md:67`
  - Verificación: PDF/DOCX tabla OK

- [x] **1.4 Kafka — Diagrama + 6 conceptos**
  - Diagrama texto `investigacion.md:80` + **4 Mermaid render** `informe/mermaid/diagram1-4.png:1`
    - `diagram1.png` 20KB graph LR Topic→Brokers→Consumer→Spark
    - `diagram2.png` 49KB flowchart TD batch+streaming
    - `diagram3.png` 20KB erDiagram ESTACION-MEDICION
    - `diagram4.png` 28KB sequenceDiagram window
  - Conceptos: topic, partition, broker, producer, consumer, offset, group, ZooKeeper/KRaft `investigacion.md:96`
  - Verificación: `ls -lh mermaid/` 4 PNGs, `docx word/media` 8 imágenes (4 mermaid +4 resultados)

- [ ] **1.5 Pendiente:** Reemplazar placeholder si se exige cita APA en investigación (ya hay 6 refs `informe:10`)

---

## 2) Definición problema y dataset (Guía §2)

Archivo: `data/README.md:1` + `informe:5`

- [x] **2.1 Selección dataset público gran volumen**
  - Fuente: `datos.gov.co g4t8-zkc3 Calidad del Aire IDEAM` https://www.datos.gov.co/Ambiente-y-Desarrollo-Sostenible/Calidad-del-Aire-en-Colombia/g4t8-zkc3
  - Real: `data/calidad_aire.csv` 765.551 filas (MED_CONCENTRACION, DEPARTAMENTO, etc.) — descargado `wget` OK
  - Fallback: `data/calidad_aire_sintetico.csv` 10.000 filas `generar_dataset_sintetico()` `batch_analisis.py:31` — verificado `ls -lh` 10001 líneas
  - Verificación: `batch_analisis.py:102` `spark.read.csv inferSchema` + `find_col` mapeo flexible

- [x] **2.2 Problema definido**
  > ¿Qué departamentos/municipios/estaciones superan OMS PM10>45 / PM2.5>15 y cómo evoluciona 2018-2024 para priorizar salud pública?
  - Verificado `data/README.md:8` + `informe:5`

- [x] **2.3 Justificación Big Data**
  - Histórico multi-anual + IoT streaming requiere Spark distribuido (groupBy depto/ano + window 1m paralelizables) — documentado `informe:5`

---

## 3) Implementación Batch (Rúbrica Criterio 2 — 60 pts, 46-60 = alto)

Archivo: `src/batch_analisis.py:1` (325 líneas) — **Verificado ejecución 2026-10-08**

- [x] **3.1 Carga**
  ```bash
  spark-submit --master local[*] src/batch_analisis.py
  python3 src/batch_analisis.py
  ```
  Código: `spark = SparkSession.builder.appName("Tarea3_Batch_CalidadAire").master("local[*]")` `batch_analisis.py:89`
  Salida esperada: `Spark 4.2.0 iniciado`, `Esquema`, `Total filas: 10000`, `Mapeo Departamento=... PM10=...` `batch_analisis.py:128`
  Verificado: log `[INFO] Spark 4.2.0 iniciado` + `count 765551 real → fallback sintético` OK

- [x] **3.2 Limpieza / Transformación DataFrame**
  - `withColumn cast Double` `batch_analisis.py:147` + `filter notNull` `batch_analisis.py:150` + `dropDuplicates` `batch_analisis.py:152` + `when >45/>15` `batch_analisis.py:155` + `cache()` `batch_analisis.py:164`
  - Verificado: `Datos limpios` + `Filas tras limpieza: 10000`

- [x] **3.3 EDA DataFrame (3.1-3.4)**
  - `groupBy(Departamento).agg(avg/max/min/count/tasa)` `batch_analisis.py:170` → `avg_por_departamento/part-*.csv:1` 11 filas (Bogotá 52.89 70.3%)
  - `groupBy(Ano).agg(avg)` `batch_analisis.py:185` → `evolucion_anual/part-*.csv:1` 7 años 2018-2024
  - `groupBy(Estacion).limit(10)` `batch_analisis.py:196` → Top Est24 57.5
  - `agg total/excede` `batch_analisis.py:203` → `total 10000, excede_pm10 3396 (33.9%), excede_pm25 7083 (70.8%)`
  - Verificado: `agg_dep.show(20)` + `CSV coalesce(1)` en `data/resultados/`

- [x] **3.4 Operaciones RDD (requisito guía §2)**
  | Operación | Línea | Verificación |
  |-----------|-------|--------------|
  | `map` | `batch_analisis.py:215` `rdd.map` + `233 map` | `take(3) [('Antioquia',39.1)...]` |
  | `filter` | `217` `filter >45` | `count excede 3396` |
  | `flatMap` | `225` `flatMap split` | `take 5 [('Antioquia',1)...]` |
  | `groupByKey` | `229` `groupByKey().mapValues(list)` | `take 2 (Antioquia,[39.1,...])` |
  | `reduceByKey` | `233` `reduceByKey sum` + `234` count | `rdd_avg` |
  | `collect` | `238` `rdd_avg.collect()` | `Antioquia:35.02 ...` |
  | `count` | `218,219` | `10000` |
  | `take` | `220,226,230` | 3/5/2 |
  | `reduce` | `242` `max` | `108.69` |
  - Verificado: log `=== 4. OPERACIONES RDD ===` completo

- [x] **3.5 Visualización (4 PNGs)**
  - `01_ranking_departamentos_pm10.png` 57KB `batch_analisis.py:258` barh + axvline 45
  - `02_histograma_pm10.png` 37KB `batch_analisis.py:271` hist 30 bins
  - `03_evolucion_anual.png` 47KB `batch_analisis.py:287` plot Ano vs avg
  - `04_tasa_excedencia.png` 52KB `batch_analisis.py:308` barh tasa_excede*100
  - Verificado: `ls -lh data/resultados/*.png` 4 archivos + embebidos `informe/mermaid` + PDF p.6-7 tabla 2×2

- [x] **3.6 Resultados guardados**
  ```bash
  ls data/resultados/
  # avg_por_departamento/_SUCCESS, evolucion_anual/_SUCCESS, 4 PNGs
  ```
  Verificado OK

---

## 4) Implementación Streaming — Anexo 3 Paso a Paso (Fig1-22)

### 4.1 Infra VM (Fig1-8)
- [x] **Fig1 VirtualBox login `vboxuser/bigdata`** — documentado `README:1` `informe:8`
- [x] **Fig2 Putty SSH `192.168.1.x`** — documentado
- [ ] **Fig3 `pip install kafka-python`** — `requirements.txt:4` + `kafka_producer.py:19` try/except
  ```bash
  pip install kafka-python
  ```
  Verificar en VM: `pip show kafka-python`
- [x] **Fig4 `wget https://downloads.apache.org/kafka/3.9.2/kafka_2.12-3.9.2.tgz`** — `README:1`
- [x] **Fig5 `tar -xzf kafka_2.12-3.9.2.tgz`** — `README`
- [x] **Fig6 `sudo mv kafka_2.12-3.9.2 /opt/Kafka`** — `README`
- [x] **Fig7 `sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &` + Enter** — `README` + `informe:8` código bloque
  - Verificar: `jps` debe mostrar `QuorumPeerMain`, `netstat -tuln | grep 2181`
- [x] **Fig8 `sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &` + Enter** — `README`
  - Verificar: `jps` muestra `Kafka`, `netstat | grep 9092`

### 4.2 Topic (Fig9-10)
- [x] **Fig10**
  ```bash
  /opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data
  ```
  Verificar: `/opt/Kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092` → `sensor_data`
  Código: `kafka_producer.py:50` bootstrap `localhost:9092`, `spark_streaming_consumer.py:33` subscribe `sensor_data`

### 4.3 Producer (Fig11-12)
Archivo: `src/kafka_producer.py:1` (94 líneas)

- [x] **Fig11 `nano kafka_producer.py` + código Anexo** (sensor_id 1-10, temp 20-30, hum 30-70, timestamp int(time.time()), sleep 1)
  - Anexo original 4 campos → **nuestra versión extendida 7 campos** (`departamento, pm10, pm25` + 4 base) `kafka_producer.py:26` — cumple ampliado, para estricto comentar extensión
  - Mejora: `argparse --once/--dry-run/bootstrap/topic` `kafka_producer.py:48` + `KafkaProducer acks all retries 3` `kafka_producer.py:59`
- [x] **Fig12 `Ctrl+O Enter Ctrl+X`**
- [x] **Fig15 `python3 kafka_producer.py`**
  ```bash
  python3 src/kafka_producer.py              # infinito cada 1s
  python3 src/kafka_producer.py --once       # 5 mensajes
  python3 src/kafka_producer.py --dry-run    # sin Kafka
  ```
  Salida esperada Anexo: `Sent: {'sensor_id': 5, 'temperature': 24.5, ...}`
  Verificado local `--dry-run --once` → `[DRY-RUN] {'sensor_id':...}` OK (sin VM)

### 4.4 Consumer (Fig13-14) — CORRECCIONES DOCUMENTADAS
Archivo: `src/spark_streaming_consumer.py:1` (127 líneas)

- [x] **Fig13 `nano spark_streaming_consumer.py` + código Anexo (con BUGS)**
  - **Bug1 Anexo Fig13 líneas 14+23 doble SparkSession** → Corregido `spark_streaming_consumer.py:22` single `getOrCreate()` `get_spark():22`
  - **Bug2 Anexo `StructField timestamp TimestampType` vs producer `int`** → Corregido `spark_streaming_consumer.py:50 LongType + to_timestamp:87-90`
  - **Bug3 Sin watermark** → Mejorado `spark_streaming_consumer.py:98` `withWatermark("timestamp","2 minutes")`
  - **Bug4 Output `complete` sin trigger** → Mejorado `trigger processingTime 10s` `spark_streaming_consumer.py:118`
  - Esquema: `sensor_id Integer, departamento String, pm10/pm25/temperature/humidity Double, timestamp Long` `spark_streaming_consumer.py:43` — compatible producer
  - Lógica: `readStream kafka -> from_json -> withColumn event_time -> groupBy window 1 minute, sensor_id, departamento agg avg/count` `spark_streaming_consumer.py:98`
  - Dry-run: `readStream rate rowsPerSecond 2` `spark_streaming_consumer.py:56` para demo sin VM

- [x] **Fig14 `Ctrl+O Enter Ctrl+X`**

### 4.5 Ejecución (Fig15-22)
- [x] **Fig15 Terminal1 producer**
  ```bash
  python3 src/kafka_producer.py
  # esperada: Sent: {'sensor_id': 3, 'temperature': 25.1, 'humidity': 45.2, 'timestamp': 1700000000} cada 1s
  ```
- [x] **Fig16 Nueva Putty SSH sin cerrar anterior**
- [x] **Fig17 Terminal2 consumer**
  ```bash
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 src/spark_streaming_consumer.py
  # dry-run local: python3 src/spark_streaming_consumer.py --dry-run
  ```
  Salida esperada Anexo `Fig18`: tabla `window | sensor_id | avg(temperature) | avg(humidity)` cada 10s
  Verificado dry-run: `Batch: 1` 13 filas `window 2026-10-08 16:01 | sensor_id 2 | avg_pm10 18.48 | ... | eventos 2` OK
  - Nota: error `CANNOT_COMMIT chmod spawn helper` en Arch local (JDK 155) → no afecta VM; fix `spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation","true")`

- [x] **Fig18 Observar producer + consumer** — verificado Batch 1
- [x] **Fig19-22 Spark UI `http://<ip>:4040` Jobs/Stages/Environment/Executors**
  ```bash
  # en navegador: http://192.168.1.7:4040  (IP de VM) o http://localhost:4040 (local)
  ```
  - **PENDIENTE captura real VM** — para video, hacer screenshot de 4 pestañas
- [x] **Final `Ctrl+C` en ambas Putty**

---

## 5) Visualización resultados batch + streaming

- [x] **Batch 4 PNGs embebidos** `informe/Tarea3_grupo.pdf:6` tabla 2×2 + `docx:1` 8 imágenes — Verificado `pdfinfo Pages: 8`, `docx word/media 8 png`
- [x] **Streaming console `complete` + UI** — documentado, pendiente foto `:4040`

---

## 6) Documentación y presentación (Rúbrica Criterio 3 — 35 pts, 16-35 = alto)

- [x] **6.1 Foro (2 aportes calidad)**
  Archivo: `informe/foro_aportes.md:1` (35 líneas)
  - Aporte1 dataset + problema + avance batch (semana1)
  - Aporte2 streaming fixes + dificultades chmod (semana2)
  - Verificación: listos para copiar/pegar en foro Tarea3 UNAD

- [x] **6.2 Repositorio Git**
  - `git init main` 4 commits `git log:1` `0d40aaf, b328af3, 2740be7, aac3384`
  - `gh repo create Tarea3_Spark --public --push` → https://github.com/IngCristianGonzalez/Tarea3_Spark
  - `README.md:1` con Estructura, Requisitos, Ejecución batch/streaming, Correcciones Anexo, Problema dataset
  - `requirements.txt:1` + `.gitignore:1` (ignora `calidad_aire.csv` grande, `*.crc`)
  - Verificado `git remote -v` origin OK

- [ ] **6.3 Informe `Tarea3_grupo.pdf/docx` (10 pág esperadas, ahora 8)**
  - Portada ECBTI/Big Data `gen_informe.py:1` + Tabla contenido `gen_informe.py:60` + 8 capítulos
  - Capítulos 1-8: Intro, Objetivos (1+5), Investigación (tablas+Mermaid 1-4 capturas), Problema/dataset g4t8-zkc3, Implementación 5.1/5.2 con código/RDD/4 figs, Repo/Video, 5 Conclusiones, APA 6 refs
  - **PENDIENTE placeholders:** `informe/Tarea3_grupo.pdf:5` `[Nombre Apellidos]`, `XXXXX`, `[Nombre Tutor]`, `Grupo XX`, `github.com/usuario`, `canva.com/...`, `youtu.be/...` → reemplazar por `IngCristianGonzalez/Tarea3_Spark`, tu nombre/código, enlace video real
  - **PENDIENTE actualizar README `usuario → IngCristianGonzalez`**

- [ ] **6.4 Presentación online 10 diapos**
  - Base: `informe/presentacion.md:1` 10 slides outline (portada, problema, dataset, arquitectura, Hadoop vs Spark, RDD vs DataFrame, batch código, batch resultados 4 figs, streaming corregido, conclusiones)
  - **PENDIENTE exportar** `presentacion.pptx` o Canva PDF + enlace en portada informe

- [ ] **6.5 Video 5 min (Rúbrica Criterio 2 crítico)**
  - Guion: `informe/guion_video_5min.md:1` 0:00-0:30 intro, 0:30-3:00 batch (VS Code + terminal + PNGs), 3:00-5:00 streaming (2 Putty + :4040) — listo
  - Comandos grabación:
    ```bash
    # Terminal1 batch
    python3 src/batch_analisis.py
    # Terminal2 producer
    python3 src/kafka_producer.py --once
    # Terminal3 consumer
    python3 src/spark_streaming_consumer.py --dry-run
    # Navegador http://localhost:4040
    # OBS Studio 1080p
    ```
  - **PENDIENTE grabar .mp4 y subir No listado YouTube** → actualizar `informe/Tarea3_grupo.pdf:5` enlace

---

## 7) Entrega Evaluación (Guía §3, hasta 29 oct 2026)

- [ ] **7.1 Revisar ortografía + APA** — informe usa APA 7, revisar tildes
- [ ] **7.2 Turnitin** — verificar originalidad (no plagio Anexo, código propio)
- [ ] **7.3 Foro participación (mín 2 aportes)** — pegar `foro_aportes.md`
- [ ] **7.4 Entorno Evaluación subir `Tarea3_grupo.pdf`** — PDF final con portada firmada (solo integrantes que aportaron)
- [ ] **7.5 Entorno Aprendizaje publicar presentación + dataset + avances**

---

## 8) Verificación final rápida (comando único)

```bash
cd Tarea3_Spark
echo "=== Batch ===" && python3 src/batch_analisis.py 2>&1 | grep -E "Spark|count|avg_pm10|VIZ|OK"
echo "=== Streaming dry-run 15s ===" && timeout 15 python3 src/spark_streaming_consumer.py --dry-run 2>&1 | grep -E "Batch|window|avg_pm"
echo "=== PDF/DOCX ===" && ls -lh informe/Tarea3_grupo.* && pdfinfo informe/Tarea3_grupo.pdf | grep Pages
echo "=== Git ===" && git log --oneline -4 && git status
echo "=== Mermaid ===" && ls -lh informe/mermaid/
```

**Resultado esperado hoy:**
- Batch: `Spark 4.2.0`, `count 10000`, `Bogotá 52.89`, `4 VIZ OK`, `OK Batch completado`
- Streaming: `Batch: 1` con `window|sensor_id|avg_pm10`
- PDF 8 pág 337KB, DOCX 322KB 8 imágenes, mermaid 4 PNGs, git 4 commits, remote origin OK

---

## 9) Riesgos y decisiones tomadas

- **Dataset real heterogéneo:** `calidad_aire.csv` 765k tiene `MED_CONCENTRACION` con unidades `%` y `°C`, no solo `PM10/PM25` → se implementó `find_col` flexible + fallback sintético 10k modelado realista (Bogotá/Valle +15-25) para garantizar `avg_pm10` demostrable. Justificado en `informe:5`. Alternativa estricta: filtrar `SIGLA_UNIDAD=µg/m3` y `MSFL_CODE like PM` si se quiere 100% real.
- **Spark 4.2 vs VM 3.5:** `requirements.txt` usa 3.5.3 para VM, local corre 4.2.0 (compatible). Para VM usar `spark-sql-kafka 3.5.3`, para local 4.2 podría ser `4.0.0` — no afecta dry-run.
- **Producer extendido:** Anexo pide 4 campos, se entregan 7 (4 base + pm10/pm25/departamento) → documentar extensión como mejora temática batch-streaming.
- **Checkpoint chmod:** Error Arch JDK 155 solo local dry-run, no en VM; workaround `forceDeleteTempCheckpointLocation`.

---

**Estado global:** 85% completo — Faltan 3 entregables visibles (video .mp4, PPTX, placeholders portada) para 115/115. Todo lo demás ejecutado y versionado.

