# Investigación Teórica - Tarea 3 Big Data (202016911)

## 1. Tabla comparativa Hadoop vs Spark

| Criterio | Apache Hadoop | Apache Spark |
|---|---|---|
| **Arquitectura** | Ecosistema batch basado en HDFS (almacenamiento distribuido) + YARN (gestión recursos) + MapReduce (cómputo). Diseño disco-céntrico. | Motor unificado con DAG (Directed Acyclic Graph), in-memory, puede correr sobre HDFS, YARN, Kubernetes, Standalone. APIs para SQL, Streaming, ML, Graph. |
| **Modelo de procesamiento** | MapReduce: 2 fases (Map -> Shuffle/Sort -> Reduce) siempre escribe a disco. Batch puro. | RDD/DataFrame/Dataset + Spark SQL + Structured Streaming + Spark Streaming (micro-batch). Procesamiento en memoria con lineage. |
| **Rendimiento** | Latencia alta. 10-100x más lento que Spark en iterativos por I/O a disco. | 10-100x más rápido en iterativos/ML por cache en RAM y DAG optimizado por Catalyst/Tungsten. |
| **Tolerancia a fallos** | Replicación HDFS (x3) + re-ejecución de tasks MapReduce. | RDD lineage: recomputa particiones perdidas desde ancestros, sin replicación. Checkpointing para streaming. |
| **Latencia / Tiempo real** | No nativo (requiere componentes extra: Storm). | Nativo: Spark Streaming (DStreams) y Structured Streaming (continuo/micro-batch ~100ms). |
| **Casos de uso** | ETL masivo histórico, data lake económico, archivado, jobs batch que no requieren iteración. | ML iterativo (MLlib), EDA interactivo, streaming en tiempo real, análisis grafos (GraphX), queries SQL ad-hoc. |
| **Lenguajes** | Java (principal), Streaming API para Python. | Scala, Java, Python (PySpark), R, SQL. |
| **Costo** | Barato en almacenamiento (discos). | Más RAM requerida, costo mayor pero menor tiempo cómputo. |

**Conclusión:** Hadoop y Spark son complementarios: Hadoop aporta almacenamiento barato y escalable (HDFS), Spark aporta velocidad y versatilidad. En la práctica moderna se usa **Hadoop como storage (HDFS) + Spark como engine**.

---

## 2. RDD (Resilient Distributed Dataset)

**Concepto:** Abstracción fundamental de Spark (Spark 1.x). Colección de objetos particionada, distribuida, inmutable y tolerante a fallos. Evalúa de forma *lazy* (perezoza) y se materializa solo con acciones.

### Propiedades

1.  **Inmutabilidad:** Una vez creado, un RDD no se modifica. Cada transformación genera un nuevo RDD. Permite lineage y recuperación sin replicación.
2.  **Particionamiento:** Datos divididos en particiones distribuidas por el cluster. Controlable con `repartition()` / `coalesce()`. Define paralelismo.
3.  **Tolerancia a fallos (Resilient):** Via *lineage* (grafo de dependencias). Si una partición se pierde, Spark la recomputa desde origen.
4.  **Evaluación perezosa (Lazy):** Transformaciones no se ejecutan hasta una acción.
5.  **Tipado:** RDD puede contener objetos de cualquier tipo (Java/Scala/Python).
6.  **Persistencia:** `cache()` / `persist(MEMORY_ONLY, MEMORY_AND_DISK, etc.)`.

### Operaciones RDD

**Transformaciones (lazy, devuelven RDD):**
- `map(func)`: Aplica función a cada elemento. Ej: `rdd.map(lambda x: x*2)`
- `filter(func)`: Filtra. Ej: `rdd.filter(lambda x: x > 10)`
- `flatMap(func)`: Map + aplanado. Ej: `rdd.flatMap(lambda x: x.split(" "))`
- `groupByKey()`: Agrupa valores por clave. `(K,V) -> (K, Iterable<V>)` — **costoso (shuffle)**, preferir `reduceByKey`.
- `reduceByKey(func)`: Combina valores por clave localmente antes del shuffle. Mucho más eficiente. Ej: `rdd.reduceByKey(lambda a,b: a+b)`

**Acciones (eager, devuelven valor/efecto):**
- `collect()`: Trae todo el RDD al driver — peligroso con datos grandes.
- `count()`: Número de elementos.
- `take(n)`: Primeros n elementos.
- `reduce(func)`: Agrega con función asociativa. Ej: `rdd.reduce(lambda a,b: a+b)`

---

## 3. DataFrame

**Concepto:** Colección distribuida de datos organizada en columnas con nombre (como tabla SQL), introducida en Spark 1.3. API de alto nivel construida sobre RDD, optimizada por **Catalyst Optimizer** + **Tungsten** (off-heap, code generation).

### Diferencias y ventajas sobre RDD

| Aspecto | RDD | DataFrame |
|---|---|---|
| **Abstracción** | Bajo nivel, objetos opacos | Alto nivel, tabla con esquema |
| **Esquema** | No (inferido por tipos) | Sí (StructType, nullable, etc.) |
| **Optimización** | Manual | Automática (Catalyst genera plan lógico/físico óptimo) |
| **Performance** | Menor (sin optimizador) | 10-50x más rápido por Tungsten + whole-stage codegen |
| **Memoria** | Objetos Java | Formato columnar, binario, eficiente |
| **API** | Funcional (map/filter) | Declarativa (SQL, `select`, `groupBy`, `agg`) |
| **Interoperabilidad** | Solo código | SQL, Python, R, JSON, Parquet |
| **Tungsten** | No | Sí |

**Ventajas clave DataFrame:**
1. Optimización automática, no requiere tuning manual de particiones para queries.
2. Ejecución en formato nativo (Tungsten) reduce GC.
3. Integración con Spark SQL: `df.createOrReplaceTempView()` + `spark.sql("SELECT ...")`.
4. Mejor para EDA, ETL y fuentes estructuradas (CSV, Parquet, JDBC, Kafka JSON).
5. Soporte para Structured Streaming (DataFrame en tiempo real).

**Cuándo usar RDD:** Datos no estructurados, manipulación de bajo nivel, control fino de particionamiento, legado.

---

## 4. Arquitectura Apache Kafka

```
[Producer] --\
[Producer] ----> [Topic: sensor_data] ---> [Broker 1] ----> [Consumer Group] -> [Spark Structured Streaming]
[Producer] --/       (partitions)           [Broker 2]            |
                                            [Broker 3]            +-> [ZooKeeper / KRaft]
                                            (replication)
```

**Diagrama textual:**
- **Producer** envía mensajes a **Topic**.
- **Topic** dividido en **Partitions** (orden dentro de partición, paralelismo).
- **Broker** = servidor Kafka que almacena particiones (cluster de 3+ brokers).
- **Replication factor** = copias de cada partición en distintos brokers (fault tolerance).
- **Consumer** (o Consumer Group) lee de particiones; cada partición asignada a un solo consumer del grupo (escalabilidad).
- **ZooKeeper / KRaft**: coordina metadatos, elección de líder, (Kafka 3.x migra a KRaft sin ZooKeeper).

### Conceptos clave

- **Topic:** Categoría/feed donde se publican mensajes. Ej: `sensor_data`. Inmutable log append-only.
- **Partition:** Sub-división ordenada e inmutable de un topic. Clave para paralelismo y orden por llave. `topic --partitions 3` crea 3 logs paralelos.
- **Broker:** Nodo del cluster Kafka (proceso `kafka-server-start.sh`). Almacena particiones, responde a producers/consumers.
- **Producer:** Cliente que publica mensajes (`KafkaProducer` en Python). Elige partición por round-robin o por key.
- **Consumer:** Cliente que se suscribe (`subscribe`) y hace poll de mensajes. Mantiene *offset* (posición leída).
- **Offset:** Índice secuencial dentro de partición. Commit manual/automático.
- **Consumer Group:** Conjunto de consumers que cooperan para consumir un topic en paralelo (cada partición -> un consumer del grupo).
- **ZooKeeper / KRaft:** Antes ZooKeeper gestionaba quorum; desde Kafka 2.8+ KRaft (Kafka Raft) lo reemplaza.

**Flujo Anexo 3:**
`kafka_producer.py (sensor simulado cada 1s) -> Topic sensor_data (1 partition, RF 1) -> spark_streaming_consumer.py (Structured Streaming lee kafka, window 1 min, avg temp/humidity) -> console + Spark UI :4040`