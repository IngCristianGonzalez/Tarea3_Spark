#!/usr/bin/env python3
"""
Consumer Spark Structured Streaming + Kafka - Tarea 3 (Anexo 3 CORREGIDO)
Corrige bugs del anexo: doble SparkSession, timestamp INT vs TimestampType, window sin watermark, package outdated.

Uso VM (vboxuser):
  /opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data
  python3 src/kafka_producer.py              # terminal 1
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 src/spark_streaming_consumer.py  # terminal 2
  # Ver Spark UI: http://<ip-vm>:4040

Uso local sin Kafka (para demo sin VM):
  python3 src/spark_streaming_consumer.py --dry-run  # usa rate source simulada

Notas:
 - Este archivo usa timestamp como Long (epoch seconds) y lo convierte a TimestampType correctamente.
 - Añade watermark y ventana de 1 minuto por sensor_id + departamento.
 - Calcula avg PM10/PM2.5 + temp/humidity y cuenta eventos.
"""
import argparse

def get_spark():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("Tarea3_Streaming_CalidadAire") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Sin Kafka, usa rate source")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="sensor_data")
    args = parser.parse_args()

    spark = get_spark()

    from pyspark.sql.functions import from_json, col, window, avg, count, expr, to_timestamp
    from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, DoubleType, StringType, LongType

    # Esquema debe coincidir con kafka_producer.py (timestamp es Long epoch)
    schema = StructType([
        StructField("sensor_id", IntegerType()),
        StructField("departamento", StringType()),
        StructField("pm10", DoubleType()),
        StructField("pm25", DoubleType()),
        StructField("temperature", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("timestamp", LongType())
    ])

    if args.dry_run:
        # Simulación sin Kafka: generamos stream de rate y lo transformamos a datos fake
        print("[INFO] Modo dry-run: usando rate source (sin Kafka)")
        df_raw = spark.readStream.format("rate").option("rowsPerSecond", 2).load()
        # rate genera: timestamp, value (long)
        from pyspark.sql.functions import rand, round as spark_round
        df = df_raw.select(
            (col("value") % 10 + 1).cast(IntegerType()).alias("sensor_id"),
            expr("CASE WHEN rand() > 0.5 THEN 'Bogotá D.C.' ELSE 'Antioquia' END").alias("departamento"),
            (spark_round(rand()*30+15, 2)).cast(DoubleType()).alias("pm10"),
            (spark_round(rand()*20+8, 2)).cast(DoubleType()).alias("pm25"),
            (spark_round(rand()*10+20, 2)).cast(DoubleType()).alias("temperature"),
            (spark_round(rand()*40+30, 2)).cast(DoubleType()).alias("humidity"),
            col("timestamp")  # ya es TimestampType
        )
        # df ya está parseado
        parsed_df = df
    else:
        # Kafka source
        df_kafka = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", args.bootstrap) \
            .option("subscribe", args.topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        # Kafka value es binary -> string -> json
        parsed_df = df_kafka.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        # Convertir timestamp Long (epoch seconds) -> TimestampType
        # El anexo usaba TimestampType directo y fallaba. Corrección:
        parsed_df = parsed_df.withColumn(
            "event_time",
            to_timestamp(col("timestamp").cast(LongType()))
        ).drop("timestamp").withColumnRenamed("event_time", "timestamp")

        # Si ya es TimestampType (dry-run), watermark funciona directo
        # Para Kafka, también ya es TimestampType tras conversión

    # Si no es dry-run, timestamp ya es TimestampType; si dry-run, también lo es (rate)
    # Asegurar que parsed_df tenga columna timestamp TimestampType
    # Watermark + ventana 1 minuto por sensor + departamento
    windowed_stats = parsed_df \
        .withWatermark("timestamp", "2 minutes") \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("sensor_id"),
            col("departamento")
        ).agg(
            avg("pm10").alias("avg_pm10"),
            avg("pm25").alias("avg_pm25"),
            avg("temperature").alias("avg_temp"),
            avg("humidity").alias("avg_hum"),
            count("*").alias("eventos")
        )

    # Output a consola (complete o append según watermark)
    query = windowed_stats.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 20) \
        .trigger(processingTime="10 seconds") \
        .start()

    print("[INFO] Streaming iniciado. Esperando datos...")
    print("[INFO] Spark UI: http://localhost:4040  (o http://<ip-vm>:4040 en VM)")
    print("[INFO] Ctrl+C para detener")
    query.awaitTermination()

if __name__ == "__main__":
    main()
