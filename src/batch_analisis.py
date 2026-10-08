#!/usr/bin/env python3
"""
Tarea 3 - Big Data UNAD 202016911 - Procesamiento Batch con Apache Spark
Dataset: Calidad del Aire en Colombia - IDEAM (datos.gov.co)
Problema: Identificar departamentos/estaciones con PM10/PM2.5 críticos y excedencias OMS.

Ejecución:
  spark-submit --master local[*] src/batch_analisis.py
  # o
  python3 src/batch_analisis.py

Requisitos: pyspark, pandas, matplotlib, seaborn
"""

import os
import sys
import random
import urllib.request

# --- Configuración rutas ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "calidad_aire.csv")
SINTETICO_PATH = os.path.join(BASE_DIR, "data", "calidad_aire_sintetico.csv")
RESULTS_DIR = os.path.join(BASE_DIR, "data", "resultados")
os.makedirs(RESULTS_DIR, exist_ok=True)
os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)

# URL oficial datos.gov.co
DATA_URL = "https://www.datos.gov.co/api/views/g4t8-zkc3/rows.csv?accessType=DOWNLOAD"

def generar_dataset_sintetico(path=SINTETICO_PATH, n=10000):
    """Fallback si no hay internet: genera dataset sintético coherente con el problema."""
    import pandas as pd
    random.seed(42)
    departamentos = ["Bogotá D.C.", "Antioquia", "Valle del Cauca", "Atlántico", "Santander", "Boyacá", "Cundinamarca", "Bolívar", "Norte de Santander", "Huila"]
    municipios_map = {
        "Bogotá D.C.": ["Bogotá"], "Antioquia": ["Medellín", "Bello", "Envigado"], "Valle del Cauca": ["Cali", "Palmira"],
        "Atlántico": ["Barranquilla", "Soledad"], "Santander": ["Bucaramanga", "Floridablanca"], "Boyacá": ["Tunja", "Duitama"],
        "Cundinamarca": ["Soacha", "Zipaquirá"], "Bolívar": ["Cartagena"], "Norte de Santander": ["Cúcuta"], "Huila": ["Neiva"]
    }
    estaciones = [f"Estacion_{i:02d}" for i in range(1, 31)]
    rows = []
    for i in range(n):
        dep = random.choice(departamentos)
        mun = random.choice(municipios_map[dep])
        est = random.choice(estaciones)
        year = random.randint(2018, 2024)
        mes = random.randint(1, 12)
        # PM10 y PM2.5 con distribución realista (media ~ 35 y 18, cola alta en Bogotá, Valle)
        base_pm10 = random.gauss(35, 15)
        if dep in ["Bogotá D.C.", "Valle del Cauca"]:
            base_pm10 += random.uniform(10, 25)
        pm10 = max(5, round(base_pm10, 2))
        pm25 = max(3, round(pm10 * random.uniform(0.4, 0.65), 2))
        rows.append([dep, mun, est, year, mes, pm10, pm25, round(random.uniform(-75, -73), 4), round(random.uniform(4, 11), 4)])
    df = pd.DataFrame(rows, columns=["Departamento", "Municipio", "Estacion", "Ano", "Mes", "PM10", "PM25", "Longitud", "Latitud"])
    df.to_csv(path, index=False)
    print(f"[INFO] Dataset sintético generado: {path} ({len(df)} filas)")
    return path

def obtener_dataset():
    """Intenta descargar el CSV real; si falla, usa sintético."""
    # Si ya existe alguno, usarlo
    if os.path.exists(DATA_PATH) and os.path.getsize(DATA_PATH) > 1000:
        print(f"[INFO] Usando dataset existente: {DATA_PATH}")
        return DATA_PATH
    if os.path.exists(SINTETICO_PATH) and os.path.getsize(SINTETICO_PATH) > 1000:
        print(f"[INFO] Usando sintético existente: {SINTETICO_PATH}")
        return SINTETICO_PATH
    try:
        print(f"[INFO] Descargando dataset desde {DATA_URL} ...")
        urllib.request.urlretrieve(DATA_URL, DATA_PATH)
        if os.path.getsize(DATA_PATH) > 1000:
            print(f"[INFO] Descarga OK: {DATA_PATH}")
            return DATA_PATH
    except Exception as e:
        print(f"[WARN] No se pudo descargar ({e}), generando sintético...")
    return generar_dataset_sintetico()

def main():
    csv_path = obtener_dataset()
    print(f"[INFO] CSV path: {csv_path}")

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, avg, max as spark_max, min as spark_min, count, when, year as spark_year
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    import pandas as pd

    spark = SparkSession.builder \
        .appName("Tarea3_Batch_CalidadAire") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[INFO] Spark {spark.version} iniciado")

    # ============================================================
    # 1. CARGA - DataFrame
    # ============================================================
    # Detectar si es sintético (esquema conocido) o real (esquema variable)
    # Intentar inferir esquema automáticamente para robustez
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    print("=== Esquema ===")
    df.printSchema()
    print(f"Total filas (count): {df.count()}")
    print("=== Primeras filas ===")
    df.show(5, truncate=False)
    print("=== Estadísticas descriptivas ===")
    df.describe().show()

    # Normalizar nombres de columnas a estándar interno
    # Mapeo flexible: buscar columnas que contengan pm10, pm2.5, departamento, etc.
    cols_lower = {c.lower(): c for c in df.columns}
    def find_col(keywords):
        for k in keywords:
            for lc, orig in cols_lower.items():
                if k in lc:
                    return orig
        return None

    col_dep = find_col(["departamento", "depto", "department"]) or df.columns[0]
    col_mun = find_col(["municipio", "municipio", "city"]) or df.columns[1] if len(df.columns) > 1 else col_dep
    col_est = find_col(["estacion", "station", "estación"]) or col_dep
    col_pm10 = find_col(["pm10", "pm_10", "pm 10"])
    col_pm25 = find_col(["pm2.5", "pm25", "pm_2.5", "pm2_5"])
    col_ano = find_col(["ano", "año", "year", "vigencia", "fecha"])

    print(f"[MAPEO] Departamento={col_dep}, Municipio={col_mun}, Estacion={col_est}, PM10={col_pm10}, PM25={col_pm25}, Año={col_ano}")

    # Si no se detecta PM10/PM25, usar sintético directamente
    if col_pm10 is None or col_pm25 is None:
        print("[WARN] No se detectaron columnas PM10/PM25 en el CSV real. Regenerando sintético y recargando...")
        csv_path = generar_dataset_sintetico(n=10000)
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        df.printSchema()
        col_dep, col_mun, col_est, col_pm10, col_pm25, col_ano = "Departamento", "Municipio", "Estacion", "PM10", "PM25", "Ano"

    # ============================================================
    # 2. LIMPIEZA Y TRANSFORMACIÓN (DataFrame API)
    # ============================================================
    # Cast y limpieza de nulos
    # Crear columnas normalizadas
    df_clean = df
    # Asegurar tipos numéricos para PM
    for c in [col_pm10, col_pm25]:
        if c:
            df_clean = df_clean.withColumn(c, col(c).cast(DoubleType()))

    # Filtrar nulos en claves
    df_clean = df_clean.filter(col(col_dep).isNotNull() & col(col_pm10).isNotNull())
    # Eliminar duplicados
    df_clean = df_clean.dropDuplicates()
    # Crear columna de excedencia OMS (PM10 > 45, PM2.5 > 15 anual)
    if col_pm10:
        df_clean = df_clean.withColumn("excede_pm10_oms", when(col(col_pm10) > 45, 1).otherwise(0))
    if col_pm25:
        df_clean = df_clean.withColumn("excede_pm25_oms", when(col(col_pm25) > 15, 1).otherwise(0))

    print("=== Datos limpios ===")
    df_clean.show(5)
    print(f"Filas tras limpieza: {df_clean.count()}")

    # Cache para reuso
    df_clean.cache()

    # ============================================================
    # 3. ANÁLISIS EXPLORATORIO (DataFrame)
    # ============================================================
    print("\n=== 3.1 Promedio PM10/PM25 por Departamento ===")
    agg_dep = df_clean.groupBy(col_dep).agg(
        avg(col_pm10).alias("avg_pm10"),
        avg(col_pm25).alias("avg_pm25") if col_pm25 else avg(col_pm10).alias("avg_pm25"),
        spark_max(col_pm10).alias("max_pm10"),
        spark_min(col_pm10).alias("min_pm10"),
        count("*").alias("n_muestras"),
        avg("excede_pm10_oms").alias("tasa_excede_pm10")
    ).orderBy(col("avg_pm10").desc())
    agg_dep.show(20, truncate=False)
    agg_dep.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(RESULTS_DIR, "avg_por_departamento"))

    print("\n=== 3.2 Evolución anual (si hay columna año) ===")
    if col_ano:
        # Si col_ano es fecha string, intentar extraer año
        try:
            agg_year = df_clean.groupBy(col_ano).agg(
                avg(col_pm10).alias("avg_pm10"),
                avg(col_pm25).alias("avg_pm25") if col_pm25 else avg(col_pm10).alias("avg_pm25"),
                count("*").alias("n")
            ).orderBy(col_ano)
            agg_year.show(20)
            agg_year.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(RESULTS_DIR, "evolucion_anual"))
        except Exception as e:
            print(f"[WARN] No se pudo agrupar por año: {e}")

    print("\n=== 3.3 Top 10 estaciones más contaminadas ===")
    agg_est = df_clean.groupBy(col_est, col_dep).agg(
        avg(col_pm10).alias("avg_pm10"),
        count("*").alias("n")
    ).orderBy(col("avg_pm10").desc()).limit(10)
    agg_est.show(truncate=False)

    print("\n=== 3.4 Excedencias totales ===")
    total_excede = df_clean.agg(
        count("*").alias("total"),
        count(when(col("excede_pm10_oms") == 1, True)).alias("excede_pm10"),
        count(when(col("excede_pm25_oms") == 1, True)).alias("excede_pm25")
    )
    total_excede.show()

    # ============================================================
    # 4. OPERACIONES RDD (requisito guía: demostrar RDD)
    # ============================================================
    print("\n=== 4. OPERACIONES RDD (map, filter, flatMap, groupByKey, reduceByKey + collect/count/take/reduce) ===")
    # Convertir a RDD[(departamento, pm10)]
    rdd = df_clean.select(col_dep, col_pm10).rdd.map(lambda row: (row[0], float(row[1]) if row[1] is not None else 0.0))
    # filter: solo registros con PM10 > 45 (excede OMS)
    rdd_filtrado = rdd.filter(lambda x: x[1] > 45)
    print(f"RDD count total: {rdd.count()}")  # acción count
    print(f"RDD count excede OMS (filter >45): {rdd_filtrado.count()}")
    print(f"RDD take(3): {rdd.take(3)}")  # acción take

    # map: duplicar para ejemplo
    rdd_map = rdd.map(lambda x: (x[0], x[1] * 1.0))
    # flatMap: ejemplo tokenizar departamentos en palabras
    rdd_flat = rdd.flatMap(lambda x: [(word, 1) for word in x[0].split()])
    print(f"flatMap ejemplo (palabras en departamentos) take 5: {rdd_flat.take(5)}")

    # groupByKey: agrupar PM10 por departamento (menos eficiente, demostración)
    rdd_group = rdd.groupByKey().mapValues(list)
    print(f"groupByKey ejemplo take 2: {rdd_group.take(2)}")

    # reduceByKey: sumar PM10 por departamento y contar para promedio (eficiente)
    rdd_sum = rdd.reduceByKey(lambda a, b: a + b)  # suma
    rdd_count = rdd.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)  # conteo
    # join para promedio
    rdd_avg = rdd_sum.join(rdd_count).map(lambda x: (x[0], x[1][0] / x[1][1]))
    print("reduceByKey - avg PM10 por departamento (collect):")
    for dep, avg_val in rdd_avg.collect():  # acción collect
        print(f"  {dep}: {avg_val:.2f}")

    # reduce: encontrar PM10 máximo global vía RDD
    max_pm10 = rdd.map(lambda x: x[1]).reduce(lambda a, b: max(a, b))  # acción reduce
    print(f"RDD reduce - PM10 máximo global: {max_pm10}")

    # ============================================================
    # 5. VISUALIZACIONES (matplotlib/seaborn)
    # ============================================================
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import seaborn as sns
        sns.set(style="whitegrid")

        # Preparar pandas para gráficas (colectar agregados pequeños, no todo el dataset)
        pdf_dep = agg_dep.toPandas()
        # 5.1 Ranking departamentos
        plt.figure(figsize=(10, 6))
        plt.barh(pdf_dep[col_dep].astype(str), pdf_dep["avg_pm10"], color="tomato")
        plt.axvline(45, color="red", linestyle="--", label="Límite OMS PM10 (45)")
        plt.xlabel("PM10 promedio (µg/m³)")
        plt.title("PM10 Promedio por Departamento - Calidad del Aire Colombia")
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_DIR, "01_ranking_departamentos_pm10.png"), dpi=150)
        plt.close()
        print("[VIZ] 01_ranking_departamentos_pm10.png OK")

        # 5.2 Histograma PM10
        pdf_sample = df_clean.select(col_pm10).limit(5000).toPandas()
        plt.figure(figsize=(8, 5))
        plt.hist(pdf_sample[col_pm10].dropna(), bins=30, color="skyblue", edgecolor="black")
        plt.axvline(45, color="red", linestyle="--", label="OMS 45")
        plt.xlabel("PM10 (µg/m³)")
        plt.ylabel("Frecuencia")
        plt.title("Distribución PM10")
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_DIR, "02_histograma_pm10.png"), dpi=150)
        plt.close()
        print("[VIZ] 02_histograma_pm10.png OK")

        # 5.3 Evolución anual
        if col_ano:
            try:
                pdf_year = agg_year.toPandas()
                plt.figure(figsize=(8, 5))
                plt.plot(pdf_year[col_ano].astype(str), pdf_year["avg_pm10"], marker="o", label="PM10")
                if "avg_pm25" in pdf_year.columns:
                    plt.plot(pdf_year[col_ano].astype(str), pdf_year["avg_pm25"], marker="s", label="PM2.5")
                plt.axhline(45, color="red", linestyle="--", label="OMS PM10")
                plt.xlabel("Año")
                plt.ylabel("Promedio µg/m³")
                plt.title("Evolución Anual PM10/PM2.5")
                plt.legend()
                plt.xticks(rotation=45)
                plt.tight_layout()
                plt.savefig(os.path.join(RESULTS_DIR, "03_evolucion_anual.png"), dpi=150)
                plt.close()
                print("[VIZ] 03_evolucion_anual.png OK")
            except Exception as e:
                print(f"[WARN] No se pudo graficar evolución anual: {e}")

        # 5.4 Tasa excedencia por departamento
        plt.figure(figsize=(10, 6))
        pdf_dep_sorted = pdf_dep.sort_values("tasa_excede_pm10", ascending=False)
        plt.barh(pdf_dep_sorted[col_dep].astype(str), pdf_dep_sorted["tasa_excede_pm10"] * 100, color="orange")
        plt.xlabel("% muestras que exceden OMS PM10 (>45)")
        plt.title("% Excedencias OMS por Departamento")
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_DIR, "04_tasa_excedencia.png"), dpi=150)
        plt.close()
        print("[VIZ] 04_tasa_excedencia.png OK")

    except Exception as e:
        print(f"[WARN] Visualizaciones fallaron (instalar matplotlib/seaborn): {e}")

    print("\n=== RESUMEN RESULTADOS ===")
    print(f"Resultados guardados en: {RESULTS_DIR}")
    print(os.listdir(RESULTS_DIR))
    spark.stop()
    print("[OK] Batch completado. Listo para video y reporte.")

if __name__ == "__main__":
    main()
