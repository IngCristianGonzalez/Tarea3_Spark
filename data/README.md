# Dataset - Tarea 3 Spark

## Selección: Calidad del Aire en Colombia

**Fuente oficial:** Datos Abiertos Colombia - IDEAM
- URL portal: https://www.datos.gov.co/Ambiente-y-Desarrollo-Sostenible/Calidad-del-Aire-en-Colombia/g4t8-zkc3
- CSV directo: https://www.datos.gov.co/api/views/g4t8-zkc3/rows.csv?accessType=DOWNLOAD
- Alterno compilado (PM10/PM2.5 2011-2024, 8.842 filas, 25 cols): https://dathere.github.io/qsv/smart-colombia-calidad-aire.html
- Backup Kaggle sintético si no hay internet: se genera con el script (fallback)

**Licencia:** Open Data Colombia (CC BY)

### Problema definido

> **¿Qué departamentos, municipios y estaciones presentan los niveles más críticos de material particulado (PM10 y PM2.5) en Colombia entre 2011-2024, y cuáles superan los límites recomendados por la OMS (PM10 45 µg/m³ anual, PM2.5 15 µg/m³ anual) para priorizar intervenciones de salud pública y políticas ambientales?**

Justificación Big Data:
- Histórico multi-anual + múltiples estaciones (escala nacional) requiere procesamiento distribuido.
- EDA distribuido con Spark permite agregar por departamento/municipio/año y calcular excedencias de forma paralelizable.
- Conexión temática directa con streaming: el histórico (batch) se complementa con sensores IoT en tiempo real (Kafka + Spark Streaming) que alertan picos instantáneos.

### Esquema esperado

Columnas típicas (varía por versión):
`Departamento, Municipio, Estacion, Fecha, PM10, PM2.5, Promedio, Max, Min, Excedencias, Lat, Lon, ... (25 cols)`

El código `batch_analisis.py` es resiliente: detecta columnas PM10/PM25 equivalentes (PM10, PM2.5, pm10, pm25, promedio, concentration) y hace limpieza automática.

### Descarga

```bash
# Opción 1 - directa
wget "https://www.datos.gov.co/api/views/g4t8-zkc3/rows.csv?accessType=DOWNLOAD" -O data/calidad_aire.csv

# Opción 2 - socrata API (paginada)
python3 -c "import pandas as pd; df=pd.read_csv('https://www.datos.gov.co/api/views/g4t8-zkc3/rows.csv?accessType=DOWNLOAD'); print(df.shape); print(df.head())"

# Opción 3 - sin internet: el script batch_analisis.py genera data/calidad_aire_sintetico.csv automáticamente (n=10000)
```

### Tamaño
- Real: 8k - 80k filas según versión (suficiente para demostrar Spark; en producción escalaría a millones con estaciones IoT).
- Sintético fallback: 10.000 filas generadas localmente para cumplir rúbrica sin depender de internet.

### Uso en el código
`src/batch_analisis.py` hace:
1. Carga CSV (intenta descarga, si falla usa sintético)
2. Limpieza (nulos, duplicados, cast tipos)
3. EDA con DataFrame + operaciones RDD (map, filter, flatMap, groupByKey, reduceByKey) + acciones (collect, count, take, reduce)
4. 4 visualizaciones (histograma PM10, ranking departamentos, evolución anual, exceedances)
5. Guarda resultados en `data/resultados/`