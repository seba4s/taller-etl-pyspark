# Taller 1 — ETL con PySpark (Online Retail Dataset)

ETL sobre el [Online Retail Dataset](https://archive.ics.uci.edu/ml/datasets/Online+Retail) de UCI ML Repository, usando PySpark: lectura, selección, filtros, ordenamiento, agregaciones, agrupaciones, columnas derivadas, joins y funciones de ventana.

## Estructura

```
.
├── data/
│   ├── OnlineRetail.csv        # dataset fuente (541,909 filas)
│   └── country_region.csv      # tabla de referencia usada para el join()
├── outputs/                    # resultados de cada pregunta (CSV)
├── etl_pyspark.py              # script principal
├── CONCLUSIONES.md             # respuestas a las 10 preguntas + hallazgos
└── requirements.txt
```

## Cómo correrlo

Requiere Python 3.9+ y Java 8/11/17 instalado (PySpark corre sobre la JVM).

```bash
pip install -r requirements.txt
python3 etl_pyspark.py
```

Los resultados quedan en `outputs/`, uno por cada pregunta del taller, más dos rankings (top 20 clientes y top 20 productos) calculados con `rank()`/`row_number()`.

## Notas para Windows

- **`winutils.exe` / Hadoop**: los CSV de resultados se exportan con `toPandas().to_csv()` en vez del `df.write.csv()` nativo de Spark, porque ese último necesita `winutils.exe`/Hadoop nativo, que Windows no trae por defecto y suele fallar con `HADOOP_HOME and hadoop.home.dir are unset`. El script igual incluye una demostración de `df.write.csv()` nativo sobre el dataset completo (carpeta `outputs/dataset_enriquecido_spark_write/`), envuelta en un `try/except` para que no interrumpa el resto si falla por esto.
- **"no se encontró Python" / Microsoft Store**: si alguna vez ves este mensaje al correr un script de PySpark, significa que el comando `python` en tu PATH está mapeado al *stub* de Microsoft Store en vez del Python real. Se arregla de dos formas (basta con una):
  1. Windows > Configuración > Aplicaciones > Configuración avanzada de aplicaciones > Alias de ejecución de aplicaciones > apaga los alias de `python.exe` y `python3.exe`.
  2. O fija la variable de entorno `PYSPARK_PYTHON` a la ruta real de tu Python, ej. en PowerShell: `$env:PYSPARK_PYTHON = (Get-Command python).Source` antes de correr el script.
  
  Este script en particular ya lo evita del todo: las respuestas que son un solo número (preguntas 1, 2 y 10) se exportan directo con pandas, sin pasar por Spark, así que no dependen de esto.

## Preguntas respondidas

Ver [CONCLUSIONES.md](./CONCLUSIONES.md) para las 10 respuestas, la metodología y los hallazgos.
