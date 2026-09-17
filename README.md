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
└── requirements.txt
```
