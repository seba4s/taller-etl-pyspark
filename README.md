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

## Preguntas respondidas

Ver [CONCLUSIONES.md](./CONCLUSIONES.md) para las 10 respuestas, la metodología y los hallazgos.
