# Conclusiones — Taller 1: ETL con PySpark (Online Retail Dataset)

Repositorio: `<<< PEGA AQUÍ EL LINK DE TU REPOSITORIO DE GITHUB >>>`

## Metodología y supuestos

- Se descartaron filas con `UnitPrice <= 0` (ajustes contables/errores, no ventas reales).
- **Ingreso total**: se calcula neto, es decir incluye las devoluciones (`Quantity` negativo), porque eso refleja el ingreso real de la tienda.
- **Producto más vendido** y **ranking de productos**: se calculan solo sobre `Quantity > 0` (ventas reales, sin devoluciones), porque mezclar devoluciones distorsiona el volumen vendido.
- **Productos por factura** (pregunta 8) se interpretó como el número de líneas de producto (filas) por factura, no la suma de unidades. Es la lectura más común de "número de productos por factura".
- **Mes con más ventas** (pregunta 9) se agrupó por mes calendario (enero–diciembre), sumando los dos años del dataset (dic-2010 a dic-2011) para cada mes. El archivo `09_ventas_por_mes.csv` trae el detalle de los 12 meses.
- **Devoluciones** (pregunta 10) se identifican por `Quantity < 0`, tal como pide el enunciado.

## Resultados

| # | Pregunta | Resultado |
|---|----------|-----------|
| 1 | Total de facturas | **23,796** |
| 2 | Clientes únicos | **4,371** |
| 3 | Ingreso total neto | **£9,769,872.05** |
| 4 | Producto más vendido (cantidad) | **23843 – PAPER CRAFT , LITTLE BIRDIE** (80,995 unidades) |
| 5 | Cliente con mayor compra en dinero | **CustomerID 14646** (£279,489.02) |
| 6 | Top 5 países fuera de UK | Netherlands, EIRE, Germany, France, Australia |
| 7 | Ticket promedio por factura | **£410.57** |
| 8 | Productos por factura (min / max / prom) | **1 / 1,114 / 22.67** |
| 9 | Mes con más ventas | **Noviembre** (£1,461,756.25) |
| 10 | % de facturas con devoluciones | **16.12%** (3,836 de 23,796) |

## Hallazgos adicionales

- El producto más vendido (`PAPER CRAFT , LITTLE BIRDIE`) está fuertemente influenciado por una sola factura con una cantidad inusualmente alta — es un outlier conocido de este dataset, no un patrón de consumo típico. Para un análisis de negocio real convendría revisar esa factura puntual antes de sacar conclusiones sobre el producto.
- Noviembre concentra el pico de ventas, consistente con compras de temporada (previo a diciembre/fin de año).
- Casi 1 de cada 6 facturas incluye alguna devolución, lo cual es un porcentaje considerable y podría ser un punto de análisis para el negocio (calidad de producto, política de devoluciones, etc.).

## Entregables

- `etl_pyspark.py` — script con todas las operaciones de PySpark solicitadas (select, filter/where, orderBy, agregaciones, groupBy+agg, withColumn, join, funciones de ventana, export a CSV).
- `outputs/` — un CSV por cada pregunta, más dos rankings (top 20 clientes y top 20 productos) generados con funciones de ventana.
- `data/` — dataset fuente (`OnlineRetail.csv`) y la tabla de referencia país→región usada para el `join()`.
