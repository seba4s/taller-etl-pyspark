"""
Taller 1 - ETL con PySpark (Online Retail Dataset)
===================================================
Aplica un proceso ETL con PySpark sobre el dataset "Online Retail" (UCI ML
Repository) usando select, filter/where, orderBy, agregaciones, groupBy+agg,
withColumn, join y funciones de ventana (rank/row_number), y responde las
10 preguntas de negocio del taller exportando cada resultado a un CSV.

Ejecución:
    python3 etl_pyspark.py
"""

import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "data", "OnlineRetail.csv")
REGION_PATH = os.path.join(BASE_DIR, "data", "country_region.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")


def save_csv(df, name, coalesce=True):
    """Exporta un DataFrame a un único CSV legible dentro de OUTPUT_DIR."""
    tmp_path = os.path.join(OUTPUT_DIR, f"_tmp_{name}")
    (df.coalesce(1) if coalesce else df).write.mode("overwrite").option(
        "header", "true"
    ).csv(tmp_path)

    final_path = os.path.join(OUTPUT_DIR, f"{name}.csv")
    part_file = [f for f in os.listdir(tmp_path) if f.startswith("part-")][0]
    shutil.move(os.path.join(tmp_path, part_file), final_path)
    shutil.rmtree(tmp_path)
    print(f"  -> {name}.csv exportado")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("TallerETL_OnlineRetail")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # 1. LECTURA DE DATOS
    # ------------------------------------------------------------------
    print("1) Leyendo dataset...")
    raw = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(DATA_PATH)
    )
    print(f"   Filas leídas: {raw.count():,}  |  Columnas: {raw.columns}")

    # ------------------------------------------------------------------
    # 2. SELECCIÓN DE COLUMNAS + limpieza básica
    # ------------------------------------------------------------------
    df = raw.select(
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
    )

    # ------------------------------------------------------------------
    # 3. FILTRADO — nos quedamos con filas con precio válido
    #    (descartamos UnitPrice <= 0, que son ajustes contables, no ventas)
    # ------------------------------------------------------------------
    df = df.filter(F.col("UnitPrice") > 0)

    # ------------------------------------------------------------------
    # 4. COLUMNAS DERIVADAS (withColumn)
    # ------------------------------------------------------------------
    df = (
        df.withColumn("TotalPrice", F.round(F.col("Quantity") * F.col("UnitPrice"), 2))
        .withColumn("IsReturn", F.when(F.col("Quantity") < 0, F.lit(True)).otherwise(F.lit(False)))
        .withColumn("InvoiceYear", F.year("InvoiceDate"))
        .withColumn("InvoiceMonthNum", F.month("InvoiceDate"))
        .withColumn("InvoiceMonthName", F.date_format("InvoiceDate", "MMMM"))
        .withColumn("InvoiceYearMonth", F.date_format("InvoiceDate", "yyyy-MM"))
    )

    df.cache()

    # ------------------------------------------------------------------
    # 8. JOIN — el dataset original es una sola tabla plana, así que
    #    incorporamos una tabla de referencia (país -> región) y hacemos
    #    join() para enriquecer los datos.
    # ------------------------------------------------------------------
    print("2) Uniendo con tabla de referencia país -> región (join)...")
    region_ref = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(REGION_PATH)
    )
    df = df.join(region_ref, on="Country", how="left")

    # ------------------------------------------------------------------
    # 9. FUNCIONES DE VENTANA — ranking de clientes y de productos
    # ------------------------------------------------------------------
    print("3) Calculando rankings con funciones de ventana...")
    ventas_cliente = (
        df.filter(F.col("CustomerID").isNotNull())
        .groupBy("CustomerID")
        .agg(F.round(F.sum("TotalPrice"), 2).alias("TotalGastado"))
    )
    w_clientes = Window.orderBy(F.col("TotalGastado").desc())
    ranking_clientes = ventas_cliente.withColumn(
        "Ranking", F.row_number().over(w_clientes)
    ).orderBy("Ranking")
    save_csv(ranking_clientes.limit(20), "ranking_top20_clientes")

    ventas_producto = (
        df.filter(F.col("Quantity") > 0)
        .groupBy("StockCode", "Description")
        .agg(F.sum("Quantity").alias("CantidadVendida"))
    )
    w_productos = Window.orderBy(F.col("CantidadVendida").desc())
    ranking_productos = ventas_producto.withColumn(
        "Ranking", F.rank().over(w_productos)
    ).orderBy("Ranking")
    save_csv(ranking_productos.limit(20), "ranking_top20_productos")

    # ==================================================================
    # PREGUNTAS DEL TALLER
    # ==================================================================
    print("4) Respondiendo las 10 preguntas del taller...")

    # 1. Número total de facturas
    total_facturas = df.select("InvoiceNo").distinct().count()
    print(f"   1. Total de facturas: {total_facturas:,}")
    save_csv(
        spark.createDataFrame([(total_facturas,)], ["TotalFacturas"]),
        "01_total_facturas",
    )

    # 2. Número de clientes únicos
    total_clientes = df.filter(F.col("CustomerID").isNotNull()).select(
        "CustomerID"
    ).distinct().count()
    print(f"   2. Clientes únicos: {total_clientes:,}")
    save_csv(
        spark.createDataFrame([(total_clientes,)], ["ClientesUnicos"]),
        "02_clientes_unicos",
    )

    # 3. Ingreso total (Quantity * UnitPrice), neto de devoluciones
    ingreso_total = df.agg(F.round(F.sum("TotalPrice"), 2).alias("IngresoTotalNeto"))
    print(f"   3. Ingreso total neto: {ingreso_total.collect()[0][0]:,}")
    save_csv(ingreso_total, "03_ingreso_total")

    # 4. Producto más vendido en cantidad (solo ventas, Quantity > 0)
    producto_top = (
        df.filter(F.col("Quantity") > 0)
        .groupBy("StockCode", "Description")
        .agg(F.sum("Quantity").alias("CantidadVendida"))
        .orderBy(F.col("CantidadVendida").desc())
        .limit(1)
    )
    print(f"   4. Producto más vendido: {producto_top.collect()[0].asDict()}")
    save_csv(producto_top, "04_producto_mas_vendido")

    # 5. Cliente con mayor volumen de compra en dinero
    cliente_top = (
        df.filter(F.col("CustomerID").isNotNull())
        .groupBy("CustomerID")
        .agg(F.round(F.sum("TotalPrice"), 2).alias("TotalGastado"))
        .orderBy(F.col("TotalGastado").desc())
        .limit(1)
    )
    print(f"   5. Cliente con mayor compra: {cliente_top.collect()[0].asDict()}")
    save_csv(cliente_top, "05_cliente_mayor_compra")

    # 6. Los 5 países que más compran fuera de Reino Unido
    top5_paises = (
        df.where(F.col("Country") != "United Kingdom")
        .groupBy("Country")
        .agg(F.round(F.sum("TotalPrice"), 2).alias("TotalComprado"))
        .orderBy(F.col("TotalComprado").desc())
        .limit(5)
    )
    print("   6. Top 5 países (fuera de UK):")
    for row in top5_paises.collect():
        print(f"      {row['Country']}: {row['TotalComprado']:,}")
    save_csv(top5_paises, "06_top5_paises_fuera_uk")

    # 7. Ticket promedio por factura
    total_por_factura = df.groupBy("InvoiceNo").agg(
        F.sum("TotalPrice").alias("TotalFactura")
    )
    ticket_promedio = total_por_factura.agg(
        F.round(F.avg("TotalFactura"), 2).alias("TicketPromedio")
    )
    print(f"   7. Ticket promedio por factura: {ticket_promedio.collect()[0][0]:,}")
    save_csv(ticket_promedio, "07_ticket_promedio")

    # 8. Mínimo, máximo y promedio de PRODUCTOS (líneas) por factura
    lineas_por_factura = df.groupBy("InvoiceNo").agg(
        F.count("StockCode").alias("NumProductos")
    )
    stats_productos_factura = lineas_por_factura.agg(
        F.min("NumProductos").alias("MinProductos"),
        F.max("NumProductos").alias("MaxProductos"),
        F.round(F.avg("NumProductos"), 2).alias("PromedioProductos"),
    )
    print(f"   8. Min/Max/Promedio productos por factura: "
          f"{stats_productos_factura.collect()[0].asDict()}")
    save_csv(stats_productos_factura, "08_stats_productos_por_factura")

    # 9. Mes del año con más ventas (calendario, sumando todos los años)
    ventas_por_mes = (
        df.groupBy("InvoiceMonthNum", "InvoiceMonthName")
        .agg(F.round(F.sum("TotalPrice"), 2).alias("TotalVentas"))
        .orderBy(F.col("InvoiceMonthNum"))
    )
    mes_top = ventas_por_mes.orderBy(F.col("TotalVentas").desc()).limit(1)
    print(f"   9. Mes con más ventas: {mes_top.collect()[0].asDict()}")
    save_csv(ventas_por_mes, "09_ventas_por_mes")

    # 10. % de facturas con devoluciones (Quantity negativo)
    facturas_con_devolucion = (
        df.filter(F.col("IsReturn")).select("InvoiceNo").distinct().count()
    )
    pct_devoluciones = round(100.0 * facturas_con_devolucion / total_facturas, 2)
    print(f"   10. % facturas con devoluciones: {pct_devoluciones}%")
    save_csv(
        spark.createDataFrame(
            [(facturas_con_devolucion, total_facturas, pct_devoluciones)],
            ["FacturasConDevolucion", "TotalFacturas", "PorcentajeDevoluciones"],
        ),
        "10_pct_facturas_devoluciones",
    )

    print("\nProceso ETL completado. Resultados en:", OUTPUT_DIR)
    spark.stop()


if __name__ == "__main__":
    main()
