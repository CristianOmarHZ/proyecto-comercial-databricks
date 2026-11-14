# Datasets de origen — Proyecto comercial Olist

## Fuente

- **Dataset:** Brazilian E-Commerce Public Dataset by Olist.
- **Repositorio público:** https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
- **Archivo original utilizado:** `archive (1).zip` (9 archivos CSV).
- **Origen operativo del ETL:** Azure Data Lake Storage Gen2, contenedor `raw`, cuenta `adlsmartdata08100`.
- **Ruta base:** `abfss://raw@adlsmartdata08100.dfs.core.windows.net/`.
- **Acceso desde Databricks:** Unity Catalog / External Location con Managed Identity; no se utiliza DBFS ni Volumes como origen Raw.

## Inventario de archivos

Los conteos corresponden a los registros confirmados durante la ingesta Bronze del proyecto; no incluyen la cabecera de los CSV.

| CSV original | Registros | Entidad | Campos representativos |
|---|---:|---|---|
| `olist_orders_dataset.csv` | 99.441 | Pedidos | `order_id`, `customer_id`, `order_status`, `order_purchase_timestamp` |
| `olist_order_items_dataset.csv` | 112.650 | Detalles de pedidos | `order_id`, `order_item_id`, `product_id`, `seller_id`, `price`, `freight_value` |
| `olist_customers_dataset.csv` | 99.441 | Clientes por pedido | `customer_id`, `customer_unique_id`, `customer_city`, `customer_state` |
| `olist_products_dataset.csv` | 32.951 | Productos | `product_id`, `product_category_name`, `product_weight_g` |
| `olist_sellers_dataset.csv` | 3.095 | Vendedores | `seller_id`, `seller_city`, `seller_state` |
| `olist_order_payments_dataset.csv` | 103.886 | Pagos | `order_id`, `payment_sequential`, `payment_type`, `payment_value` |
| `olist_order_reviews_dataset.csv` | 99.224 | Reseñas | `review_id`, `order_id`, `review_score`, `review_creation_date` |
| `olist_geolocation_dataset.csv` | 1.000.163 | Ubicaciones geográficas | `geolocation_zip_code_prefix`, `geolocation_lat`, `geolocation_lng` |
| `product_category_name_translation.csv` | 71 | Traducción de categorías | `product_category_name`, `product_category_name_english` |
| **Total** | **1.550.922** | **9 archivos** | |

## Relaciones principales

- `orders.customer_id` → `customers.customer_id`.
- `order_items.order_id` → `orders.order_id`.
- `order_items.product_id` → `products.product_id`.
- `order_items.seller_id` → `sellers.seller_id`.
- `order_payments.order_id` → `orders.order_id`.
- `order_reviews.order_id` → `orders.order_id`.
- `products.product_category_name` → `product_category_name_translation.product_category_name`.
- Los códigos postales de clientes y vendedores pueden asociarse a `geolocation_zip_code_prefix`; esta última fuente tiene múltiples filas por prefijo y debe depurarse antes de unirla para no duplicar datos.

**Nota de modelado:** `customer_id` identifica al cliente asociado a un pedido, mientras que `customer_unique_id` identifica al comprador a través de múltiples pedidos. Para clientes recurrentes debe utilizarse `customer_unique_id`.

## Recorrido de datos

`CSV originales → ADLS Gen2 / raw → catalog_dev.bronze → catalog_dev.silver → catalog_dev.golden → Dashboard`.

Las nueve fuentes se almacenan en el contenedor `raw` y se procesan desde los notebooks de `proceso/`. El conjunto final incluye dimensiones y hechos para análisis de ventas, clientes, productos, vendedores, logística y pagos.

## Reproducibilidad y repositorio

Los archivos íntegros de origen permanecen en ADLS Gen2. Este directorio documenta sus nombres, procedencia, volumen y relaciones para evitar publicar una copia pesada de datos en GitHub. Para obtener los CSV originales, utilizar el enlace público de Kaggle y cargar los nueve archivos al **nivel raíz** del contenedor `raw` (sin subcarpetas), respetando sus nombres.

No publicar claves, tokens, secretos, credenciales ni archivos de configuración privados junto con los datos.
