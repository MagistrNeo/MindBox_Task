from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pytest

def get_products_with_categories(products_df: DataFrame, 
                               categories_df: DataFrame, 
                               product_category_links_df: DataFrame) -> DataFrame:
    """
    Возвращает датафрейм со всеми парами "Имя продукта - Имя категории" 
    и продуктами без категорий.
    
    Args:
        products_df: Датафрейм продуктов с колонками ['product_id', 'product_name']
        categories_df: Датафрейм категорий с колонками ['category_id', 'category_name']
        product_category_links_df: Датафрейм связей с колонками ['product_id', 'category_id']
    
    Returns:
        Датафрейм с колонками ['product_name', 'category_name']
    """
    
    # Проверяем наличие необходимых колонок
    required_product_cols = ['product_id', 'product_name']
    required_category_cols = ['category_id', 'category_name']
    required_link_cols = ['product_id', 'category_id']
    
    for col_name in required_product_cols:
        if col_name not in products_df.columns:
            raise ValueError(f"Отсутствует обязательная колонка {col_name} в products_df")
    
    for col_name in required_category_cols:
        if col_name not in categories_df.columns:
            raise ValueError(f"Отсутствует обязательная колонка {col_name} в categories_df")
    
    for col_name in required_link_cols:
        if col_name not in product_category_links_df.columns:
            raise ValueError(f"Отсутствует обязательная колонка {col_name} в product_category_links_df")
    
    # 1. Получаем все пары "Продукт - Категория"
    product_category_pairs = (
        product_category_links_df
        .join(products_df, "product_id")
        .join(categories_df, "category_id")
        .select("product_name", "category_name")
    )
    
    # 2. Находим продукты без категорий
    products_without_categories = (
        products_df
        .join(product_category_links_df, "product_id", "left_anti")
        .select(
            col("product_name"), 
            lit(None).cast(StringType()).alias("category_name")
        )
    )
    
    # 3. Объединяем результаты
    result_df = product_category_pairs.union(products_without_categories)
    
    return result_df.orderBy("product_name", "category_name")