from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType

def example_usage():
    """Пример использования метода"""
    spark = SparkSession.builder \
        .appName("ProductsCategoriesExample") \
        .master("local[2]") \
        .getOrCreate()
    
    # Создаем тестовые данные
    products_data = [
        (1, "MacBook Pro"),
        (2, "Wireless Mouse"), 
        (3, "Mechanical Keyboard"),
        (4, "USB Cable"),
        (5, "Gaming Chair")
    ]
    products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
    
    categories_data = [
        (1, "Laptops"),
        (2, "Accessories"),
        (3, "Electronics"),
        (4, "Gaming")
    ]
    categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
    
    links_data = [
        (1, 1),  # MacBook Pro -> Laptops
        (1, 3),  # MacBook Pro -> Electronics
        (2, 2),  # Wireless Mouse -> Accessories
        (2, 3),  # Wireless Mouse -> Electronics
        (3, 2),  # Mechanical Keyboard -> Accessories
        (3, 3),  # Mechanical Keyboard -> Electronics
        (5, 4),  # Gaming Chair -> Gaming
        # USB Cable без категорий
    ]
    links_df = spark.createDataFrame(links_data, ["product_id", "category_id"])
    
    # Используем наш метод
    result_df = get_products_with_categories(products_df, categories_df, links_df)
    
    print("Результат:")
    result_df.show()
    
    spark.stop()

if __name__ == "__main__":
    example_usage()
