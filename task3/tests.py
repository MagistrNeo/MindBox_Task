import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType

class TestProductsWithCategories:
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Создание Spark сессии для тестов"""
        spark = SparkSession.builder \
            .appName("ProductsCategoriesTest") \
            .master("local[2]") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    @pytest.fixture
    def sample_products(self, spark):
        """Фикстура с тестовыми продуктами"""
        data = [
            (1, "Laptop"),
            (2, "Mouse"), 
            (3, "Keyboard"),
            (4, "Monitor"),
            (5, "Headphones")
        ]
        schema = StructType([
            StructField("product_id", IntegerType(), True),
            StructField("product_name", StringType(), True)
        ])
        return spark.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_categories(self, spark):
        """Фикстура с тестовыми категориями"""
        data = [
            (1, "Electronics"),
            (2, "Peripherals"),
            (3, "Audio"),
            (4, "Gaming")
        ]
        schema = StructType([
            StructField("category_id", IntegerType(), True),
            StructField("category_name", StringType(), True)
        ])
        return spark.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_links(self, spark):
        """Фикстура с тестовыми связями"""
        data = [
            (1, 1),  # Laptop -> Electronics
            (2, 1),  # Mouse -> Electronics
            (2, 2),  # Mouse -> Peripherals
            (3, 1),  # Keyboard -> Electronics
            (3, 2),  # Keyboard -> Peripherals
            (5, 3),  # Headphones -> Audio
            # Product 4 (Monitor) без категорий
            # Product 5 (Headphones) также может быть в Gaming, но связи нет
        ]
        schema = StructType([
            StructField("product_id", IntegerType(), True),
            StructField("category_id", IntegerType(), True)
        ])
        return spark.createDataFrame(data, schema)
    
    def test_basic_functionality(self, spark, sample_products, sample_categories, sample_links):
        """Тест базовой функциональности"""
        result_df = get_products_with_categories(sample_products, sample_categories, sample_links)
        
        result_data = sorted([tuple(row) for row in result_df.collect()])
        
        expected_data = [
            ("Headphones", "Audio"),
            ("Keyboard", "Electronics"),
            ("Keyboard", "Peripherals"), 
            ("Laptop", "Electronics"),
            ("Monitor", None),  # Продукт без категорий
            ("Mouse", "Electronics"),
            ("Mouse", "Peripherals")
        ]
        
        assert result_data == expected_data
    
    def test_empty_categories(self, spark):
        """Тест когда у всех продуктов нет категорий"""
        products_data = [(1, "Product1"), (2, "Product2")]
        products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
        
        categories_data = [(1, "Category1")]
        categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
        
        # Пустые связи
        links_df = spark.createDataFrame([], ["product_id", "category_id"])
        
        result_df = get_products_with_categories(products_df, categories_df, links_df)
        result_data = sorted([tuple(row) for row in result_df.collect()])
        
        expected_data = [("Product1", None), ("Product2", None)]
        assert result_data == expected_data
    
    def test_all_products_have_categories(self, spark):
        """Тест когда у всех продуктов есть категории"""
        products_data = [(1, "Product1"), (2, "Product2")]
        products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
        
        categories_data = [(1, "Category1"), (2, "Category2")]
        categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
        
        links_data = [(1, 1), (2, 2)]
        links_df = spark.createDataFrame(links_data, ["product_id", "category_id"])
        
        result_df = get_products_with_categories(products_df, categories_df, links_df)
        result_data = sorted([tuple(row) for row in result_df.collect()])
        
        expected_data = [("Product1", "Category1"), ("Product2", "Category2")]
        assert result_data == expected_data
    
    def test_empty_datasets(self, spark):
        """Тест с пустыми датафреймами"""
        empty_products = spark.createDataFrame([], ["product_id", "product_name"])
        empty_categories = spark.createDataFrame([], ["category_id", "category_name"]) 
        empty_links = spark.createDataFrame([], ["product_id", "category_id"])
        
        result_df = get_products_with_categories(empty_products, empty_categories, empty_links)
        assert result_df.count() == 0
    
    def test_missing_columns(self, spark):
        """Тест обработки отсутствующих колонок"""
        products_df = spark.createDataFrame([(1, "Test")], ["id", "name"])  # Неправильные имена колонок
        categories_df = spark.createDataFrame([(1, "Cat")], ["category_id", "category_name"])
        links_df = spark.createDataFrame([(1, 1)], ["product_id", "category_id"])
        
        with pytest.raises(ValueError, match="Отсутствует обязательная колонка product_id в products_df"):
            get_products_with_categories(products_df, categories_df, links_df)
    
    def test_multiple_categories_per_product(self, spark):
        """Тест когда у продукта несколько категорий"""
        products_data = [(1, "Smartphone")]
        products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
        
        categories_data = [(1, "Electronics"), (2, "Mobile"), (3, "Gadgets")]
        categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
        
        links_data = [(1, 1), (1, 2), (1, 3)]  # Один продукт в трех категориях
        links_df = spark.createDataFrame(links_data, ["product_id", "category_id"])
        
        result_df = get_products_with_categories(products_df, categories_df, links_df)
        result_data = sorted([tuple(row) for row in result_df.collect()])
        
        expected_data = [
            ("Smartphone", "Electronics"),
            ("Smartphone", "Gadgets"), 
            ("Smartphone", "Mobile")
        ]
        assert result_data == expected_data
    
    def test_duplicate_links(self, spark):
        """Тест обработки дублирующихся связей"""
        products_data = [(1, "Product1")]
        products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
        
        categories_data = [(1, "Category1")]
        categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
        
        # Дублирующиеся связи
        links_data = [(1, 1), (1, 1)]
        links_df = spark.createDataFrame(links_data, ["product_id", "category_id"])
        
        result_df = get_products_with_categories(products_df, categories_df, links_df)
        result_data = [tuple(row) for row in result_df.collect()]
        
        # Дубликаты должны быть удалены
        assert len(result_data) == 1
        assert result_data[0] == ("Product1", "Category1")
