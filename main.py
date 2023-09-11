from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.types import *

spark: SparkSession = SparkSession.builder.getOrCreate()

productDataFrame = spark.createDataFrame([
    Row(id=0, name="Bread"),
    Row(id=1, name="Cheese"),
    Row(id=2, name="Chicken"),
    Row(id=3, name="Water"),
    Row(id=4, name="Daewoo Nexia"),
    Row(id=5, name="Sprite"),
    Row(id=6, name="Milk"),
    Row(id=7, name="Pasta"),
    Row(id=8, name="Rice"),
    Row(id=9, name="Cola")
])

categoryDataFrame = spark.createDataFrame([
    Row(id=0, name="Wheat"),
    Row(id=1, name="Liquid"),
    Row(id=2, name="Carbonated"),
    Row(id=3, name="Meat"),
    Row(id=4, name="Porridge"),
    Row(id=5, name="With Sugar"),
    Row(id=6, name="Milk Product"),
    Row(id=7, name="Green Product")
])

relationsDataFrame = spark.createDataFrame([
    Row(productId=0, categoryId=0),
    Row(productId=0, categoryId=7),
    Row(productId=1, categoryId=6),
    Row(productId=1, categoryId=7),
    Row(productId=2, categoryId=3),
    Row(productId=3, categoryId=1),
    Row(productId=5, categoryId=1),
    Row(productId=5, categoryId=5),
    Row(productId=5, categoryId=2),
    Row(productId=6, categoryId=1),
    Row(productId=6, categoryId=6),
    Row(productId=7, categoryId=0),
    Row(productId=8, categoryId=4),
    Row(productId=8, categoryId=7),
    Row(productId=9, categoryId=1),
    Row(productId=9, categoryId=2),
    Row(productId=9, categoryId=5)
])

def getJoinedProductsAndCategories(products: DataFrame, relations: DataFrame, categories: DataFrame):
    return products.join(
        relations, products.id == relations.productId, 'outer'
        ).join(
            categories, categories.id == relations.categoryId, 'outer'
        ).select(
            products.name.alias('Product'), categories.name.alias('Category')
        )

result = getJoinedProductsAndCategories(products=productDataFrame, relations=relationsDataFrame, categories=categoryDataFrame)
result.show()