import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark_assignment.src.assignment1.util import (only_product_iphone13,
                                iphone13_to_iphone14, product_unique)

class Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestFilterAndJoinData").getOrCreate()
        schema1 = StructType([
            StructField("customer", StringType(), nullable=False),
            StructField("product_model", StringType(), nullable=False)
        ])
        data1 = [
            ("1", "iphone13"),
            ("1", "dell i5 core"),
            ("2", "iphone13"),
            ("2", "dell i5 core"),
            ("3", "iphone13"),
            ("3", "dell i5 core"),
            ("1", "dell i3 core"),
            ("1", "hp i5 core"),
            ("1", "iphone14"),
            ("3", "iphone14"),
            ("4", "iphone13")
        ]
        schema2 = StructType([
            StructField("product_model", StringType(), nullable=False)
        ])

        data2 = [
            ("iphone13",),
            ("dell i5 core",),
            ("dell i3 core",),
            ("hp i5 core",),
            ("iphone14",)
        ]
        # Create DataFrame
        cls.purchase_data_df = cls.spark.createDataFrame(data=data1, schema=schema1)
        cls.product_data_df = cls.spark.createDataFrame(data=data2, schema=schema2)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_only_product_iphone13(self):
        result_df = only_product_iphone13(self.purchase_data_df)
        self.assertTrue(result_df.count() > 0, "Result DataFrame is empty")

    def test_iphone13_to_iphone14(self):
        result_df = iphone13_to_iphone14(self.purchase_data_df)
        self.assertTrue(result_df.count() > 0, "Result DataFrame is empty")

    def test_product_unique(self):
        result_df = product_unique(self.purchase_data_df, self.product_data_df)
        self.assertTrue(result_df.count() > 0, "Result DataFrame is empty")

if __name__ == '__main__':
    unittest.main()
