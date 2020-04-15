import unittest
from pyspark.sql import SparkSession 
from main import id_relationship_fact, dedup_regroup, format_output

class PySparkTest(unittest.TestCase):
    @classmethod
    def create_testing_pyspark_session(cls):
        spark = SparkSession \
                    .builder \
                    .master("local[3]")\
                    .appName("FullContact_JingScribner_Test_Main_Functions") \
                    .config("spark.ui.enabled", "true") \
                    .getOrCreate()
        return spark

    @classmethod
    def setUpClass(cls):
        cls.spark = cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

class TestMain(PySparkTest):
    def test_id_relationship_fact(self):
        test_queries = [
            ('HLLCCNX', ),
            ('LMLHENN', ),
            ('OOFDUJC', ),
            ('FSNMWAF', )
            ]
        test_df_a = self.spark.createDataFrame(data=test_queries, schema = ['identity_id'])
        test_records = [
            ('HLLCCNX',["HLLCCNX", "ZVREMGG"]),
            ('HLLCCNX',["HLLCCNX", "ZVREMGG"]),
            ('WONIWXW',["WONIWXW", "LMLHENN", "QULQKGC"]),
            ('LMLHENN',["WONIWXW", "LMLHENN", "QULQKGC"]),
            ('QULQKGC',["WONIWXW", "LMLHENN", "QULQKGC"]),
            ('MEKQMTV',["MEKQMTV", "HEVOKKP"]),
            ('HEVOKKP',["MEKQMTV", "HEVOKKP"])
            ]
        test_df_b = self.spark.createDataFrame(data=test_records, schema = ['identity_id','group_ids'])
        results = id_relationship_fact(test_df_a, test_df_b)
        expected_results = [
            ('HLLCCNX',["HLLCCNX", "ZVREMGG"]),
            ('HLLCCNX',["HLLCCNX", "ZVREMGG"]), 
            ('LMLHENN',["WONIWXW", "LMLHENN", "QULQKGC"])        
        ]
        expected_results = self.spark.createDataFrame(data=expected_results, schema = ['identity_id','group_ids'])
        self.assertEqual(set(results), set(expected_results))

    def test_dedup_regroup(self):
        test_df_rel = [
            ('HLLCCNX', 'HLLCCNX'),
            ('HLLCCNX', 'WKNDMRK'),
            ('HLLCCNX', 'GQDANUQ'),
            ('HLLCCNX', 'HLLCCNX'),
            ('HLLCCNX', 'CACQDHT')
            ]
        test_df_rel = self.spark.createDataFrame(data=test_df_rel, schema = ['identity_id','identity_id_dedup'])
        results = dedup_regroup(test_df_rel)
        expected_results = [
            ('HLLCCNX',["HLLCCNX", "WKNDMRK","GQDANUQ","CACQDHT"]) 
        ]
        self.assertEqual(set(results), set(expected_results))

    def test_format_output(self):
        pass

if __name__ == '__main__':
    unittest.main()
