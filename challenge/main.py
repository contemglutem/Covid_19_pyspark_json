import pathlib
from datetime import date

from pyspark.sql import SparkSession
from urllib.request import urlopen

import challenge.constants as constants
from challenge.process_control import ProcessControl


class Base:

    def __init__(self):
        self.__process_control = ProcessControl()

    def main(self):
        spark = self.spark_session()
        self.run(spark)

    def run(self, spark: SparkSession) -> None:

        last_sequential_number = self.__process_control.get_last_sequential_number()[0][0] + 1
        last_process_date = self.__process_control.get_last_process_date()[0][0]
        if last_process_date < date.today():
            self.__process_control.update_process_parameters(str(date.today()), last_sequential_number)
            countries_df = self.reader_csv(spark)
            self.writer(countries_df, 'countries_data')
            covid_df = self.reader_json(spark)
            self.writer(covid_df, 'covid_data')
            daily_data = self.reader_csv(spark, self.reader_path_daily_cases())
            self.writer(daily_data, 'daily_data')

        else:
            print("File already processed")

    @staticmethod
    def reader_path_countries_data():
        path = str(pathlib.Path(__file__).parent.resolve()) + '\\countries_data\\countries of the world.csv'
        return path

    @staticmethod
    def reader_path_covid_data():
        url = "https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/json/"

        return url

    @staticmethod
    def reader_path_daily_cases():
        path = str(pathlib.Path(__file__).parent.resolve()) + '\\daily_data\\data.csv'
        return path

    @staticmethod
    def reader_csv(spark, path):
        df = spark.read.option("header", True).csv(path)

        return df

    def reader_json(self, spark):
        jsonData = urlopen(self.reader_path_covid_data()).read().decode('utf-8')
        rdd = spark.sparkContext.parallelize([jsonData])
        df = spark.read.json(rdd)

        return df

    @staticmethod
    def spark_session() -> SparkSession:
        spark = SparkSession.builder.master("local[*]") \
            .appName("SparkSessionApp") \
            .getOrCreate()

        return spark

    @staticmethod
    def writer(data_frame, table_name):
        mode = "overwrite"
        url = f"jdbc:postgresql://{constants.hostname}:{constants.port_id}/{constants.database}"
        properties = {"user": constants.username, "password": constants.pwd, "driver": constants.driver}
        data_frame.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)


if __name__ == '__main__':
    Base().main()
