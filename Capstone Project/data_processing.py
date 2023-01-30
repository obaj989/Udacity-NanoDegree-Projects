from configparser import ConfigParser
from os import environ

from pandas import to_timedelta, Timestamp, read_csv
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.functions import udf, col, lit, year, month, upper, to_date, monotonically_increasing_id


def create_spark_session():
    spark = SparkSession.builder \
        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport().getOrCreate()

    print('spark session created')
    return spark

convert_to_datetime = udf(lambda date: to_timedelta(date, unit='D') + Timestamp('1960-1-1') if date else None,
                              DateType())


def parse_i94_immigration_data(spark, output_data_path):
    """
    Parse I94 Immigration data to extract data for immigration_info, immigration_personal
    and immigration_airline tables.
        Arguments:
            spark: The SparkSession object that will be used to process the data
            output_data_path: The Target S3 endpoint where the data will be stored after processing
        Returns:
            None
    """

    print('Working on immigration data.......')
    immigration_data_path = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    df = spark.read.format('com.github.saurfang.sas.spark').load(immigration_data_path)
    df.createOrReplaceTempView('immigration_data')

    immigration_info = spark.sql("""
            SELECT DISTINCT cicid AS cic_id,
                            i94yr AS year, 
                            i94mon  AS month, 
                            i94port AS city_code,
                            i94addr AS state_code,
                            arrdate AS arrive_date, 
                            depdate  AS departure_date, 
                            i94mode AS mode,
                            i94visa AS visa
            FROM immigration_data
        """)
    immigration_info = immigration_info.withColumn("immigration_id", monotonically_increasing_id())

    # adding country info for readability
    immigration_info = immigration_info.withColumn('country', lit('United States'))

    # changing the date type columns to have datetime values
    immigration_info = immigration_info.withColumn('arrive_date', convert_to_datetime(col('arrive_date')))
    immigration_info = immigration_info.withColumn('departure_date', convert_to_datetime(col('departure_date')))

    print('Table immigration_info created')

    immigration_personal = spark.sql("""
                SELECT  DISTINCT cicid AS cic_id, 
                                 i94cit AS citizen_country, 
                                 i94res  AS residence_country, 
                                 biryear AS birth_year,
                                 gender,
                                 insnum AS ins_num
                FROM immigration_data
            """)
    immigration_personal = immigration_personal.withColumn("personal_id", monotonically_increasing_id())

    print('Table immigration_personal created')

    immigration_airline = spark.sql("""
                    SELECT  DISTINCT cicid AS cic_id, 
                                     airline, 
                                     admnum  AS admin_num, 
                                     fltno AS flight_number,
                                     visatype AS visa_type
                    FROM immigration_data
                """)
    immigration_airline = immigration_airline.withColumn("airline_id", monotonically_increasing_id())

    print('Table immigration_airline created')

    # writing immigration_info table to S3 in parquet files after partitioning by state
    print('Writing immigration_info table to AWS S3.......')
    immigration_info.write.mode("ignore").partitionBy('state_code').parquet(output_data_path + 'immigration_info')

    # writing immigration_personal table to S3 in parquet files
    print('Writing immigration_personal table to AWS S3.......')
    immigration_personal.write.mode("ignore").parquet(output_data_path + 'immigration_personal')

    # writing immigration_airline table to S3 in parquet files
    print('Writing immigration_airline table to AWS S3.......')
    immigration_airline.write.mode("ignore").parquet(output_data_path + 'immigration_airline')


def parse_immigration_labels(spark, output_data_path):
    """
    Parse the immigration labels provided to extract codes for city, country and state to store in city_code,
    state_code and country_code table
        Arguments:
            spark: The SparkSession object that will be used to process the data
            output_data_path: The Target S3 endpoint where the data will be stored after processing
        Returns:
            None
    """

    with open("I94_SAS_Labels_Descriptions.SAS") as f:
        contents = f.readlines()

    country_code = {}
    for countries in contents[10:298]:
        pair = countries.split('=')
        code, country = pair[0].strip(), pair[1].strip().strip("'")
        country_code[code] = country
    df_country_codes = spark.createDataFrame(country_code.items(), ['code', 'country'])

    city_code = {}
    for cities in contents[303:962]:
        pair = cities.split('=')
        code, city = pair[0].strip("\t").strip().strip("'"), pair[1].strip('\t').strip().strip("''")
        city_code[code] = city
    df_city_codes = spark.createDataFrame(city_code.items(), ['code', 'city'])
    state_code = {}
    for states in contents[982:1036]:
        pair = states.split('=')
        code, state = pair[0].strip('\t').strip("'"), pair[1].strip().strip("'")
        state_code[code] = state

    df_state_codes = spark.createDataFrame(state_code.items(), ['code', 'state'])

    print('Labels file processed. City, state and country code tables are created.')

    # writing state_codes table to S3 in parquet files
    print('Writing state_codes table to AWS S3.......')
    df_state_codes.write.mode("ignore").parquet(output_data_path + 'state_code')

    # writing country_codes table to S3 in parquet files
    print('Writing country_codes table to AWS S3.......')
    df_country_codes.write.mode("ignore").parquet(output_data_path + 'country_code')

    # writing city_codes table to S3 in parquet files
    print('Writing city_codes table to AWS S3.......')
    df_city_codes.write.mode("ignore").parquet(output_data_path + 'city_code')


def parse_world_temperature_data(spark, output_data_path):
    """
    Parse the world temperature data to extract data for temperature table
        Arguments:
            spark: The SparkSession object that will be used to process the data
            output_data_path: The Target S3 endpoint where the data will be stored after processing
        Returns:
            None
    """
    print('Working on temperature data.......')
    df = spark.read.csv('../../data2/GlobalLandTemperaturesByCity.csv', header=True)
    df.createOrReplaceTempView("temp_data")

    temperature = spark.sql("""
        SELECT DISTINCT dt, 
                        AverageTemperature AS avg_temp, 
                        AverageTemperatureUncertainty  AS avg_temp_uncertainity, 
                        City AS city,
                        Country AS country
        FROM temp_data
        WHERE country == 'United States'
    """)
    temperature = temperature.withColumn("temperature_id", monotonically_increasing_id())
    temperature = temperature.withColumn('dt', to_date(col('dt')))
    temperature = temperature.withColumn('year', year(temperature['dt']))
    temperature = temperature.withColumn('month', month(temperature['dt']))

    print('Table temperature created')

    # write temperature table to parquet files
    print('Writing temperature table to AWS S3.......')
    temperature.write.mode("ignore").parquet(output_data_path + 'temperature')


def parse_demographics_data(spark, output_data_path):
    """
    Parse the US city demographics data to extract data for demographics table
        Arguments:
            spark: The SparkSession object that will be used to process the data
            output_data_path: The Target S3 endpoint where the data will be stored after processing
        Returns:
            None
    """
    print('Working on demographics data.......')
    df = spark.read.format('csv').options(header=True, delimiter=';').load('us-cities-demographics.csv')
    df.createOrReplaceTempView('demographics')

    demographics = spark.sql("""
            SELECT DISTINCT UPPER(City) AS city, 
                            UPPER(State) AS state, 
                            "Median Age"  AS median_age, 
                            "Average Household Size" AS avg_household_size, 
                            "Male Population"  AS male_population, 
                            "Female Population" AS female_population,
                            "Number of Veterans" AS num_vetarans,
                            "Foreign-born" AS foreign_born,
                            Race AS race
            FROM demographics
        """)
    demographics = demographics.withColumn("demographic_id", monotonically_increasing_id())

    print('Table demographics created')
    # write demographics table to parquet files
    print('Writing demographics table to AWS S3.......')
    demographics.write.mode("ignore").parquet(output_data_path + 'demographics')


def null_check(spark, output_data_path):
    """
    Performing empty/completeness check on all table stored in S3 and check if there are rows present.
        Arguments:
            spark: The SparkSession object that will be used to process the data
            output_data_path: The Target S3 endpoint where the data was stored after processing
        Returns:
            None if checks pass.
            ValueError if the check fails
    """
    files = ['city_code', 'country_code', 'state_code', 'temperature', 'demographics', 'immigration_airline', 'immigration_personal', 'immigration_info' ]
    for file_dir in files:
        record_num = spark.read.parquet(f'{output_data_path}{file_dir}').count()
        if record_num <= 0:
            raise ValueError(f"Records are not inserted properly. {file_dir} is empty")
        else:
            print(f'Data quality check passed for {file_dir}. Rows found: {record_num}')


def integrity_check(spark, output_data_path):
    """
    Performing data type check for all dates store in the immigration_info table.
        Arguments:
            spark: The SparkSession object that will be used to process the data
            output_data_path: The Target S3 endpoint where the data was stored after processing
        Returns:
            None if checks pass.
            ValueError if the check fails
    """
    immigration_info = spark.read.parquet(output_data_path + "immigration_info")
    
    if type(immigration_info.schema['arrive_date'].dataType) == DateType and \
        type(immigration_info.schema['departure_date'].dataType) == DateType:

    print(f'Integrity check passed for immigration_info table.')

    else:
        raise ValueError(f"Records are not inserted in the right format in immigration_info table.")


def main():
    """
    Parse the configurations, define the user defined function for date processing, call functions to create
    all tables and then perform data quality checks
        Arguments:
            None
        Returns:
            None
    """
    config = ConfigParser()
    config.read('AWS.cfg', encoding='utf-8-sig')

    environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
    output_data_path = config['S3']['DEST_S3_BUCKET']

    spark = create_spark_session()
    parse_i94_immigration_data(spark, output_data_path)
    parse_immigration_labels(spark, output_data_path)
    parse_world_temperature_data(spark, output_data_path)
    parse_demographics_data(spark, output_data_path)
    print("Data processing completed")

    print("Performing null check on the data")
    null_check(spark, output_data_path)

    print("Performing integrity check on the data")
    integrity_check(spark, output_data_path)


if __name__ == "__main__":
    main()
