from pyspark.sql import DataFrame

from jobs.pitags_job import PitagsJob


def test_extract():
    # GIVEN
    pitags_job = PitagsJob()

    # WHEN
    got_pitags_df = pitags_job.extract()

    # THEN
    got_pitags_df.show()
    assert True


def test_transform():
    # GIVEN
    # given_datetime = [{"datetime": convert_string_to_datetime("2022-04-28 21:54:05.605010")}]
    # date_time_df = spark.createDataFrame(data=given_datetime, schema=PitagsWriterWorker.PITAGS_FILE_SCHEMA)
    pitags_job = PitagsJob()
    given_pitags_data = [{"Tags": "I_302TK4404_1:T4404.", "date": "2012-01-01"},
                         {"Tags": "I_302T.K4404_1:T4403.", "date": "2012-01-02"}]
    given_df = pitags_job.spark.createDataFrame(data=given_pitags_data)

    # WHEN
    expected_pitags_data = [{"Tags": "I_302TK4404_1:T4404", "date": "2012-01-01"},
                            {"Tags": "I_302T.K4404_1:T4403", "date": "2012-01-02"}]
    expected_df = pitags_job.spark.createDataFrame(data=expected_pitags_data)
    result_df = pitags_job.transform(pitags_df=given_df)

    # THEN
    assert convert_df_to_dict(result_df) == convert_df_to_dict(expected_df)


def test_remove_point_at_last_position():
    # GIVEN
    pitags_job = PitagsJob()
    given_pitags_data = [{"Tags": "I_302TK4404_1:T4404.", "date": "2012-01-01"},
                         {"Tags": "I_302T.K4404_1:T4403.", "date": "2012-01-02"}]
    given_pitags_df = pitags_job.spark.createDataFrame(data=given_pitags_data)
    expected_pitags_data = [{"Tags": "I_302TK4404_1:T4404", "date": "2012-01-01"},
                            {"Tags": "I_302T.K4404_1:T4403", "date": "2012-01-02"}]
    expected_df = pitags_job.spark.createDataFrame(data=expected_pitags_data)

    # WHEN
    got_df = given_pitags_df.withColumn("Tags", pitags_job.remove_point_at_last_position("Tags"))

    # THEN
    assert convert_df_to_dict(got_df) == convert_df_to_dict(expected_df)


def convert_df_to_dict(df_to_convert: DataFrame) -> list[dict]:
    return df_to_convert.rdd.map(lambda row: row.asDict()).collect()
