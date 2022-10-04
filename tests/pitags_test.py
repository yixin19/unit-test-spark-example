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
    pitags_job = PitagsJob()

    # WHEN
    pitags_job.transform(pitags_df=None)

    # THEN
    assert True
