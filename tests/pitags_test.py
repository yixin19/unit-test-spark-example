from jobs.pitags_job import PitagsJob


def test_transform():
    # GIVEN
    pitags_job = PitagsJob()

    # WHEN
    pitags_job.transform(pitags_df=None)

    # THEN
    assert True