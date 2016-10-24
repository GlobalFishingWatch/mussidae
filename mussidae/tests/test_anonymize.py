import mussidae.time_range_tools.anonymize as anonymize

def test_anonymize():
    assert anonymize.mmsi_to_id(54321, "this is my salt") == 603341044276
    assert anonymize.mmsi_to_id(54321, "a different salt") == 167929438223817
    assert anonymize.mmsi_to_id(12345, "a different salt") == 65861318181684
