import pytest

from ticker.extractor.xlsx_segment_extractor import XlsxSegmentExtractor

@pytest.mark.parametrize(
    ("expected", "path"),
    [
        (455, "tests/resources/SetorialB3.xlsx"),
    ],
)
def test_segment_extract(expected, path):

    extractor = XlsxSegmentExtractor("Plan3")
    data = extractor.extract(path)
    assert expected == len(data.index)
    