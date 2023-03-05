from pandas import DataFrame

from ticker.extractor.xlsx_segment_extractor import XlsxSegmentExtractor

def extract(sheet, filepath) -> DataFrame:
    extractor = XlsxSegmentExtractor(sheet)
    return extractor.extract(filepath)