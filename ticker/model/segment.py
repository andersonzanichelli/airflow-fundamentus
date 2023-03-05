class Segment:
    
    def __init__(self, sector, subsector, segment, code, segcode) -> None:
        self.sector = sector
        self.subsector = subsector
        self.segment = segment
        self.code = code
        self.segcode = segcode
    
    def asdict(self):
        return (
            {
                "SETOR ECONÔMICO": self.sector,
                "SUBSETOR": self.subsector,
                "SEGMENTO": self.segment,
                "Papel": self.code,
                "cod": self.segcode
            }
        )

    def __str__(self) -> str:
        return (f"{self.sector} | {self.subsector} | {self.segment} | {self.code} | {self.segcode}")
    

class SegmentBuilder:

    def __init__(self) -> None:
        self.sector = None
        self.subsector = None
        self.segment = None
        self.code = None
        self.segcode = None

    def set_sector(self, sector):
        self.sector = sector
        return self
    
    def set_subsector(self, subsector):
        self.subsector = subsector
        return self
    
    def set_segment(self, segment):
        self.segment = segment
        return self
    
    def set_code(self, code):
        self.code = code
        return self
    
    def set_segcode(self, segcode):
        self.segcode = segcode
        return self

    def build(self) -> Segment:
        segment = Segment(sector=self.sector, subsector=self.subsector, segment=self.segment, code=self.code, segcode=self.segcode)
        return segment