from .config import LogReferences, EKGReferences

class Knowledge:
    
    log:LogReferences | None
    ekg: EKGReferences | None
    
    def __init__(self):
        self.log = None
        self.ekg = None
        
knowledge = Knowledge()


