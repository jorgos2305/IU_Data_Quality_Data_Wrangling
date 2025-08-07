from dataclasses import dataclass
import pandas as pd
from typing import List, Dict, Optional

@dataclass
class ClientResult:
    data: Optional[pd.DataFrame]
    metadata: Optional[List[Dict]]
    errors: Optional[List[Dict]]
