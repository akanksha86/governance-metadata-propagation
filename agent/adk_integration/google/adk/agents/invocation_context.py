from dataclasses import dataclass
from typing import Any, Dict

@dataclass
class Session:
    state: Dict[str, Any]

@dataclass
class InvocationContext:
    session: Session
