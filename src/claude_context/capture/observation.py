from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Observation:
    service_name: str
    caller: str
    method: str
    path_template: str
    request_fields: frozenset[str]
    request_headers: frozenset[str]
    query_params: frozenset[str]
    status_code: int
    timestamp: datetime


@dataclass
class AggregatedObservation:
    service_name: str
    caller: str
    method: str
    path_template: str
    request_fields: set[str] = field(default_factory=set)
    request_headers: set[str] = field(default_factory=set)
    query_params: set[str] = field(default_factory=set)
    response_codes: set[str] = field(default_factory=set)
    call_count: int = 0
    first_seen: datetime = field(default_factory=datetime.utcnow)
    last_seen: datetime = field(default_factory=datetime.utcnow)
