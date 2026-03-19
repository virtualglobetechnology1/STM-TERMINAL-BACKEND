# app/schemas/search_schema.py

from pydantic import BaseModel
from typing import Optional, List

class SearchRequest(BaseModel):
    ticker_name: str
    exchange: Optional[str] = None
    page: int = 1
    page_size: int = 10

class SearchResult(BaseModel):
    ticker_id:     int
    ticker:        str
    exchange:      str
    issuer_name:   Optional[str]
    isin:          Optional[str]
    status:        Optional[str]

class PaginationMeta(BaseModel):
    total:       int
    page:        int
    page_size:   int
    total_pages: int
    has_next:    bool
    has_prev:    bool

class SearchResponse(BaseModel):
    status:      bool
    message:     str
    source:      str
    pagination:  PaginationMeta
    data:        List[SearchResult]
    suggestions: List[str]