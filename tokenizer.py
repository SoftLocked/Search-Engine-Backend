from collections.abc import Iterator
from pathlib import Path
import bs4



class Token:
    def __init__(self, tok_str: str) -> None:
        self.tok_str: str = tok_str
    
    def __eq__(self, value: object) -> bool:
        return self.tok_str.lower() == value.tok_str.lower()

    def __repr__(self) -> str:
        return self.tok_str
