from collections.abc import Iterator
from pathlib import Path
from bs4 import BeautifulSoup
import json


class Token:
    def __init__(self, tok_str: str) -> None:
        self.tok_str: str = tok_str
    
    def __eq__(self, value: object) -> bool:
        return self.tok_str.lower() == value.tok_str.lower()

    def __repr__(self) -> str:
        return self.tok_str


def parse_json(file_path: Path) -> Iterator[Token]:
    url = ''
    content = ''
    encoding = ''
    with open(file_path, 'r') as in_file:
        return json.load(in_file)




