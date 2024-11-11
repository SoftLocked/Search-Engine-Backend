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

    def __hash__(self) -> int:
        return len(self.tok_str)


class Index:
    def __init__(self) -> None:
        pass

    def parse_json(self, file_path: Path) -> dict[str, str]:
        url = ''
        content = ''
        encoding = ''
        with open(file_path, 'r') as in_file:
            return json.load(in_file)
    
    def get_tokens(self, content: str, encoding: str) -> Iterator[Token]:
        page_text = BeautifulSoup(content, 'lxml', from_encoding=encoding).text
        
        curr_buff = []
        while True:
            curr_char = f.read(1).lower()
            if not curr_char:
                if len(curr_buff) > 0:
                    yield ''.join(curr_buff)
                break
            
            if 'a' <= curr_char.lower() <= 'z' or '0' <= curr_char <= '9':
                curr_buff.append(curr_char)
            else:
                if len(curr_buff) > 0:
                    yield ''.join(curr_buff)
                curr_buff = []
                continue
