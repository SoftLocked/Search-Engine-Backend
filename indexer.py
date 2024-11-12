from collections.abc import Iterator
from pathlib import Path
from bs4 import BeautifulSoup
import json
from nltk.stem import PorterStemmer

class Token:
    def __init__(self, tok_str: str) -> None:
        self.tok_str: str = tok_str
    
    def __eq__(self, value: object) -> bool:
        return self.tok_str.lower() == value.tok_str.lower()

    def __repr__(self) -> str:
        return self.tok_str
    
    def __hash__(self) -> int:
        return abs(hash(self.tok_str))



class Page_Json:
    def __init__(self, json_url) -> None:
        with open(json_url, 'r') as in_file:
            json_dict = json.load(in_file)
            self._url = json_dict['url']
            self._content = json_dict['content']
            self._encoding = json_dict['encoding']

    @property
    def url(self) -> str:
        return self._url
    
    @url.setter
    def url(self, new_url: str) -> None:
        print(f'Attempted to change url | {self.url} -> {new_url}')

    @property
    def content(self) -> str:
        return self._content
    
    @content.setter
    def content(self, new_content: str) -> None:
        print(f'Attempted to change content | {self.content} -> {new_content}')

    @property
    def encoding(self) -> str:
        return self._encoding
    
    @encoding.setter
    def encoding(self, new_encoding:str) -> None:
        print(f'Attempted to change encoding | {self.encoding} -> {new_encoding}')

    def get_tokens(self) -> Iterator[Token]:
        page_text = BeautifulSoup(self.content, 'lxml', from_encoding=self.encoding).text
        
        ps = PorterStemmer()

        curr_buff = []
        for i, curr_char in enumerate(page_text):
            if 'a' <= curr_char.lower() <= 'z' or '0' <= curr_char <= '9':
                curr_buff.append(curr_char.lower())
            else:
                if len(curr_buff) > 0:
                    yield Token(ps.stem(''.join(curr_buff)))

                curr_buff = []
                continue
        
        if len(curr_buff) > 0:
            yield Token(ps.stem(''.join(curr_buff)))


class Index:
    def __init__(self, bins:int) -> None:
        self.bins = bins
        for item in Path('index_store/').iterdir():
            if item.is_file():
                if item.name != '.gitignore':
                    item.unlink()
        for i in range(self.bins):
            with open(f'index_store/{i}.json', 'w') as json_file:
                json.dump(dict(), json_file)

    def crawl_json(self) -> None:
        root_path = Path('DEV/')
        for dir in root_path.iterdir():
            if dir.isdir():
                for file in dir.iterdir():
                    pass

    def store_tokens(self, page:Page_Json, tok_stream:Iterator[Token]):
        for token in tok_stream:
            self.store_token(page, token)
        
    
    def store_token(self, page:Page_Json, token:Token) -> None:
        bin = hash(token)%self.bins
        # print(f'{token} : {bin}')
        data = dict()
        with open(f'index_store/{bin}.json', 'r') as json_file:
            data = json.load(json_file)
        

        if token.tok_str not in data:
            data[token.tok_str] = {
                page.url: 1
            }
        else:
            if page.url not in data[token.tok_str]:
                data[token.tok_str][page.url] = 1
            else:
                data[token.tok_str][page.url] += 1

        with open(f'index_store/{bin}.json', 'w') as json_file:
            json.dump(data, json_file)
        
i = Index(10)

page = Page_Json('DEV/aiclub_ics_uci_edu/8ef6d99d9f9264fc84514cdd2e680d35843785310331e1db4bbd06dd2b8eda9b.json')
tok_stream = page.get_tokens()

i.store_tokens(page, tok_stream)