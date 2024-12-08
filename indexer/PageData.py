from bs4 import BeautifulSoup
import json
from time import sleep

from indexer.Token import Token



class PageData:
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

    def get_tokens(self) -> list[Token]:
        soup =  BeautifulSoup(self.content, 'lxml', from_encoding=self.encoding)
        page_text = soup.get_text()

        # print(page_text)

        token_freq = dict()

        curr_buff = []
        for i, curr_char in enumerate(page_text):
            if 'a' <= curr_char <= 'z' or 'A' <= curr_char <= 'Z' or '0' <= curr_char <= '9':
                curr_buff.append(curr_char)
            else:
                if len(curr_buff) > 0:
                    tok = Token(''.join(curr_buff))
                    if tok not in token_freq:
                        token_freq[tok] = [1,0,0]
                    else:
                        token_freq[tok][0] += 1

                curr_buff = []
                continue
        
        if len(curr_buff) > 0:
            tok = Token(''.join(curr_buff))
            if tok not in token_freq:
                token_freq[tok] = [1,0,0]
            else:
                token_freq[tok][0] += 1

        for tag in soup.find_all(["b", "h1", "h2", "h3", "title"]):
            words = tag.text.strip().split()
            for word in words:
                tok = Token(word)
                if tok in token_freq:
                    token_freq[tok][1] += 1

        for tag in soup.find_all(["title"]):
            words = tag.text.strip().split()
            for word in words:
                tok = Token(word)
                if tok in token_freq:
                    token_freq[tok][2] += 1




        for token in token_freq:
            token_freq[token][0] = token_freq[token][0] - token_freq[token][1] - token_freq[token][2]

        return token_freq.items()