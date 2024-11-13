from collections.abc import Iterator
from pathlib import Path
from bs4 import BeautifulSoup
import json
import shutil
from nltk.stem import PorterStemmer
import time
from datetime import datetime, timedelta

class Token:
    def __init__(self, tok_str: str) -> None:
        self.tok_str: str = tok_str
    
    def __eq__(self, value: object) -> bool:
        return self.tok_str.lower() == value.tok_str.lower()

    def __repr__(self) -> str:
        return self.tok_str
    
    def __hash__(self) -> int:
        return abs(hash(self.tok_str.lower()))



class Page_Data:
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

        token_freq = dict()

        curr_buff = []
        for i, curr_char in enumerate(page_text):
            if 'a' <= curr_char.lower() <= 'z' or '0' <= curr_char <= '9':
                curr_buff.append(curr_char.lower())
            else:
                if len(curr_buff) > 0:
                    tok = Token(ps.stem(''.join(curr_buff)))
                    if tok not in token_freq:
                        token_freq[tok] = 1
                    else:
                        token_freq[tok] += 1

                curr_buff = []
                continue
        
        if len(curr_buff) > 0:
            tok = Token(ps.stem(''.join(curr_buff)))
            if tok not in token_freq:
                token_freq[tok] = 1
            else:
                token_freq[tok] += 1
        
        for i in token_freq.items():
            yield i


class Index:
    def __init__(self, bins:int) -> None:
        self.num_docs = 0
        self.bins = bins
        for item in Path('index_store/').iterdir():
            if item.is_file():
                if item.name != '.gitignore':
                    item.unlink()
        for i in range(self.bins):
            # Path(f'index_store/{i}.json').touch()
            with open(f'index_store/{i}.json', 'w') as out_file:
                json.dump({}, out_file)

    def crawl_json(self) -> None:

        # cumulative_time_per_iter = 0


        root_path = Path('DEV/')

        page_count = len(list(root_path.rglob("*.json")))

        start_time = time.time()
        
        temp_iter = 0
        iter = 0
        for i, dir in enumerate(root_path.iterdir()):
            if dir.is_dir():
                
                
                for j, file in enumerate(dir.iterdir()):
                    
                    if iter % 500 == 0:
                        start_time = time.time()
                        temp_iter = 0
                    

                    if file.is_file() and file.suffix == '.json':
                        page = Page_Data(file)
                        self.store_tokens(page, page.get_tokens())

                    
                    #if elapsed_time <= 3 * cumulative_time_per_iter/(iter + 1):
                        #cumulative_time_per_iter += elapsed_time
                    #elif cumulative_time_per_iter == 0:
                        #cumulative_time_per_iter += elapsed_time
                
                    #avg = cumulative_time_per_iter/(iter + 1)
                    

                    #print(elapsed_time, iter+1, eta, etat)

                    self.num_docs += 1

                    if ((iter+1) % 50 == 0):
                        curr_time = time.time()
                        elapsed_time = curr_time - start_time
                        eta = (elapsed_time)/(temp_iter+1) * (page_count - (iter + 1))
                        eta = str(timedelta(seconds=eta))
                        with open('report.txt', 'w') as out_file:
                            out_file.write(f'Number of Documents Indexed: {self.num_docs}\n')
                        perc = f'{100*(iter)/page_count:.4f} %'
                        print(f'{datetime.now().strftime("%H:%M:%S")} | Processing File {iter + 1:<5} of {page_count} | {perc:<10} | eta: {eta:<25} | {file}')

                    temp_iter += 1
                    iter += 1
        
        with open('report.txt') as out_file:
            out_file.write(f'Number of Documents Indexed: {self.num_docs}\n')
        print('Indexing Compete!')

    def store_tokens(self, page:Page_Data, tok_stream:Iterator[tuple[Token,int]]):
        for token, freq in tok_stream:
            self.store_token(page, token, freq)
        
    
    def store_token(self, page:Page_Data, token:Token, freq:int) -> None:
        bin = hash(token)%self.bins
        # shutil.copy(f'index_store/{bin}.json', 'index_store/temp.txt')

        data = dict()

        with open(f'index_store/{bin}.json', 'r') as in_file:
            data = json.load(in_file)

        if token.tok_str not in data:
            data[token.tok_str] = dict()
            data[token.tok_str][page.url] = freq
        elif page.url not in data[token.tok_str]:
            data[token.tok_str][page.url] = freq

        with open(f'index_store/{bin}.json', 'w') as out_file:
            json.dump(data, out_file, indent=4)

        '''
        with open('index_store/temp.txt', 'r') as in_file:
            with open(f'index_store/{bin}.txt', 'w') as out_file:
                found_flg = False
                for line in in_file:
                    curr_tok = line[:line.find(':')]
                    values = json.loads(line[line.find(':')+1:].replace("'", '"'))

                    if curr_tok == token.tok_str and not found_flg:
                        found_flg = True
                        value_flg = False
                        for value in values:
                            if value[0] == page.url and not value_flg:
                                value_flg = True
                                value[1] += 1
                                out_file.write(f'{token.tok_str}:{values}\n')
                        if not value_flg:
                            values.append([page.url, 1])
                            out_file.write(f'{token.tok_str}:{values}\n')
                    else:
                        out_file.write(line)
                if not found_flg:
                    out_file.write(f'{token.tok_str}:{[[page.url, 1]]}\n')
        '''

