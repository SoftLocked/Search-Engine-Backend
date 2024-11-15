from pathlib import Path
from bs4 import BeautifulSoup
import json
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
import time
from datetime import datetime, timedelta
from multiprocessing.dummy import Pool
import sys

ps = PorterStemmer()


def read_files(file):
    # print(f"Processing File | {file}" + (" "*25) + "\r", end='')
    page = Page_Data(file)
    return (page, page.get_tokens())

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

    def get_tokens(self) -> list[Token]:
        page_text = BeautifulSoup(self.content, 'lxml', from_encoding=self.encoding).text

        token_freq = dict()

        curr_buff = []
        for i, curr_char in enumerate(page_text):
            if 'a' <= curr_char <= 'z' or 'A' <= curr_char <= 'Z' or '0' <= curr_char <= '9':
                curr_buff.append(curr_char)
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
        
        return token_freq.items()


class Index:
    def __init__(self, offload) -> None:
        self.offload = offload
        self.current_index = dict()
        for item in Path('index_store/').iterdir():
            if item.is_file():
                if item.name != '.gitignore':
                    item.unlink()

    def crawl_json(self) -> None:

        # cumulative_time_per_iter = 0


        root_path = Path('DEV/')

        pages = list(root_path.rglob("*.json"))

        page_count = len(list(pages))

        start_time = time.time()

        for i in range(0, page_count, self.offload):
            iter = i + self.offload
            batch = i//self.offload + 1
            batch_size = min(self.offload, page_count-i)

            print(f"Batch {batch}: Processing Batch of {batch_size} Files...")

            p = Pool()
            token_group = p.map(read_files, pages[i:i+self.offload])

            file_process = 1

            #print()
            print(f"Batch {batch}: Finished Processing Batch!")
            print(f"Batch {batch}: Adding Batch to Index...")

            for page,tokens in token_group:
                self.store_tokens(page, tokens)

            print(f"Batch {batch}: Finished Adding!")
            print(f"Batch {batch}: Writing batch")

            with open(f'index_store/{i + batch_size}_pages_checkpoint.json', 'w') as out_file:
                #print(iter)
                #print(self.current_index)
                json.dump(self.current_index, out_file, indent=4)
                self.current_index = dict()

            print(f"Batch {batch}: Finished Writing!")

            print("Took: " + str(timedelta(seconds=time.time()-start_time)))
            start_time = time.time()
            
            #curr_time = time.time()
            #elapsed_time = curr_time - start_time
            #eta = (elapsed_time)/(self.offload) * (page_count - (iter))
            #eta = str(timedelta(seconds=eta))
            #with open('report.txt', 'w') as out_file:
                #out_file.write(f'Number of Documents Indexed: {num_docs}\n')
            #perc = f'{100*(i)/page_count:.4f} %'
            #print(f'{datetime.now().strftime("%H:%M:%S")} | Processed File {i+self.offload:<5} of {page_count} | {perc:<10} | eta {eta:<10}')
            
            #
            #temp_iter = 0

        #with open('report.txt', 'w') as out_file:
            #out_file.write(f'Number of Documents Indexed: {num_docs}\n')

        print(f"Finished Processing! {page_count} Pages Processed.")

    def store_tokens(self, page:Page_Data, tokens:list[tuple[Token, int]]):
        #print(tokens)
        for token, freq in tokens:
            if token.tok_str not in self.current_index:
                self.current_index[token.tok_str] = [[page.url, freq]]
            else:
                self.current_index[token.tok_str].append([page.url, freq])
            
        
    
    def store_token(self, page:Page_Data, token:Token, freq:int, fp) -> None:
        pass

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

