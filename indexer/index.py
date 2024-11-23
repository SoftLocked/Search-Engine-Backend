from pathlib import Path
import json
# from nltk.stem import PorterStemmer
# from nltk.corpus import stopwords
import time
from datetime import timedelta
from multiprocessing.dummy import Pool, Manager
import bisect

from indexer.Token import Token
from indexer.PageData import PageData

doc_id_dict = dict()

# Pooled Process
def read_files(args):
    file, v = args
    v.value += 1
    # print(f"Processing File | {file}" + (" "*25) + "\r", end='')
    page = PageData(file)
    if page.url not in doc_id_dict:
        doc_id = len(doc_id_dict)
        doc_id_dict[page.url] = doc_id
    return (page, page.get_tokens())

class Index:
    def __init__(self, offload) -> None:
        self.offload = offload
        self.current_index = dict()
        for item in Path('index/partial_index/').iterdir():
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
            
            q = Manager().Value(int, 0)
            
            token_group = p.map_async(read_files, [(i,q) for i in pages[i:i+self.offload] ])
            
            while True:
                if token_group.ready():
                    
                    break
                else:
                    print(f"Files Processed: {q.value} of {batch_size} | {100*q.value/batch_size:.2f}%", end='\r')

            token_group = token_group.get()

            file_process = 1

            #print()
            print(f"Batch {batch}: Finished Processing Batch!")
            print(f"Batch {batch}: Adding Batch to Index...")

            for page,tokens in token_group:
                self.store_tokens(page, tokens)

            print(f"Batch {batch}: Finished Adding!")
            print(f"Batch {batch}: Writing batch")

            with open(f'index/partial_index/{i + batch_size}_pages_checkpoint.json', 'w') as out_file:
                #print(iter)
                #print(self.current_index)
                json.dump(self.current_index, out_file, indent=4)
                self.current_index = dict()

            print(f"Batch {batch}: Finished Writing!")

            print("Took: " + str(timedelta(seconds=time.time()-start_time)))
            start_time = time.time()

            reversed_dict = {}

            for key, value in doc_id_dict.items():
                reversed_dict[value] = key

            with open('index/DocIDtoURL.json', 'w') as file:
                json.dump(reversed_dict, file)

        print(f"Finished Processing! {page_count} Pages Processed.")

    def store_tokens(self, page:PageData, tokens:list[tuple[Token, int]]):
        #print(tokens)
        for token, freq in tokens:
            if token.tok_str not in self.current_index:
                self.current_index[token.tok_str] = [[freq, doc_id_dict[page.url]]]
            else:
                if freq <= self.current_index[token.tok_str][-1][0]:
                    self.current_index[token.tok_str].append([freq, doc_id_dict[page.url]])
                else:
                    for i,v in enumerate( self.current_index[token.tok_str] ):
                        if v[0] <= freq:
                            self.current_index[token.tok_str].insert(i, [freq, doc_id_dict[page.url]])
                            break
                
                
            
        
    
    def store_token(self, page:PageData, token:Token, freq:int, fp) -> None:
        pass

        '''
        with open('index/partial_index/temp.txt', 'r') as in_file:
            with open(f'index/partial_index/{bin}.txt', 'w') as out_file:
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

