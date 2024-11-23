import json
from nltk.stem import PorterStemmer
from indexer import Token
from preprocessor import merge_two_sorted_lists

ps = PorterStemmer()

class Querier:
    def __init__(self, bins):
        self.bins = bins
        self.stop_word_file = dict()
        self.doc_ids = dict()
        # preload the index and docid files
        with open(f'index/bin_index/{self.bins}.json') as in_file:
            self.stop_word_file = json.load(in_file)
        with open('index/DocIDtoURL.json', 'r') as doc_id_file:
            self.doc_ids = json.load(doc_id_file)

    def query_to_tokens(self, query: str):
        token_list = []

        curr_buff = []
        for i, curr_char in enumerate(query):
            if 'a' <= curr_char <= 'z' or 'A' <= curr_char <= 'Z' or '0' <= curr_char <= '9':
                curr_buff.append(curr_char)
            else:
                if len(curr_buff) > 0:
                    tok = Token(''.join(curr_buff))
                    token_list.append(tok)
                curr_buff = []
                continue
        
        if len(curr_buff) > 0:
            tok = Token(''.join(curr_buff))
            token_list.append(tok)

        return token_list

    def get_url_set(self, tok_list):
        unique_tok_list = list(set(tok_list))

        url_sets = [0 for _ in unique_tok_list]

        for idx,v in enumerate(unique_tok_list):
            if v.is_stop():
                if v.tok_str not in self.stop_word_file:
                    url_sets[idx] = set()
                else:
                    url_sets[idx] = set([self.doc_ids[str(i[1])] for i in self.stop_word_file[v.tok_str]])
            else:    
                
                with open(f"index/bin_index/{hash(v)%self.bins}.json") as in_file:
                    temp = json.load(in_file)
                    if v.tok_str not in temp:
                        url_sets[idx] = set()
                    else:
                        url_sets[idx] = set([self.doc_ids[str(i[1])] for i in temp[v.tok_str]])
        return url_sets


    def merge_url_sets(self, url_sets):
        return list(url_sets[0].intersection(*url_sets[1:]))
    
    def query(self, query: str):
        urls = self.merge_url_sets(
            self.get_url_set(
                self.query_to_tokens(
                    query
                )
            )
        )
        
        print("--------------------------------------------------")
        print(f'\t{'\n\t'.join(urls[:5])}')
        print("--------------------------------------------------")
        print(f"{len(urls)} URLs Found")