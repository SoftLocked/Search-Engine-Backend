import json
from nltk.stem import PorterStemmer
from indexer import Token
from preprocessor import merge_two_sorted_lists
import math

ps = PorterStemmer()

class Querier:
    def __init__(self, bins):
        self.bins = bins
        self.stop_word_file = dict()
        self.doc_ids = dict()
        self.idf = list()
        self.rank = dict()
        self.docfreq = 55393
        self.strongMult = 1000
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

        docid_sets = [0 for _ in unique_tok_list]
        self.idf = [0 for _ in unique_tok_list]
        tf = [0 for _ in unique_tok_list]

        for idx,v in enumerate(unique_tok_list):
            if v.is_stop():
                if v.tok_str not in self.stop_word_file:
                    docid_sets[idx] = set()
                else:
                    docid_set = set()
                    for i in self.stop_word_file[v.tok_str]:
                        docid_set.add(i[1])
                        if i[1] not in self.rank:
                            self.rank[i[1]] = [0] * (len(unique_tok_list) + 1)
                        agg_score = i[0][0]+ self.strongMult*i[0][1] #Strong is worth 5 hundred points OMG!!>!??!
                        tf = 1 + math.log(agg_score,10)
                        self.rank[i[1]][idx] = tf
                        self.rank[i[1]][-1] += tf**2
                        #print(self.doc_ids[str(i[1])], self.rank[i[1]])
                    docid_sets[idx] = docid_set
                    self.idf[idx] = math.log((self.docfreq/len(docid_sets[idx])),10)
            else:    
                
                with open(f"index/bin_index/{hash(v)%self.bins}.json") as in_file:
                    temp = json.load(in_file)
                    if v.tok_str not in temp:
                        docid_sets[idx] = set()
                    else:
                        docid_set = set()
                        for i in temp[v.tok_str]:
                            docid_set.add(i[1])
                            if i[1] not in self.rank:
                                self.rank[i[1]] = [0] * (len(unique_tok_list) + 1)
                            agg_score = i[0][0]+ self.strongMult*i[0][1] #Strong is worth 5 hundred points OMG!!>!??!
                            tf = 1 + math.log(agg_score,10)
                            self.rank[i[1]][idx] = tf
                            self.rank[i[1]][-1] += tf**2
                            #print(self.doc_ids[str(i[1])], self.rank[i[1]])
                        docid_sets[idx] = docid_set
                        self.idf[idx] = math.log((self.docfreq/len(docid_sets[idx])),10)
                    
        return docid_sets


    def merge_url_sets(self, url_sets):
        #return list(url_sets[0].intersection(*url_sets[1:]))
        return list(url_sets[0].union(*url_sets[1:]))
    
    def query(self, query: str):
        token_list = self.query_to_tokens(query)
        urls = self.merge_url_sets(
            self.get_url_set(token_list
            )
        )
        
        if len(urls) == 0:
            urls = [self.doc_ids[str(i)] for i in urls]
            return urls

        rank_dict = self.tfidf(urls, token_list)
        rank_list = list(rank_dict.keys())  
        return rank_list
    
    
    def tfidf(self, docids:list, tokens:list):
        final_rank = dict()

        # Uncomment to normalize
        
        # normalize query idf
        q_length = math.sqrt(sum([value ** 2 for value in self.idf]))

        for i in range(len(self.idf)):
            self.idf[i] = self.idf[i]/q_length
        
        
        # find rank (tf * idf)
        for doc in docids:
            rank = 0
            doc_len = math.sqrt(self.rank[doc][-1])
            for i in range(len(tokens)):
                rank += self.rank[doc][i]/doc_len * self.idf[i]
                #ank += self.rank[doc][i] * self.idf[i]
            final_rank[self.doc_ids[str(doc)]] = rank 

        self.rank = dict()
        self.idf = list()
        final_rank = dict(sorted(final_rank.items(), key=lambda item: item[1], reverse = True))

        return final_rank
