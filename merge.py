import json
import time
from datetime import datetime, timedelta
from multiprocessing.dummy import Pool
from pathlib import Path

token_set = set()
json_list = ['index_store/10000_pages_checkpoint.json', 
             'index_store/20000_pages_checkpoint.json',
             'index_store/30000_pages_checkpoint.json',
             'index_store/40000_pages_checkpoint.json',
             'index_store/50000_pages_checkpoint.json',
             'index_store/55393_pages_checkpoint.json']

def get_tokens():
    start_time = time.time()
    print("Getting tokens")
    for index in json_list:
        with open(index, 'r') as file:
            data = json.load(file)
            for key in data:
                if key in token_set:
                    pass
                else:
                    token_set.add(key)
    token_dict = dict.fromkeys(token_set, 0)
    with open('tokens.json', 'w') as out_file:
        json.dump(token_dict, out_file, indent=4)
    print("Took: " + str(timedelta(seconds=time.time()-start_time)))
    return list(token_set)

def open_token_json():
     with open('tokens.json', 'r') as file:
        index_dict = json.load(file)
        return list(index_dict)
    

def merge_tokens(partial_token_list):
    batches = [{},{},{},{},{}]
    # For each partial index, look for token and combine postings
    for index in json_list:
        with open(index, 'r') as file:
            index_dict = json.load(file)
            for token in partial_token_list:     
                if token in index_dict:
                    bin = abs(hash(token.lower())) % 5
                    if token in batches[bin]:
                        for post in index_dict[token]:
                            batches[bin][token].append(post)
                        batches[bin][token].sort(key=lambda x: x[0])
                    else:
                        batches[bin][token] = index_dict[token]
    return batches


def pool_index_merge(token_list):
    start_time = time.time()
    print("Merging")
    p = Pool()
    partial_batch = (p.map(merge_tokens, token_list))

    for i in range(len(partial_batch)):
    # JSON DUMP 
        for j in range(5):
            file_path = Path(f'index_store/{j}_index.json')
            if(file_path.is_file()):
                with open(file_path, 'r') as out_file:
                    index_dict = json.load(out_file)
                    index_dict.update(partial_batch[i][j])
                with open(file_path, 'w') as out_file:
                    json.dump(index_dict, out_file, indent=4)
            else:
                with open(file_path, 'w') as out_file:
                    json.dump(partial_batch[i][j], out_file, indent=4)
    print("Took: " + str(timedelta(seconds=time.time()-start_time)))
    

def index_merge(token_list):
    start_time = time.time()
    print("Merging")
    for partial_list in token_list:
        print("Merging new batch")
        partial_batch = merge_tokens(partial_list)
        for j in range(5):
            file_path = Path(f'{j}_index.json')
            if(file_path.is_file()):
                with open(f'{j}_index.json', 'r') as out_file:
                    index_dict = json.load(out_file)
                    index_dict.update(partial_batch[j])
                with open(f'{j}_index.json', 'w') as out_file:
                    json.dump(index_dict, out_file, indent=4)
            else:
                with open(f'{j}_index.json', 'w') as out_file:
                    json.dump(partial_batch[j], out_file, indent=4)
        print("Finished a batch:", str(timedelta(seconds=time.time()-start_time)))
    print("Took: " + str(timedelta(seconds=time.time()-start_time)))


if __name__ == '__main__':
    #get_tokens()
    token_list = open_token_json()
    pool_index_merge([token_list[:10000], token_list[10000:20000], token_list[20000:30000], token_list[30000:40000], token_list[40000:50000], token_list[50000:]])
