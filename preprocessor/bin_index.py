from pathlib import Path
import json

from indexer import Token

class BinIndex:

    def __init__(self, bins: int) -> None:
        self.bins = bins

        for bin in range(self.bins):
            with open(f'index/bin_index/{bin}.json', 'w') as out_file:
                json.dump(dict(), out_file, indent=4)
    
    def index(self) -> None:
        for partial in Path('index/partial_index/').glob('*.json'):
            print(f"Processing {partial}")
            self.partial_to_bins(partial)

    def partial_to_bins(self, path:Path) -> list[dict]:
        data = dict()

        dicts = [dict() for i in range(self.bins)]

        with open(path, 'r') as in_file:
            data = json.load(in_file)
        
        print("Splitting Tokens...")
        for k,v in data.items():
            dicts[hash(Token(k))%self.bins][k] = v
        
        print("Merging to Hash Files...")
        for bin in range(self.bins):
            self.dict_to_hashfile(dicts[bin], bin)
    


    def dict_to_hashfile(self, in_data:dict, bin:int) -> None:
        curr_data = dict()
        
        with open(f'index/bin_index/{bin}.json', 'r') as in_file:
            curr_data = json.load(in_file)
        
        print(f"Merging bin {bin}")
        for k, v in in_data.items():
            if k not in curr_data:
                curr_data[k] = v
            else:
                curr_data[k].append(v)

        with open(f'index/bin_index/{bin}.json', 'w') as out_file:
            json.dump(curr_data, out_file, indent=4)