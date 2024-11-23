from pathlib import Path
import json

from indexer import Token

def merge_two_sorted_lists(list1, list2):

    merged = []
    i, j = 0, 0

    while i < len(list1) and j < len(list2):
        if list1[i][0] > list2[j][0]:
            merged.append(list1[i])
            i += 1
        else:
            merged.append(list2[j])
            j += 1

    # Append any remaining elements from list1 or list2
    merged.extend(list1[i:])
    merged.extend(list2[j:])
    return merged

class BinIndex:

    def __init__(self, bins: int) -> None:
        self.bins = bins
        for item in Path('index/bin_index/').iterdir():
            if item.is_file():
                if item.name != '.gitignore':
                    item.unlink()

        for bin in range(self.bins+1):
            with open(f'index/bin_index/{bin}.json', 'w') as out_file:
                json.dump(dict(), out_file, indent=4)
    
    def index(self) -> None:
        for partial in Path('index/partial_index/').glob('*.json'):
            print(f"Processing {partial}")
            self.partial_to_bins(partial)

    def partial_to_bins(self, path:Path) -> list[dict]:
        data = dict()

        dicts = [dict() for i in range(self.bins)]
        stop_dict = dict()

        with open(path, 'r') as in_file:
            data = json.load(in_file)
        
        print("Splitting Tokens...")
        for k,v in data.items():
            if Token(k).is_stop():
                stop_dict[k] = v
            else:
                dicts[hash(Token(k))%self.bins][k] = v
        
        print("Merging to Hash Files...")
        for bin in range(self.bins):
            self.dict_to_hashfile(dicts[bin], bin)
        
        self.dict_to_hashfile(stop_dict, self.bins)

        print("Finished Merging!")
    


    def dict_to_hashfile(self, in_data:dict, bin:int) -> None:
        curr_data = dict()
        
        with open(f'index/bin_index/{bin}.json', 'r') as in_file:
            curr_data = json.load(in_file)
        
        print(f"Merging bin {bin}", end='\r')
        for k, v in in_data.items():
            if k not in curr_data:
                curr_data[k] = v
            else:
                curr_data[k] = merge_two_sorted_lists(curr_data[k], v)

        with open(f'index/bin_index/{bin}.json', 'w') as out_file:
            json.dump(curr_data, out_file, indent=4)