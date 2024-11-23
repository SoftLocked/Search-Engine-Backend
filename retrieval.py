import json
from nltk.stem import PorterStemmer
from indexer import Token

ps = PorterStemmer()

class Retrieval:
    def __init__(self):
        self.index_files = {}
        self.doc_ids = {}
        # preload the index and docid files
        for i in range(5):
            with open(f'index/bin_index/{i}.json', 'r') as file:
                self.index_files[i] = json.load(file)
        with open('index/DocIDtoURL.json', 'r') as doc_id_file:
            self.doc_ids = json.load(doc_id_file)

    def boolean_query(self, query: str) -> None:
        matches = []
        stemmed_query = [ps.stem(word) for word in query]
        if len(query) == 0:
            return
        if len(query) == 1:
            matches = self.get_postings_list(stemmed_query[0])
            #print(matches)
            self.print_found_urls(matches)
            return
        word_1 = ps.stem(stemmed_query.pop(0))
        word_2 = ps.stem(stemmed_query.pop(0))

        posting_1 = self.get_postings_list(word_1)
        posting_2 = self.get_postings_list(word_2)

        if not posting_1 or not posting_2:
            return
        matches = self.get_matches(posting_1, posting_2)
        while stemmed_query:
            new_word = stemmed_query.pop(0)
            new_posting = self.get_postings_list(new_word)
            if not new_posting:
                return
            matches = self.get_matches(matches, new_posting)
        save_matches(matches)
        self.print_found_urls(matches)

    def print_found_urls(self, posting: list[tuple]) -> None:
        count = 0
        if posting is None:
            print('Error: empty posting')
            return
        for pair in posting:
            print(pair)
            print(self.doc_ids.get(str(pair[0])))
            count += 1
        print(f'Number of found urls: {len(posting)}')

    def get_postings_list(self, word: str) -> list[tuple]:
        file_num = hash(Token(word)) % 5
        # print(token, file_num)
        # print(file_num)
        return self.index_files[file_num].get(word)

def get_matches(posting_1: list[tuple], posting_2: list[tuple]) -> list[tuple]:
    matches = []
    n, m = len(posting_1), len(posting_2) 
    i = 0
    j = 0
    while (i != n and j != m):
        if posting_1[i][0] == posting_2[j][0]:
            matches.append(posting_1[i])
            i += 1
            j += 1
        elif posting_1[i] < posting_2[j]:
            i += 1
        else:
            j += 1
    return matches

def save_matches(matches):
    with open("query_result.txt", "w") as f:
        for match in matches:
            f.write(match, "\n")

if __name__ == '__main__':
    pass