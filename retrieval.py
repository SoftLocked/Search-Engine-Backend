import json
# 1) load 2 index posting lists at a time (maybe n docids from a posting list at a time in case 
#    of not having enough memory)

# 2) get the intersection of docids between the 2 posting lists

# 3) repeat step 1 and 2 until we have gone through all boolean parameters

def boolean_query(query: list[str]) -> list[tuple]:
    matches = []
    if len(query) == 0:
        return
    if len(query) == 1:
        # return the posting list for the one word
        posting = get_postings_list(query[0])
        return posting
    
    word_1 = query.pop(0)
    word_2 = query.pop(0)

    posting_1 = get_postings_list(word_1)
    posting_2 = get_postings_list(word_2)

    if not posting_1 or posting_2:
        return
    
    matches = get_matches(posting_1, posting_2)
    while query:
        new_word = query.pop(0)
        new_posting = get_postings_list(new_word)
        # if new_word not found in index then return empty list
        if not new_posting:
            return
        matches = get_matches(matches, new_posting)
    return matches


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

def get_postings_list(word) -> list[tuple]:
    file_num = abs(hash(word.islower())) % 5
    print(file_num)
    with open(f'{file_num}_index.json', 'r') as read_file:
        data = json.load(read_file)
        for key, value in data.items():
            if key == word:
                return value
    return []

if __name__ == '__main__':
    pass
