# 1) load 2 index posting lists at a time (maybe n docids from a posting list at a time in case 
#    of not having enough memory)

# 2) get the intersection of docids between the 2 posting lists

# 3) repeat step 1 and 2 until we have gone through all boolean parameters


def boolean_query(query: List[str]):
    matches = []
    if len(query == 0):
        return
    if len(query) == 1:
        # return the posting list for the one word
        posting = []
        return posting
    word_1 = query.pop(0)
    word_2 = query.pop(0)

    # check that both words are in the index, if yes proceed, otherwise return empty list

    posting_1 = []  # get the postings of word 1
    posting_2 = []  # get the postings of word 2
    matches = get_matches(posting_1, posting_2)
    while query:
        new_word = query.pop(0)

        # if new_word not found in index then return empty list

        new_posting = [] # get posting of the new word from query
        matches = get_matches(matches, new_posting)
    return matches


def get_matches(posting_1: "List", posting_2: "List"):
    matches = []
    n, m = len(posting_1), len(posting_2)   #length of our postings
    i, j = 0 # our indexes to iterate the postings
    while (i != n and j != m):
        if posting_1[i] == posting_2[j]:
            matches.append(posting_1[i])
            i += 1
            j += 1
        elif posting_1[i] < posting_2[j]:
            i += 1
        else:
            j += 1
    return matches