from retrieval import Retrieval
import time

def main():
    retriever = Retrieval()
    while True:
        query = input('Enter query here: ').split()
        if query and query[0] == 'exit':
            break
        start_time = time.time()
        retriever.boolean_query(query)
        print(f'time: {time.time() - start_time}')

if __name__ == '__main__':
    main()