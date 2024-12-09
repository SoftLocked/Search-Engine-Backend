from querier.querier import Querier
import time



if __name__ == '__main__':
    q = Querier(10000)

    querystr = ""

    while querystr := input(">>> "):
        start_time = time.monotonic()
        urls = q.query(querystr)
        
        print("--------------------------------------------------")
        print(f'\t{'\n\t'.join(urls[:10])}')
        print("--------------------------------------------------")
        print(f"{len(urls)} URLs Found")
        print(f"Took {(time.monotonic() - start_time):.3f} Seconds\n")