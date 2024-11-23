from querier.querier import Querier
import time



if __name__ == '__main__':
    q = Querier(10000)

    querystr = ""

    while querystr := input(">>> "):
        start_time = time.monotonic()
        q.query(querystr)
        print(f"Took {(time.monotonic() - start_time):.3f} Seconds\n")