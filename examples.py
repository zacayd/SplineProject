import concurrent.futures
import pandas as pd

import threading

def write_to_orientdb(row,lock,inpt):
    # acquire the lock before writing to OrientDB

    with lock:
        try:
            print(inpt)
            print(row['col1'])
        except Exception as e:
         print(e)

if __name__ == "__main__":
    # create Pandas DataFrame
    data = {'col1': [1, 2, 3], 'col2': [4, 5, 6], 'col3': [7, 8, 9]}

    # create dataframe from dictionary
    df = pd.DataFrame(data)



    # create a lock to synchronize access to OrientDB
    lock = threading.Lock()
    inpt='5'

    # create a ThreadPoolExecutor with 4 workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        # submit a separate write_to_orientdb task for each row
        futures = [executor.submit(write_to_orientdb, row, lock,inpt) for index, row in df.iterrows()]

        # wait for all tasks to complete
        concurrent.futures.wait(futures)
