from Code.extract import ExtractStock
from Code.load import LoadData
import time

if __name__ == "__main__":
    ext = ExtractStock()
    ld = LoadData()
    print('Starting Execution....', end='')
    ext.processData("GAINERS")
    time.sleep(30)
    sQuery = ld.process()
    print("Done")
    sQuery.awaitTermination()