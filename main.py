from extract import ExtractStock
from load import LoadData

if __name__ == "__main__":
    ext = ExtractStock()
    ld = LoadData()
    print('Starting Execution....', end='')
    ext.processData("GAINERS")
    ld.process()
    print("Done")