from Code.extract import ExtractStock
import time

if __name__ == "__main__":

    ext = ExtractStock()
#----------------------------#
    print("Starting Execution...", end='')
    ext.processData("GAINERS")
    time.sleep(30)
    ext.processData("LOSERS")
    time.sleep(30)
    ext.processData("MOST_ACTIVE")
    print("Done")
#----------------------------#
