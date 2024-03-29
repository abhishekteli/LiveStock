
from Code.load import LoadData
import time

if __name__ == "__main__":

    gld = LoadData('GAINERS')
    lld = LoadData('LOSERS')
    ald = LoadData('MOST_ACTIVE')
#------------------------------------#
    sQuery_gainers = gld.process()
    sQuery_losers = lld.process()
    sQuery_active = ald.process()
#------------------------------------#
    sQuery_gainers.awaitTermination()
    sQuery_losers.awaitTermination()
    sQuery_active.awaitTermination()

