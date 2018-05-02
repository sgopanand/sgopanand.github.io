
'''
Created on May 1, 2018

@author: sgopanand
'''

import gzip
import io, logging
import sys
from collections import defaultdict

#################################################################################################################
##################################                  CONSTANTS           #########################################
#################################################################################################################

ROW_SEPERATOR = ' '
PRICE_MESSAGE = 'P'
ORDER_CONFIRM = 'F'
USD = 'USD'

#################################################################################################################
##################################                  UTILS               #########################################
#################################################################################################################

def unZipAndFetchAsStreamUtil(filePath):
    '''
    Typically found in utils lib, ability to read a '.gz' file, uncompress into a buffered stream
    '''
    try:
        fileStream = io.BufferedReader(gzip.GzipFile(filePath))
    except IOError:
        logging.exception('File %s, not found', filePath)
        fileStream = None
    finally:
        logging.debug('File %s opened and uncompressed', filePath)
        return fileStream

#################################################################################################################
##################################             MESSAGE PARSERS          #########################################
#################################################################################################################


class Message(object):

    MESSAGE_SEED = None
    ORDER = []

    def __str__(self):
        return '[%s] <%s>' % ( self.__class__.__name__, ' ; '.join([ '%s - %s' % (prop, getattr(self, prop)) for prop in self.ORDER]) )

    @property
    def msgType(self):
        return self._msgType

    @msgType.setter
    def msgType(self, value):
        if value and value == self.MESSAGE_SEED:
            self._msgType = value
        else:
            raise ValueError('Invalid value for msg Type %s' % value)

    @property
    def msgTime(self):
        return self._msgTime

    @msgTime.setter
    def msgTime(self, value):
        if value not in [None, '']:
            self._msgTime = float(value)
        else:
            raise ValueError('Invalid value for time %s' % value)

    @property
    def ticker(self):
        return self._ticker

    @ticker.setter
    def ticker(self, value):
        if value and len(value) == 4:
            self._ticker = value
        else:
            raise ValueError('Invalid value for Ticker %s' % value)

    @classmethod
    def parseAndValidate(cls, row):
        if row:
            new = cls()
            for attr, value in zip(cls.ORDER, row.replace('\n', '').split(ROW_SEPERATOR)):
                setattr(new, attr, value)
            return new


class PriceMessage(Message):

    MESSAGE_SEED = 'P'
    ORDER = ['msgType', 'msgTime', 'ticker', 'price']

    @property
    def price(self):
        return self._price

    @price.setter
    def price(self, value):
        if value:
            self._price = float(str(value).replace(',', ''))
        else:
            raise ValueError('Invalid value for price %s' % value)


class FillMessage(Message):

    MESSAGE_SEED = 'F'
    ORDER = ['msgType', 'msgTime', 'ticker', 'execPrice', 'fillQuantity', 'direction']

    @property
    def execPrice(self):
        return self._execPrice

    @execPrice.setter
    def execPrice(self, value):
        if value:
            self._execPrice = float(str(value).replace(',', ''))
        else:
            raise ValueError('Invalid value for execution price %s' % value)

    @property
    def fillQuantity(self):
        return self._fillQuantity

    @fillQuantity.setter
    def fillQuantity(self, value):
        if value:
            self._fillQuantity = float(str(value).replace(',', ''))
        else:
            raise ValueError('Invalid value for fill quantity %s' % value)

    @property
    def direction(self):
        return self._direction

    @direction.setter
    def direction(self, value):
        if value and value in set(['B', 'S']):
            self._direction = value
        else:
            raise ValueError('Invalid value for direction %s' % value)

    @property
    def delta(self):
        return (self.fillQuantity * -1) if self._direction == 'S' else self.fillQuantity


#################################################################################################################
##################################             Business Logic           #########################################
#################################################################################################################


class PnlContainer(object):
    __singleton = None

    def __new__(cls, *args, **kwargs):
        if not cls.__singleton:
            cls.__singleton = super(PnlContainer, cls).__new__(cls, *args, **kwargs)
        return cls.__singleton

    def __init__(self):
        self._position = defaultdict(float)
        self._cash = defaultdict(float)
        self._latestPrice = dict()

    def handleMessage(self, msg):
        if msg.msgType == ORDER_CONFIRM:
            self._position[msg.ticker] += msg.delta
            self._cash[USD] += (-1. * msg.delta * msg.execPrice) # assuming USD cash currency

        elif msg.msgType == PRICE_MESSAGE:
            self._latestPrice[msg.ticker] = msg.price
            pnl = self._cash[USD] + sum(
                [ self._position[ticker] * self._latestPrice.get(ticker, 0.) for ticker in self._position.keys() ]
            )
            logging.info('PNL @ %d is %s', msg.msgTime, pnl)
            return pnl


class PnlProcessor(object):

    def __init__(self, priceFilePath, fillsFilePath):
        self._priceFilePath, self._fillsFilePath = priceFilePath, fillsFilePath

    def initialize(self):
        self._priceStream = unZipAndFetchAsStreamUtil(self._priceFilePath)
        self._fillsStream = unZipAndFetchAsStreamUtil(self._fillsFilePath)
        self._pnlContainer = PnlContainer()
        initStatus = 'Successful' if all([self._priceStream, self._fillsStream]) else 'Unsuccessfull'
        logging.debug('Initializing PNL Processor - %s', initStatus)

    def getNextFillsAndParse(self):
        try:
            return FillMessage.parseAndValidate(self._fillsStream.next())
        except StopIteration:
            return None

    def getNextPriceAndParse(self):
        try:
            return PriceMessage.parseAndValidate(self._priceStream.next())
        except StopIteration:
            return None

    def run(self):
        nextFill = self.getNextFillsAndParse()
        nextPrice = self.getNextPriceAndParse()
        while True:
            if nextFill and nextPrice:
                if nextFill.msgTime <= nextPrice.msgTime:
                    self._pnlContainer.handleMessage(nextFill)
                    nextFill = self.getNextFillsAndParse()
                elif nextFill.msgTime > nextPrice.msgTime:
                    self._pnlContainer.handleMessage(nextPrice)
                    nextPrice = self.getNextPriceAndParse()
            elif nextFill and not nextPrice:
                self._pnlContainer.handleMessage(nextFill)
                nextFill = self.getNextFillsAndParse()
            elif not nextFill and nextPrice:
                self._pnlContainer.handleMessage(nextPrice)
                nextPrice = self.getNextPriceAndParse()
            elif not nextFill and not nextPrice:
                break

    def finalize(self):
        if self._fillsStream:
            self._fillsStream.close()
            del self._fillsStream
        if self._priceStream:
            self._priceStream.close()
            del self._priceStream
        logging.debug('Finalizing PNL Processor - Completed')


def run():
    priceFilePath = 'c:\working\prices.gz'
    fillsFilePath = 'c:\\working\\fills.gz'
    p = PnlProcessor(priceFilePath, fillsFilePath)
    p.initialize()
    p.run()
    p.finalize()


def main():
    print sys.argv
    logging.info('Received the following command line args, %s ', str(sys.argv[1:]))
    run()
    #priceFilePath, fillsFilePath = sys.argv[1:]
    # priceFilePath = 'c:\working\prices.gz'
    # fillsFilePath = 'c:\working\fills.gz'
    # processor = PNLProcessor()

# if __name__ == '__main__':
#     priceFilePath = 'c:\working\prices.gz'
#     fillsFilePath = 'c:\working\fills.gz'
#     main()
