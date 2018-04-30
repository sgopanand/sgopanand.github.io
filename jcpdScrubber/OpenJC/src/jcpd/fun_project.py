
'''
Created on Apr 29, 2018

@author: sgopanand
'''

import gzip
import io, logging
import sys

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
        logging.info('File %s opened and uncompressed', filePath)
        return fileStream
        

class PNLProcessor(object):
    
    def __init__(self, priceFilePath, fillsFilePath):
        self._priceFilePath, self._fillsFilePath = priceFilePath, fillsFilePath
        
    def initialize(self):
        self.priceStream = unZipAndFetchAsStreamUtil(self._priceFilePath)
        self.fillsStream = unZipAndFetchAsStreamUtil(self._fillsFilePath)
    
    def run(self):
        pass
    
    def finalize(self):
        pass
    
    


def main():
    print sys.argv
    logging.info('Received the following command line args, %s ', str(sys.argv[1:]))
    #priceFilePath, fillsFilePath = sys.argv[1:]
    #processor = PNLProcessor()

if __name__ == '__main__':
    priceFilePath = 'prices.gz'
    fillsFilePath = 'fills.gz'
    main()
