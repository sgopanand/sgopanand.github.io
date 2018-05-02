import unittest2
from .. import PnlContainer, PnlProcessor, PriceMessage, FillMessage

class PriceParserTest(unittest2.TestCase):

    def setUp(self):
        self.sut = PriceMessage()

    def test_parseAndValidate(self):
        testMsg = 'P 1388534400000 MSFT 42.43'
        res = self.sut.parseAndValidate(testMsg)
        self.assertIsInstance(res, PriceMessage)
        self.assertEqual(str(res), '[PriceMessage] <msgType - P ; msgTime - 1.3885344e+12 ; ticker - MSFT ; price - 42.43>')
        self.assertEqual(res.msgType, 'P')
        self.assertEqual(res.msgTime, 1388534400000)
        self.assertEqual(res.ticker, 'MSFT')
        self.assertEqual(res.price, 42.43)

        testMsg = 'P 1388534400000 MSFT 42.43\n'
        res = self.sut.parseAndValidate(testMsg)
        self.assertIsInstance(res, PriceMessage)
        self.assertEqual(res.msgType, 'P')
        self.assertEqual(res.msgTime, 1388534400000)
        self.assertEqual(res.ticker, 'MSFT')
        self.assertEqual(res.price, 42.43)

        testMsg = 'F 1388534400000 MSFT 42.43' #invalid message type
        self.assertRaises(ValueError, self.sut.parseAndValidate, testMsg)
        testMsg = 'P 1388534400000 MSFT1 42.43' #invalid ticker
        self.assertRaises(ValueError, self.sut.parseAndValidate, testMsg)
        testMsg = 'P 1388534400000 MSFT 42.a43' #invalid price
        self.assertRaises(ValueError, self.sut.parseAndValidate, testMsg)
        testMsg = 'P 138853a4400000 MSFT 42.43' #invalid message time
        self.assertRaises(ValueError, self.sut.parseAndValidate, testMsg)


class FillsParserTest(unittest2.TestCase):

    def setUp(self):
        self.sut = FillMessage()

    def test_parseAndValidate(self):
        testMsg = 'F 1388534400000 MSFT 42.43 300 B'
        res = self.sut.parseAndValidate(testMsg)
        self.assertIsInstance(res, FillMessage)
        self.assertEqual(str(res), '[FillMessage] <msgType - F ; msgTime - 1.3885344e+12 ; ticker - MSFT ; execPrice - 42.43 ; fillQuantity - 300.0 ; direction - B>')
        self.assertEqual(res.msgType, 'F')
        self.assertEqual(res.msgTime, 1388534400000)
        self.assertEqual(res.ticker, 'MSFT')
        self.assertEqual(res.execPrice, 42.43)
        self.assertEqual(res.fillQuantity, 300)
        self.assertEqual(res.direction, 'B')
        self.assertEqual(res.delta, +300.)

        testMsg = 'F 1388534400000 MSFT 42.43 300 S\n'
        res = self.sut.parseAndValidate(testMsg)
        self.assertIsInstance(res, FillMessage)
        self.assertEqual(res.msgType, 'F')
        self.assertEqual(res.msgTime, 1388534400000)
        self.assertEqual(res.ticker, 'MSFT')
        self.assertEqual(res.execPrice, 42.43)
        self.assertEqual(res.fillQuantity, 300)
        self.assertEqual(res.direction, 'S')
        self.assertEqual(res.delta, -300.)


        testMsg = 'P 1388534400000 MSFT 42.43 300 B' #invalid message type
        self.assertRaises(ValueError, self.sut.parseAndValidate, testMsg)
        testMsg = 'F 1388534400000 MSFT1 42.43 300 B' #invalid ticker
        self.assertRaises(ValueError, self.sut.parseAndValidate, testMsg)
        testMsg = 'F 1388534400000 MSFT 42.4w3 300 B' #invalid execution price
        self.assertRaises(ValueError, self.sut.parseAndValidate, testMsg)
        testMsg = 'F 13885344d00000 MSFT 42.43 300 B' #invalid message time
        self.assertRaises(ValueError, self.sut.parseAndValidate, testMsg)
        testMsg = 'F 1388534400000 MSFT 42.43 30a0 B' #invalid fillQuantity
        self.assertRaises(ValueError, self.sut.parseAndValidate, testMsg)
        testMsg = 'F 1388534400000 MSFT 42.43 300 x' #invalid direction
        self.assertRaises(ValueError, self.sut.parseAndValidate, testMsg)


class PnlContainerTest(unittest2.TestCase):
    #TODO add edge cases like price before fills and vice versa

    def setUp(self):
        self.sut = PnlContainer()
        self.sut1 = PnlContainer()

    def test_singleton(self):
        self.assertEqual(id(self.sut), id(self.sut1))

    def test_initialisation(self):
        self.assertEqual(self.sut._position.keys(), [])
        self.assertEqual(self.sut._cash.keys(), [])
        self.assertEqual(self.sut._latestPrice, {})

    def test_updates(self):
        testMsg = 'F 1388534400000 MSFT 10.0 100 B' #initial position
        self.assertIsNone(self.sut.handleMessage(FillMessage.parseAndValidate(testMsg)))
        self.assertEqual(self.sut._position['MSFT'], 100)
        self.assertEqual(self.sut._cash['USD'], -1000)
        self.assertEqual(self.sut._latestPrice, {})

        testMsg = 'F 1388534400000 MSFT 20.0 200 B' #position update long
        self.assertIsNone(self.sut.handleMessage(FillMessage.parseAndValidate(testMsg)))
        self.assertEqual(self.sut._position['MSFT'], 300)
        self.assertEqual(self.sut._cash['USD'], -5000)
        self.assertEqual(self.sut._latestPrice, {})

        testMsg = 'F 1388534400000 MSFT 15.0 100 S' #positions update short
        self.assertIsNone(self.sut.handleMessage(FillMessage.parseAndValidate(testMsg)))
        self.assertEqual(self.sut._position['MSFT'], 200)
        self.assertEqual(self.sut._cash['USD'], -3500)
        self.assertEqual(self.sut._latestPrice, {})

        testMsg = 'F 1388534400000 AAPL 15.0 300 S' #positions update new ticker
        self.assertIsNone(self.sut.handleMessage(FillMessage.parseAndValidate(testMsg)))
        self.assertEqual(self.sut._position['MSFT'], 200)
        self.assertEqual(self.sut._position['AAPL'], -300)
        self.assertEqual(self.sut._cash['USD'], 1000)
        self.assertEqual(self.sut._latestPrice, {})

        testMsg = 'P 1388534400000 MSFT 42.43' # fresh price for a positon
        res = self.sut.handleMessage(PriceMessage.parseAndValidate(testMsg))
        self.assertEqual(self.sut._position['MSFT'], 200)
        self.assertEqual(self.sut._position['AAPL'], -300)
        self.assertEqual(self.sut._cash['USD'], 1000)
        self.assertEqual(self.sut._latestPrice['MSFT'], 42.43)
        self.assertEqual(res, 42.43 * 200 + 1000)

        testMsg = 'P 1388534400000 AAPL 30.43' #new price for existing unpriced positions
        res = self.sut.handleMessage(PriceMessage.parseAndValidate(testMsg))
        self.assertEqual(self.sut._position['MSFT'], 200)
        self.assertEqual(self.sut._position['AAPL'], -300)
        self.assertEqual(self.sut._cash['USD'], 1000)
        self.assertEqual(self.sut._latestPrice['MSFT'], 42.43)
        self.assertEqual(self.sut._latestPrice['AAPL'], 30.43)
        self.assertEqual(res, (42.43 * 200) + (30.43 * -300) + 1000)

        testMsg = 'P 1388534400000 GOOG 1000.43' #unrelated price update, pnl remains unchanges
        res = self.sut.handleMessage(PriceMessage.parseAndValidate(testMsg))
        self.assertEqual(self.sut._position['MSFT'], 200)
        self.assertEqual(self.sut._position['AAPL'], -300)
        self.assertEqual(self.sut._cash['USD'], 1000)
        self.assertEqual(self.sut._latestPrice['MSFT'], 42.43)
        self.assertEqual(self.sut._latestPrice['AAPL'], 30.43)
        self.assertEqual(self.sut._latestPrice['GOOG'], 1000.43)
        self.assertEqual(res, (42.43 * 200) + (30.43 * -300) + 1000)


class MockIterator(object):

    def __init__(self, l):
        self._items = l
        self._counter = 0

    def __iter__(self):
        return self

    def next(self):
        if self._counter <= len(self._items) - 1:
            ret = self._items[self._counter]
            self._counter += 1
            return ret
        else:
            raise StopIteration

    def close(self):
        self.closeCalled = True

class PnlProcessorTest(unittest2.TestCase):

    def setUp(self):
        self.sut = PnlProcessor('', '')
        self.sut.initialize()

        msg1 = 'F 1 MSFT 10.0 100 B'
        msg2 = 'F 2 MSFT 10.0 100 B'
        msg3 = 'F 4 MSFT 10.0 100 B'
        self.sut._fillsStream  = MockIterator([msg1, msg2, msg3])

        msg1 = 'P 1 MSFT 10'
        msg2 = 'P 3 MSFT 20'
        msg3 = 'P 5 MSFT 30'
        self.sut._priceStream = MockIterator([msg1, msg2, msg3])

    def test_run(self):
        self.sut.run()
        self.assertEqual(self.sut._pnlContainer._position['MSFT'], 300)
        self.assertEqual(self.sut._pnlContainer._cash['USD'], -3000)
        self.assertEqual(self.sut._pnlContainer._latestPrice, {'MSFT': 30.0})

        self.sut.finalize()
        self.assertFalse(hasattr(self.sut, '_fillsStream'))
        self.assertFalse(hasattr(self.sut, '_priceStream'))


#TODO integration test for unZipAndFetchAsStreamUtil

