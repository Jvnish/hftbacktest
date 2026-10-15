import gzip
import json
import os
import tempfile
import unittest
import zlib

import numpy as np

from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest, GTC, LIMIT
from hftbacktest.data.utils import binancefutures
from hftbacktest.types import (
    DEPTH_EVENT, DEPTH_CLEAR_EVENT, DEPTH_SNAPSHOT_EVENT, TRADE_EVENT, BUY_EVENT, SELL_EVENT, EXCH_EVENT, LOCAL_EVENT
)

# The converter reads 19-digit nanosecond receipt timestamps.
BASE_MS = 1_791_000_000_000
HOURS_3_MS = 3 * 60 * 60 * 1000


def _ns(ms):
    return (BASE_MS + ms) * 1_000_000


def _line(recv_ms, message):
    return f'{_ns(recv_ms)} {json.dumps(message, separators=(",", ":"))}\n'.encode()


SNAPSHOT = _line(1005, {
    'lastUpdateId': 10, 'E': BASE_MS + 1000, 'T': BASE_MS + 1000, 'bids': [['100.0', '1']], 'asks': [['101.0', '1']]
})
MARKER = _line(1006, {
    'stream': 'btcusdt@collector',
    'data': {'e': 'collectorEvent', 'E': BASE_MS + 1001, 's': 'BTCUSDT', 'conn': 'public', 'event': 'connected',
             'detail': ''}
})
DEPTH = _line(1010, {
    'stream': 'btcusdt@depth@0ms',
    'data': {'e': 'depthUpdate', 'E': BASE_MS + 1003, 'T': BASE_MS + 1002, 's': 'BTCUSDT', 'U': 11, 'u': 11,
             'pu': 10, 'b': [['100.0', '2']], 'a': []}
})
# In markPriceUpdate, T is the next funding time.
MARK_PRICE = _line(1012, {
    'stream': 'btcusdt@markPrice@1s',
    'data': {'e': 'markPriceUpdate', 'E': BASE_MS + 1004, 'T': BASE_MS + 1000 + HOURS_3_MS, 's': 'BTCUSDT',
             'p': '100.5', 'i': '100.4', 'P': '100.45', 'r': '0.0001'}
})


def _sides(data):
    """The event sequences the exchange and the local processor see, without RPI rows and without side flags."""
    data = data[(data['ev'] & 0xff) != binancefutures.RPI_TRADE_EVENT]
    sequences = []
    for flag in (EXCH_EVENT, LOCAL_EVENT):
        side = data[(data['ev'] & flag) == flag].copy()
        side['ev'] &= ~np.uint64(EXCH_EVENT | LOCAL_EVENT)
        sequences.append(side.tobytes())
    return sequences


def _trade(recv_ms, ms, px):
    return _line(recv_ms, {
        'stream': 'btcusdt@trade',
        'data': {'e': 'trade', 'E': BASE_MS + ms + 1, 'T': BASE_MS + ms, 's': 'BTCUSDT', 't': ms, 'p': px, 'q': '0.5',
                 'X': 'MARKET', 'm': False}
    })


class TestBinanceFuturesConvert(unittest.TestCase):
    def test_mark_price_uses_event_time(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            filename = os.path.join(tmp_dir, 'btcusdt_20261015_000000.gz')
            with gzip.open(filename, 'wb') as f:
                f.write(SNAPSHOT + MARKER + DEPTH + MARK_PRICE)
            data = binancefutures.convert(filename, opt='m', buffer_size=100)

        ev = data['ev'] & 0xff
        mark = data[(ev >= 100) & (ev <= 102)]
        self.assertEqual(len(mark), 3)
        self.assertTrue((mark['exch_ts'] == (BASE_MS + 1004) * 1_000_000).all())
        # A next-funding-time exchange timestamp would have shifted every local timestamp by hours.
        depth = data[ev == DEPTH_EVENT]
        self.assertEqual(depth['local_ts'].tolist(), [_ns(1010)])
        self.assertEqual(depth['exch_ts'].tolist(), [(BASE_MS + 1002) * 1_000_000])

    def test_reads_segments_and_tolerates_truncation(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            complete = os.path.join(tmp_dir, 'btcusdt_20261015_000000.gz')
            with gzip.open(complete, 'wb') as f:
                f.write(SNAPSHOT + MARKER + DEPTH)

            # A killed collector leaves a segment that was sync-flushed but has no gzip trailer.
            killed = os.path.join(tmp_dir, 'btcusdt_20261015_010000.gz.open')
            compressor = zlib.compressobj(6, zlib.DEFLATED, 31)
            with open(killed, 'wb') as f:
                f.write(compressor.compress(_trade(2005, 2000, '101.0')))
                f.write(compressor.flush(zlib.Z_SYNC_FLUSH))

            # A complete file whose last line is cut.
            partial = os.path.join(tmp_dir, 'btcusdt_20261015_020000.gz')
            with gzip.open(partial, 'wb') as f:
                f.write(_trade(3005, 3000, '102.0') + _trade(3006, 3001, '103.0')[:40])

            data = binancefutures.convert([complete, killed, partial], buffer_size=100)

        ev = data['ev'] & 0xff
        self.assertEqual((ev == DEPTH_CLEAR_EVENT).sum(), 2)
        self.assertEqual((ev == DEPTH_SNAPSHOT_EVENT).sum(), 2)
        self.assertEqual((ev == DEPTH_EVENT).sum(), 1)
        trades = data[ev == TRADE_EVENT]
        self.assertEqual(trades['px'].tolist(), [101.0, 102.0])
        # The connection marker produces no event.
        self.assertEqual(len(data), 7)

    def test_snapshot_clears_the_whole_side(self):
        # The first snapshot has an ask at 110.0 that is gone by the second snapshot, whose asks end at 103.0; the diff
        # deleting it was lost. Clearing only up to the last snapshot level kept 110.0, and it became the best ask once
        # 101.5 and 103.0 were deleted.
        def snapshot(recv_ms, bids, asks):
            return _line(recv_ms, {'lastUpdateId': recv_ms, 'E': BASE_MS + recv_ms - 5, 'T': BASE_MS + recv_ms - 5,
                                   'bids': bids, 'asks': asks})

        lines = (
            snapshot(1005, [['100.0', '1']], [['101.0', '1'], ['102.0', '1'], ['110.0', '1']])
            + snapshot(2005, [['100.0', '1']], [['101.5', '1'], ['103.0', '1']])
            + _line(3005, {
                'stream': 'btcusdt@depth@0ms',
                'data': {'e': 'depthUpdate', 'E': BASE_MS + 3001, 'T': BASE_MS + 3000, 's': 'BTCUSDT', 'U': 2006,
                         'u': 2006, 'pu': 2005, 'b': [], 'a': [['101.5', '0'], ['103.0', '0']]}
            })
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            filename = os.path.join(tmp_dir, 'btcusdt_20261015_000000.gz')
            with gzip.open(filename, 'wb') as f:
                f.write(lines)
            npz = os.path.join(tmp_dir, 'btcusdt.npz')
            data = binancefutures.convert(filename, output_filename=npz, buffer_size=100)

            ev = data['ev'] & 0xff
            clear = data[ev == DEPTH_CLEAR_EVENT]
            self.assertEqual(((clear['ev'] & BUY_EVENT) == BUY_EVENT).sum(), 2)
            self.assertEqual(((clear['ev'] & SELL_EVENT) == SELL_EVENT).sum(), 2)
            self.assertTrue(np.isnan(clear['px']).all())

            asset = (
                BacktestAsset()
                .data([npz])
                .linear_asset(1.0)
                .constant_order_latency(0, 0)
                .risk_adverse_queue_model()
                .no_partial_fill_exchange()
                .trading_value_fee_model(0.0, 0.0)
                .tick_size(0.5)
                .lot_size(0.001)
            )
            hbt = HashMapMarketDepthBacktest([asset])
            # The clock starts at the first event only after the first elapse.
            hbt.elapse(1)
            hbt.elapse(_ns(2500) - hbt.current_timestamp)
            self.assertEqual((hbt.depth(0).best_bid, hbt.depth(0).best_ask), (100.0, 101.5))
            hbt.elapse(_ns(3500) - hbt.current_timestamp)
            self.assertEqual(hbt.depth(0).best_bid, 100.0)
            self.assertTrue(np.isnan(hbt.depth(0).best_ask))
            hbt.close()

    def test_rpi_trades_are_kept_without_changing_fills(self):
        # An App/Web taker sells into a hidden RPI bid at 99.5, below the visible bid and below our 100.5 bid. As a
        # trade it would fill our bid; as an RPI event it must not. The MARKET sell at 100.0 then fills it.
        def trade(recv_ms, tid, px, qty, x):
            return _line(recv_ms, {
                'stream': 'btcusdt@trade',
                'data': {'e': 'trade', 'E': BASE_MS + recv_ms - 4, 'T': BASE_MS + recv_ms - 5, 's': 'BTCUSDT',
                         't': tid, 'p': px, 'q': qty, 'X': x, 'm': True}
            })

        def run(lines, opt):
            with tempfile.TemporaryDirectory() as tmp_dir:
                filename = os.path.join(tmp_dir, 'btcusdt_20261015_000000.gz')
                with gzip.open(filename, 'wb') as f:
                    f.write(lines)
                npz = os.path.join(tmp_dir, 'btcusdt.npz')
                data = binancefutures.convert(filename, output_filename=npz, opt=opt, buffer_size=100)
                asset = (
                    BacktestAsset()
                    .data([npz])
                    .linear_asset(1.0)
                    .constant_order_latency(0, 0)
                    .risk_adverse_queue_model()
                    .no_partial_fill_exchange()
                    .trading_value_fee_model(0.0, 0.0)
                    .tick_size(0.5)
                    .lot_size(0.001)
                )
                hbt = HashMapMarketDepthBacktest([asset])
                hbt.elapse(1)
                hbt.elapse(_ns(1500) - hbt.current_timestamp)
                hbt.submit_buy_order(0, 1, 100.5, 1.0, GTC, LIMIT, False)
                positions = []
                for ms in (2500, 3500):
                    hbt.elapse(_ns(ms) - hbt.current_timestamp)
                    positions.append(hbt.position(0))
                hbt.close()
            return data, positions

        def tail(x):
            return trade(2005, 1, '99.5', '0.5', x) + trade(2105, 2, '0', '0', 'NA') + trade(3005, 3, '100.0', '2', 'MARKET')

        plain, plain_positions = run(SNAPSHOT + tail('RPI'), '')
        kept, kept_positions = run(SNAPSHOT + tail('RPI'), 'r')
        _, as_trade_positions = run(SNAPSHOT + tail('MARKET'), '')

        kind = kept['ev'] & 0xff
        rpi = kept[kind == binancefutures.RPI_TRADE_EVENT]
        self.assertEqual(len(rpi), 1)
        self.assertEqual((rpi['px'][0], rpi['qty'][0]), (99.5, 0.5))
        self.assertTrue(rpi['ev'][0] & SELL_EVENT)
        # Without the RPI rows, the exchange and the local processor see exactly the events of the default conversion.
        # Rows themselves can differ: correct_event_order may split an event into separate exchange and local rows.
        self.assertEqual(_sides(kept), _sides(plain))
        self.assertEqual(plain_positions, [0.0, 1.0])
        self.assertEqual(kept_positions, [0.0, 1.0])
        # Control: the same print as a MARKET trade fills the bid at once.
        self.assertEqual(as_trade_positions, [1.0, 1.0])


if __name__ == '__main__':
    unittest.main()
