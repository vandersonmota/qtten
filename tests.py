import tempfile
import zlib
import sys
import unittest
from unittest.mock import patch, Mock
from hypothesis.strategies import text
from hypothesis import given
from qtten import Queue


class TestQtten(unittest.TestCase):
    def setUp(self):
        self.q_name = tempfile.mkstemp()[1]
        self.q = Queue(self.q_name)

    @given(x=text(), y=text(), z=text())
    def test_enqueue_dequeue(self, x, y, z):
        self.q.enqueue(x)
        self.q.enqueue(y)
        self.q.enqueue(z)

        self.assertEqual(x,  self.q.dequeue())
        self.assertEqual(y,  self.q.dequeue())
        self.assertEqual(z,  self.q.dequeue())

    @given(x=text(), y=text(), z=text())
    def test_mixed_enqueue_dequeue(self, x, y, z):
        self.q.enqueue(x)
        self.q.enqueue(y)

        self.assertEqual(x,  self.q.dequeue())

        self.q.enqueue(z)
        self.assertEqual(y,  self.q.dequeue())
        self.assertEqual(z,  self.q.dequeue())

    @given(x=text(), y=text(), z=text())
    def test_queue_persists_state(self, x, y, z):
        self.q.enqueue(x)
        self.q.enqueue(y)

        self.assertEqual(x,  self.q.dequeue())

        del self.q
        self.q = Queue(self.q_name)

        self.q.enqueue(z)
        self.assertEqual(y,  self.q.dequeue())
        self.assertEqual(z,  self.q.dequeue())

    @given(x=text(), y=text(), z=text())
    def test_corrupted_enqueue(self, x, y, z):
        self.q.enqueue(x)


        self.q.enqueue(z)

        self.assertEqual(x,  self.q.dequeue())
        self.assertEqual(z,  self.q.dequeue())
        # no more msgs
        self.assertEqual(None,  self.q.dequeue())

    def test_dequeue_empty_queue(self):
        self.assertIsNone(self.q.dequeue())

    @patch('qtten.zlib.compress')
    def test_message_compression(self, zlib_compress):
        zlib_compress.return_value = b'\x21\234' #bunch of bytes
        self.q.enqueue('hey there')
        zlib_compress.assert_called_with(b'hey there', 9)

    def test_compressed_file(self):
        big_msg = 'big msg' * 5120000
        self.q.enqueue(big_msg)

        with open(self.q.q.name, 'rb') as fd:
            contents = fd.read()
            self.assertLess(sys.getsizeof(contents), sys.getsizeof(big_msg))


if __name__ == '__main__':
    unittest.main()
