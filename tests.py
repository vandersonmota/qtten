import tempfile
import sys
import unittest
from unittest.mock import patch
from qtten import Queue


class TestQtten(unittest.TestCase):
    def setUp(self):
        self.q = Queue(tempfile.mkstemp()[1])

    def test_enqueue_dequeue(self):
        self.q.enqueue('hey there')
        self.q.enqueue('how are you?')
        self.q.enqueue('great!')

        self.assertEqual('hey there',  self.q.dequeue())
        self.assertEqual('how are you?',  self.q.dequeue())
        self.assertEqual('great!',  self.q.dequeue())

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

        with open(self.q.filename, 'rb') as fd:
            contents = fd.read()
            self.assertLess(sys.getsizeof(contents), sys.getsizeof(big_msg))


if __name__ == '__main__':
    unittest.main()
