import os
import tempfile
import zlib
from contextlib import contextmanager

COMPRESSION = 9
MSG_END_TOKEN = b'EOM'
NUL_BYTE = b'\x00'
READ_CHUNK = 2048
INDEX_CHUNK = 50
MSG_END_TOKEN_SIZE = 3


class Queue:
    def __init__(self, filename):
        try:
            self.q = open(filename, 'r+b')
        except FileNotFoundError:
            self.q = open(filename, 'w+b')
        try:
            self.index = open('{}-index'.format(filename), 'r+b')
        except FileNotFoundError:
            self.index = open('{}-index'.format(filename), 'w+b')

        idx = self.index.read().decode('utf-8')
        self.index.close()
        if idx:
            self.next_msg_at, self.last_write_at = map(int, idx.split('-'))
            self.next_msg_checkpoint = self.next_msg_at
        else:
            self.next_msg_at = 0
            self.next_msg_checkpoint = 0
            self.last_write_at = 0

    def __del__(self):
        self._commit()
        self.q.close()

    def enqueue(self, message):
        with self._commit():
            self.q.seek(self.last_write_at)
            self.q.write(zlib.compress(bytes(message, encoding='utf-8'), COMPRESSION))
            self.q.write(MSG_END_TOKEN)

    def dequeue(self):
        first_msg = None
        second_msg = None
        data = b''
        first_msg_stops_at = -1
        second_msg_stops_at = -1

        self.q.seek(self.next_msg_at)

        while True:
            chunk = self.q.read(READ_CHUNK)
            if chunk == b'' or second_msg_stops_at != -1:
                break
            data += chunk
            first_msg_stops_at = data.find(MSG_END_TOKEN)
            if first_msg_stops_at != -1:
                second_msg_stops_at = data.find(MSG_END_TOKEN, first_msg_stops_at + MSG_END_TOKEN_SIZE)

        if data:
            if first_msg_stops_at != -1:
                with self._commit():
                    first_msg = data[0:first_msg_stops_at]
                    #updates index pointing to next message
                    self.next_msg_checkpoint = self.next_msg_at + len(first_msg) + MSG_END_TOKEN_SIZE

                return zlib.decompress(first_msg).decode('utf-8')
        return None

    @contextmanager
    def _commit(self):
        yield
        self.q.flush()
        os.fsync(self.q.fileno())
        self._update_indexes()

    def _update_indexes(self):
        last_write_at = self.q.tell()

        old_next_msg_at = self.next_msg_at
        old_last_write_at = self.last_write_at
        try:
            tmp_idx = tempfile.NamedTemporaryFile(dir=os.path.dirname(self.index.name), mode='wb', delete=False)
            tmp_idx.write(bytes('{}-{}'.format(self.next_msg_checkpoint, last_write_at), encoding='utf-8'))
            tmp_idx.flush()
            os.fsync(tmp_idx.fileno())
        finally:
            tmp_idx.close()
        old_idx = os.path.abspath(self.index.name)
        new_idx = os.path.abspath(tmp_idx.name)
        try:
            self.last_write_at = last_write_at
            self.next_msg_at = self.next_msg_checkpoint
            os.rename(new_idx, old_idx)
        except:
            self.last_write_at = old_last_write_at
            self.next_msg_at = old_next_msg_at
