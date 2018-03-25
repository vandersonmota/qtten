import os
import tempfile
import zlib
from collections import deque
from contextlib import contextmanager

COMPRESSION = 9
MSG_END_TOKEN = b'\x00EOM\x00'
NUL_BYTE = b'\x00'
READ_CHUNK = 2048
MSG_END_TOKEN_SIZE = 5


class Queue:
    def __init__(self, filename, buffer_size=20):
        try:
            self.q = open(filename, 'r+b')
        except FileNotFoundError:
            self.q = open(filename, 'w+b')
        index_filename = '{}-index'.format(filename)
        try:
            self.index = open(index_filename, 'r+b')
        except FileNotFoundError:
            self.index = open(index_filename, 'w+b')

        idx = self.index.read().decode('utf-8')
        self.index.close()
        if idx:
            self.next_msg_at, self.last_write_at = map(int, idx.split('-'))
            self.next_msg_checkpoint = self.next_msg_at
        else:
            self.next_msg_at = 0
            self.next_msg_checkpoint = 0
            self.last_write_at = 0

        self.buffer_size = buffer_size
        self.write_buffer = deque()
        self.read_buffer = deque()

    def __del__(self):
        self._commit()
        self.q.close()

    def enqueue(self, message):
        if not message:
            return

        full_msg = zlib.compress(bytes(message, encoding='utf-8'), COMPRESSION) + MSG_END_TOKEN

        self.write_buffer.append(full_msg)

        if len(self.write_buffer) >= self.buffer_size:
            with self._commit(True):
                all_msgs = b''.join(self.write_buffer)
                self.q.seek(self.last_write_at)
                self.q.write(all_msgs)
                self.write_buffer = deque()

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

        if data and first_msg_stops_at != -1:
            first_msg = data[0:first_msg_stops_at]
            #updates index pointing to next message
            try:
                msg = zlib.decompress(first_msg).decode('utf-8')
                self.next_msg_checkpoint = self.next_msg_at + len(first_msg) + MSG_END_TOKEN_SIZE
                self._update_indexes()
            except zlib.error:
                # invalid msg
                msg = None
        else:
            try:
                msg = self.write_buffer.popleft()
                msg_size = len(msg) - MSG_END_TOKEN_SIZE
                msg = zlib.decompress(msg[:msg_size]).decode('utf-8')
            except (IndexError, zlib.error):
                msg = None

        return msg

    @contextmanager
    def _commit(self, written=False):
        yield
        self.q.flush()
        os.fsync(self.q.fileno())
        self._update_indexes(written)

    def _update_indexes(self, written=False):
        if written:
            last_write_at = self.q.tell()
        else:
            last_write_at = self.last_write_at

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
