import os
import zlib

COMPRESSION = 9
MSG_END_CHAR = b'\n'
NUL_BYTE = b'\x00'


class Queue:
    def __init__(self, filename):
        try:
            self.q = open(filename, 'r+b')
        except FileNotFoundError:
            self.q = open(filename, 'w+b')

    def __del__(self):
        self.q.close()

    def enqueue(self, message):
        self.q.seek(0, 2)
        self.q.write(zlib.compress(bytes(message, encoding='utf-8'), COMPRESSION))
        self.q.write(MSG_END_CHAR)
        self._commit()

    def dequeue(self):
        import pdb; pdb.set_trace()
        self.q.seek(0)
        first_msg = None
        second_msg = None
        first_msg = self.q.readline().strip()
        if first_msg:
            second_msg = self.q.readline().strip()
        else:
            return None

        if second_msg:
            self.q.seek(0)
            new_message_size = len(first_msg) + len(second_msg) + 1
            self.q.write(NUL_BYTE * new_message_size)
            self.q.write(MSG_END_CHAR)
            self.q.seek(0)
            self.q.write(second_msg)
        else:
            self.q.seek(0)
            self.q.truncate()

        self._commit()

        return zlib.decompress(first_msg).decode('utf-8')

    def _commit(self):
        self.q.flush()
        os.fsync(self.q)
