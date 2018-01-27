import os
import zlib

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

    def __del__(self):
        self.q.flush()
        os.fsync(self.q.fileno())
        self.q.close()

    def enqueue(self, message):
        self.q.seek(0)
        idx = self.q.read(INDEX_CHUNK + MSG_END_TOKEN_SIZE)
        # emtpy file, writing dummy index
        if not idx:
            self.q.write(NUL_BYTE * INDEX_CHUNK)
            self.q.write(MSG_END_TOKEN)
            pos = self.q.tell()
            self.q.seek(0)
            self.q.write(bytes(str(pos), encoding='utf-8'))
        self.q.seek(0, 2)
        self.q.write(zlib.compress(bytes(message, encoding='utf-8'), COMPRESSION))
        self.q.write(MSG_END_TOKEN)
        self.q.flush()

    def dequeue(self):
        self.q.seek(0)
        first_msg = None
        second_msg = None
        data = b''
        first_msg_stops_at = -1
        second_msg_stops_at = -1
        idx = self.q.read(INDEX_CHUNK + MSG_END_TOKEN_SIZE)
        if idx:
            idx = int(idx.rstrip(NUL_BYTE + MSG_END_TOKEN))
            self.q.seek(idx)
        else:
            return None

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
                first_msg = data[0:first_msg_stops_at]
                if second_msg_stops_at != -1:
                    #updates index pointing to next message
                    self.q.seek(0)
                    self.q.write(bytes(str(idx + len(first_msg) + MSG_END_TOKEN_SIZE), encoding='utf-8'))
                    self.q.flush()
                else:
                    # all msgs consumed
                    self.q.seek(0)
                    self.q.truncate()

                return zlib.decompress(first_msg).decode('utf-8')
        return None
