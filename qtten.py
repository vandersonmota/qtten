import os
import zlib
import mmap

COMPRESSION = 9
MSG_END_TOKEN = b'EOM'
NUL_BYTE = b'\x00'
READ_CHUNK = 2048
INDEX_CHUNK = 50
MSG_END_TOKEN_SIZE = 3


class Queue:
    def __init__(self, filename):
        self.filename = filename
        try:
            f = open(self.filename, 'r+b')
        except FileNotFoundError:
            f = open(self.filename, 'w')
        finally:
            f.close()



    def enqueue(self, message):
        with open(self.filename, 'r+b') as q:
            idx = q.read(INDEX_CHUNK + MSG_END_TOKEN_SIZE)
            # emtpy file, writing dummy index
            if not idx:
                q.write(NUL_BYTE * INDEX_CHUNK)
                q.write(MSG_END_TOKEN)
                pos = q.tell()
                q.seek(0)
                q.write(bytes(str(pos), encoding='utf-8'))
            q.seek(0, 2)
            q.write(zlib.compress(bytes(message, encoding='utf-8'), COMPRESSION))
            q.write(MSG_END_TOKEN)


    def dequeue(self):
        with open(self.filename, 'r+b') as q:
            first_msg = None
            second_msg = None
            data = b''
            first_msg_stops_at = -1
            second_msg_stops_at = -1
            idx = q.read(INDEX_CHUNK + MSG_END_TOKEN_SIZE)
            if idx:
                idx = int(idx.rstrip(NUL_BYTE + MSG_END_TOKEN))
                q.seek(idx)
            else:
                return None

            while True:
                chunk = q.read(READ_CHUNK)
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
                        q.seek(0)
                        q.write(bytes(str(idx + len(first_msg) + MSG_END_TOKEN_SIZE), encoding='utf-8'))
                    else:
                        # all msgs consumed
                        q.seek(0)
                        q.truncate()

                    return zlib.decompress(first_msg).decode('utf-8')
            return None
