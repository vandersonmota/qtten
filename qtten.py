import os
import zlib

COMPRESSION = 9
MSG_END_CHAR = b'\n'
NUL_BYTE = b'\x00'


def enqueue(filename, message):
    with open(filename, 'ab') as q:
        q.write(zlib.compress(bytes(message, encoding='utf-8'), COMPRESSION))
        q.write(MSG_END_CHAR)


def dequeue(filename):
    with open(filename, 'r+b') as q:
        first_msg = None
        second_msg = None
        first_msg = q.readline().strip()
        if first_msg:
            second_msg = q.readline().strip()
        if second_msg:
            q.seek(0)
            new_message_size = len(first_msg) + len(second_msg) + 1
            q.write(NUL_BYTE * new_message_size)
            q.write(MSG_END_CHAR)
            q.seek(0)
            q.write(second_msg)
        else:
            q.seek(0)
            q.truncate()

        return zlib.decompress(first_msg)
