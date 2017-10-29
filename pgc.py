
import socket
import struct


def pg_encode_startup_message(params):
    buf = bytearray()
    buf = buf + struct.pack('>I', 0x00030000)  # version 3.0
    for (key, value) in params:
        buf += bytes(key, 'utf-8')
        buf.append(0)
        buf += bytes(value, 'utf-8')
        buf.append(0)
    buf.append(0)

    out = struct.pack('>I', len(buf) + 4)
    out += buf
    return out


def pg_encode_query(query):
    out = bytearray([0x51])
    query_bin = bytes(query, 'utf-8')
    out += struct.pack('>I', len(query_bin) + 4 + 1)
    out += query_bin
    out.append(0)
    return out


class pgc:
    def __init__(self, server):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect(server)

    def readall(self, n):
        buf = bytes()
        while len(buf) < n:
            buf += self.socket.recv(n-len(buf))
        return buf

    def read_frame(self):
        lenbin = self.readall(5)
        (typ, datalen) = struct.unpack('>BI', lenbin)
        # print('header type=0x{0:x} len={1} <{2}>'.format(typ, datalen, op_code_to_text(typ)))
        data = self.readall(datalen-4)
        return typ, data

    def handle_row_description(self):
        while True:
            (frame_type, frame_data) = self.read_frame()
            if frame_type == ord('T'):
                row_description_bin = frame_data
                break
            if frame_type == ord('E'):
                message = self.decode_error(frame_data)
                raise Exception(message)
            if frame_type == ord('C'):
                return []

        fc_bin = row_description_bin[:2]
        (fc,) = struct.unpack('>H', fc_bin)
        data = row_description_bin[2:]
        row_desc = []
        for idx in range(0, fc):
            col_name_arr = data.split(b'\x00', 1)
            col_name = col_name_arr[0].decode('utf-8')
            data = col_name_arr[1][18:]
            row_desc.append(col_name)
        return row_desc

    @staticmethod
    def handle_row_data(row_bin, row_desc):
        fc_bin = row_bin[:2]
        (fc,) = struct.unpack('>H', fc_bin)
        data = row_bin[2:]
        row_data = {}
        for idx in range(0, fc):
            col_len_bin = data[:4]
            (col_len,) = struct.unpack('>I', col_len_bin)
            col_data_bin = data[4:col_len+4]
            data = data[col_len+4:]
            row_data[row_desc[idx]] = col_data_bin.decode('utf-8')
        return row_data

    def handle_query_response(self):
        row_desc = self.handle_row_description()
        if len(row_desc) == 0:
            return []
        rows = []
        while True:
            (frame_type, frame_data) = self.read_frame()
            if frame_type == ord('C'):
                break
            if frame_type == ord('D'):
                row_data = self.handle_row_data(frame_data, row_desc)
                rows.append(row_data)
            if frame_type == ord('E'):
                message = self.decode_error(frame_data)
                raise Exception(message)
        return rows

    @staticmethod
    def read_zero_term(input):
        split = input.split(b'\x00', 1)
        return split[0].decode('utf-8'), split[1]

    def decode_error(self, data):
        message = ""
        while len(data) > 0:
            typ = data[0]
            data = data[1:]
            if typ == 0:
                break
            elif typ == ord('M'):
                (message, data) = self.read_zero_term(data)
            else:
                (_, data) = self.read_zero_term(data)
        return message

    def query(self, q):
        self.socket.sendall(pg_encode_query(q))
        return self.handle_query_response()

    def init(self, options):
        options.append(('client_encoding', 'UTF8'))
        self.socket.sendall(pg_encode_startup_message(options))

        while True:
            (msg_type, data) = self.read_frame()
            if msg_type == ord('Z'):
                return
            if msg_type == ord('E'):
                message = self.decode_error(data)
                raise Exception(message)
