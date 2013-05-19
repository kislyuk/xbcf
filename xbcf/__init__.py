'''
Note: this demo implementation pumps data from LZ4 to AES in Python strings. Since both modules are interfaces to C
libraries which are heavily optimized, there may be a big speedup in connecting them at a lower level.

'''

import os, sys, string, random, struct
from Crypto.Cipher import AES
import lz4
import snappy
import cStringIO as StringIO
import numpy
import boto, boto.s3.multipart

codecs = {
    'LZ4': (lz4.dumps, lz4.loads),
    'sna': (snappy.compress, snappy.decompress)
}

def string_buffer_length(buf):
    orig_pos = buf.tell()
    buf.seek(0, os.SEEK_END)
    buf_len = buf.tell()
    buf.seek(orig_pos)
    return buf_len


#aes_header = struct.pack("3s5xL3s5x7s1xQ", "BCF", 0, "LZ4", "AES-CBC", iv)
#header = struct.pack("4s4xL3s5x7s1xQ", "XBCF", 0, "LZ4", "null", iv)

'''
TODO:
* Patch boto (git@github.com:kislyuk/boto.git) to allow presenting a S3 key as a file-like object

* Make benchmark on boto file
  * raw
  * lz4
  * snappy
  * lz4+aes
  * snappy+aes
Size: 24G uncompressed

level 2 assembler
  mode 1: take array of filehandles, find their lengths and write concatenation
    - see if a filesystem concat api is available?
  mode 2: integrated with s3 multipart close - generate part for index
    - Q: how to integrate with boto
level 2 reader
    - keeps track of which block pos is on, as pos moves between blocks, reads in the next one

being handled as part of main class below
'''

class XBCFFile(object):
    def __init__(self, fh, mode='r', offset=0, level=0, block_size=1024*1024, table_size=1024, codec='LZ4'):
        self.fh = fh
        self.mode = mode
        self.offset = offset
        self.level = level
        self.block_size = block_size
        self.table_size = table_size
        self.first_block_offset = None
        self.compressed_block_sizes = numpy.zeros((table_size,), dtype=numpy.uint64)
        self.compressed_block_offsets = None
        self.uncompressed_block_sizes = numpy.zeros((table_size,), dtype=numpy.uint64)
        self.uncompressed_block_offsets = None
        self.compressor, self.decompressor = None, None
        self.pos = 0
        self.cur_block_id = 0
        self._read_buf = StringIO.StringIO()
        self._write_buf = StringIO.StringIO()
        self._uncommitted_head = []

        self.block_head_fmt = "I"
        self.block_head_len = struct.calcsize(self.block_head_fmt)

        header_format = "4s4x"+"Q"+"3s5x"+"8x"+"4s12x"+"16s"+"QQ"+"16x"
        if mode == 'w':
            self.first_block_offset = 32*3 + self.table_size*8
            if level > 0:
                self.block_size = 0
                self.first_block_offset += self.table_size*8

            self.compressor, self.decompressor = codecs[codec]
            cipher = "null"
            iv = "iv"*8
            header = struct.pack(header_format, "XBCF", level, codec, cipher, iv, block_size, table_size)
            assert(len(header) == 32*3)

            table = struct.pack("{ts}x".format(ts=table_size*8))
            if isinstance(self.fh, boto.s3.multipart.MultiPartUpload):
                self._uncommitted_head.append(header)
                self._uncommitted_head.append(table)
            else:
                self.fh.write(header)
                self.fh.write(table)
                if self.block_size == 0:
                    self.fh.write(table) # uncompressed_block_offsets table
        elif mode == 'r':
            if offset != self.fh.tell():
                print "seeking to", offset
                self.fh.seek(offset)
            header = self.fh.read(32*3)
            format, level, codec, cipher, iv, block_size, table_size = struct.unpack(header_format, header)
            print "Setting level to", level
            assert(format == "XBCF" and cipher == "null")
            self.level = level
            self.compressor, self.decompressor = codecs[codec]
            self.block_size = block_size
            self.table_size = table_size
            self.first_block_offset = 32*3 + self.table_size*8
            if level > 0:
                self.first_block_offset += self.table_size*8

            self.compressed_block_offsets = numpy.fromstring(self.fh.read(table_size), dtype=numpy.uint64)
            # print "CBO:\n", self.compressed_block_offsets
            self.compressed_block_sizes = numpy.diff(self.compressed_block_offsets)

            self.uncompressed_block_sizes = None
            if self.block_size == 0:
                self.uncompressed_block_sizes = numpy.fromstring(self.fh.read(table_size), dtype=numpy.uint64)
            # print "CBS:\n", self.compressed_block_sizes
            # uncompressed file length
            self._file_length = self._get_size()
            # The code below computes the compressed length
            # To compute the uncompressed length, we need to keep the uncompressed size of the last block
            # In containerized LZ4, that number is stored before the block, but this may not generalize to other formats
            #self._compressed_file_length = int(self.compressed_block_sizes.sum())

            if self.level > 0:
                self.seek(0)
        else:
            raise Exception("unknown mode")

    def __len__(self):
        return self._file_length

    def write(self, data):
        self._write_buf.write(data)

    # TODO: part accumulator
    def add_part(self, part):
        assert(self.level > 0)
        part_contents = StringIO.StringIO()
        part_contents.write(struct.pack(self.block_head_fmt, part._file_length))
        orig_pos = part.fh.tell()
        part.fh.seek(0)
        part_contents.write(part.fh.read())
        part.fh.seek(orig_pos)
        part_contents.seek(0)

        if isinstance(self.fh, boto.s3.multipart.MultiPartUpload):
            if self.cur_block_id == 0:
                self._uncommitted_head.append(part_contents.getvalue())
            else:
                self.fh.upload_part_from_file(part_contents, self.cur_block_id+1)
        else:
            self.fh.write(part_contents.getvalue())

        self.compressed_block_sizes[self.cur_block_id] = string_buffer_length(part_contents)
        if self.block_size == 0:
            self.uncompressed_block_sizes[self.cur_block_id] = part._file_length
        self.cur_block_id += 1

    # Finish this
    # We can't seek while writing, so we must know the table before we start writing the blocks.
    # To avoid caching all the blocks in memory, we will need to use memory-mapped or temporary file-based caching.
    def write2(self, data):
        remaining_space = self.block_size - self._write_buf.tell()

        if len(data) <= remaining_space:
            self._write_buf.write(data)
        else:
            self._write_buf.write(data[:remaining_space])

            temp_data = self._write_buf.getvalue()
            self._write_buf = StringIO.StringIO()
            self._commit_block()

            self.write(data[remaining_space:], **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def _write_offset_table(self):
        self.compressed_block_offsets = numpy.cumsum(self.compressed_block_sizes)
        table_bytes = self.compressed_block_offsets.tostring()
        if self.block_size == 0:
            self.uncompressed_block_offsets = numpy.cumsum(self.uncompressed_block_sizes)
            table_bytes += self.compressed_block_offsets.tostring()
        #print "BST", self.compressed_block_sizes
        #print "Writing offset table", self.compressed_block_offsets
        assert(len(self.compressed_block_offsets.tostring()) == self.table_size*8)
        if isinstance(self.fh, boto.s3.multipart.MultiPartUpload):
            self._uncommitted_head[1] = table_bytes
        else:
            self.fh.seek(32*3)
            self.fh.write(table_bytes)

    def _get_size(self):
        if self.mode == 'r':
            # TODO: make this not suck
            for block_id in range(len(self.compressed_block_sizes)):
                if self.compressed_block_sizes[block_id] == 0:
                    orig_fh_pos = self.fh.tell()
                    assert(block_id > 0) # deal with this later and abstract out the part of read_block that also deals with this
                    self.fh.seek(self.offset + self.first_block_offset + self.compressed_block_offsets[block_id-1])
                    print "BHL:", self.block_head_len
                    last_block_size = struct.unpack(self.block_head_fmt, self.fh.read(self.block_head_len))[0]
                    self.fh.seek(orig_fh_pos)
                    return self.block_size*block_id + last_block_size
        else:
            raise Exception("can't tell size in write mode")

    def close(self):
        if self.mode == 'w' and self.level > 0:
            self._write_offset_table()
            if isinstance(self.fh, boto.s3.multipart.MultiPartUpload):
                first_part = StringIO.StringIO()
                for chunk in self._uncommitted_head:
                    first_part.write(chunk)
                first_part.seek(0)
                self.fh.upload_part_from_file(first_part, 1)
        elif self.mode == 'w':
            self.fh.seek(self.first_block_offset)
            self._write_buf.seek(0)
            while True:
                raw_block = self._write_buf.read(self.block_size)
                if raw_block == "":
                    self._write_offset_table()
                    break
                compressed_block = self.compressor(raw_block)

                #assert(struct.unpack("I", compressed_block[:4])[0] == len(raw_block))
                print "Compressed block size", len(compressed_block), "original", len(raw_block)
                self.fh.write(struct.pack(self.block_head_fmt, len(raw_block)))
                # crypto goes here
                self.compressed_block_sizes[self.cur_block_id] = len(compressed_block) + self.block_head_len
                if self.block_size == 0:
                    self.uncompressed_block_sizes[self.cur_block_id] = part._file_length
                self.cur_block_id += 1
                self.fh.write(compressed_block)

    def _read_block(self, block_id):
        if block_id > 0:
            block_start = self.compressed_block_offsets[block_id-1]
            #block_len = self.compressed_block_sizes[block_id]
            block_len = int(self.compressed_block_offsets[block_id] - self.compressed_block_offsets[block_id-1])
        else:
            block_start = 0
            block_len = int(self.compressed_block_offsets[block_id])# - block_start
        print "reading block", block_id, "at level", self.level, "start", block_start, "len", block_len

        if self.level > 0:
            if block_len == 0:
                print "Read beyond last nonempty block, returning zero bytes (L>0)"
                return None
            return XBCFFile(self.fh, mode='r', offset=block_start + self.first_block_offset + self.block_head_len)
        else:
            if block_len == 0:
                print "Read beyond last nonempty block, returning zero bytes"
                return ""
            need_pos = int(self.offset + block_start + self.first_block_offset)
            if self.fh.tell() != need_pos:
                print "Seeking to block start position", need_pos, " (this should only happen on random access)"
                self.fh.seek(need_pos)
            print "Reading", block_len, "bytes", "from", self.fh.tell()
            self.fh.read(self.block_head_len)
            return self.decompressor(self.fh.read(block_len-4))

    def seek(self, pos):
        print "Seeking to", pos
        if self.mode == 'w':
            raise Exception("seeking in write mode is not supported")

        self.pos = pos
        old_block_id = self.cur_block_id
        if self.block_size == 0:
            self.cur_block_id = numpy.searchsorted(self.uncompressed_block_offsets, pos)
        else:
            self.cur_block_id = pos / self.block_size
        print "Pos", pos, "is in block", self.cur_block_id, "(level", self.level, "block size", self.block_size, ")"
        if self.level > 0:
            self._read_buf = self._read_block(self.cur_block_id)
            if pos % self.block_size != 0:
                self._read_buf.seek(pos % self.block_size)
        else:
            # FIXME
            # if self.cur_block_id != old_block_id:
            self._read_buf = StringIO.StringIO()
            self._read_buf.write(self._read_block(self.cur_block_id))
            self.cur_block_id += 1
            self._read_buf.seek(pos % self.block_size)

    def read(self, length=None):
        if length == None:
            length = sys.maxint # TODO: make this better
        assert(length > 0)

        if self.level > 0:
            if self._read_buf == None:
                return ""
            data = self._read_buf.read(length)
            if len(data) == length:
                print "Returning", length, "bytes without changing buffers"
                return data
            else:
                assert(len(data) < length)
                buf = StringIO.StringIO()
                buf.write(data)
                length -= len(data)
                print "Reading beyond buffer (", len(data), "bytes), fetching next blocks"
                while length > 0:
                    self.cur_block_id += 1
                    self.pos += self.block_size
                    print "Reading block", self.cur_block_id
                    self._read_buf = self._read_block(self.cur_block_id)
                    if self._read_buf == None:
                        print "Block is null, stopping read"
                        break

                    data = self._read_buf.read(length)
                    if data == "":
                        print "Data is empty, stopping read"
                        break
                    print "Adding", len(data), "bytes to buffer"
                    buf.write(data)
                    length -= len(data)
                buf.seek(0)
                return buf.read()
        else:
            #if self.pos == self._file_length:
            #    return ""
            #if length == None or length > self._file_length - self.pos:
            #    length = self._file_length - self.pos

            buf = self._read_buf
            buf_remaining_bytes = string_buffer_length(buf) - buf.tell()
            if length <= buf_remaining_bytes:
                print "Returning", length, "bytes, changing buffers"
                self.pos += length
                data = buf.read(length)
                assert(len(data) == length)
                return data
            else:
                orig_buf_pos = buf.tell()
                orig_file_pos = self.pos
                buf.seek(0, os.SEEK_END)
                self.pos += buf_remaining_bytes
                while self.pos < orig_file_pos + length:
                    remaining_len = orig_file_pos + length - self.pos
                    print "Remaining len", remaining_len, orig_file_pos, "+", length, "-", self.pos
                    content = self._read_block(self.cur_block_id)
                    print "Read", len(content), "bytes from block", self.cur_block_id
                    self.cur_block_id += 1

                    if len(content) == 0:
                        print "Hit EOF"
                        break
                    elif len(content) < remaining_len:
                        buf.write(content)
                        self.pos += len(content)
                    else:
                        buf.write(content[:remaining_len])
                        self.pos += remaining_len
                        self._read_buf = StringIO.StringIO()
                        self._read_buf.write(content[remaining_len:])
                        self._read_buf.seek(0)
                buf.seek(orig_buf_pos)
                data = buf.read()
                print "Returning", len(data), "bytes"
                return data
