**WARNING: Alpha code under development**

# XBCF: eXtensible Block Compression Format

XBCF is a container format designed for indexed block compression on Amazon S3. It's designed to support the following features:
* Configurable compression codec
* Configurable block encryption codec
* Seeking support, with granularity equal to the compression block size (1MB by default)
* Recursive encapsulation of XBCF containers
  * Multiple worker nodes can independently upload parts of the same multi-part upload, each formatted as XBCF
  * At close time, a higher-level XBCF is concatenated from the parts

## Dependencies
This proof-of-concept implementation works in Python.
```
pip install snappy lz4 numpy boto
```

## Format specification
* Magic header (8 bytes) == "XBCF" (0-padded)
* Level (8 bytes) (unsigned int; if level is 0, blocks are raw data; otherwise, blocks are themselves XBCF-formatted)
* Compression method (8 bytes) == "LZ4" (0-padded)
* Reserved (8 bytes) (padding to 32 bytes)
* Cipher name (16 bytes) == "AES-CBC" or "null" (0-padded)
* IV (16 bytes)
(If cipher != "null", all subsequent data is encrypted.)
* Block size (uncompressed) (unsigned int; 8 bytes); if size is 0, block size is variable and block size table is present
* Number of blocks (unsigned int; 8 bytes)
* Reserved (16 bytes) (padding to 32 bytes)
* Block start offset table (8 bytes * (Number of blocks))
If block size is 0:
    * Block size table (uncompressed sizes) (8 bytes * (Number of blocks))
* Blocks
