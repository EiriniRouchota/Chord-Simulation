import hashlib
from numpy import uint32, frombuffer, dtype, uint8

def hash4Bytes(array_of_bytes):
    # print(hash4Bytes(np.random.randint(0, 255, 4)))
    hash_code = uint32(frombuffer(bytearray(array_of_bytes), dtype=uint32)[0] / 2)
    return hash_code

def hash_common(mstr):
    hashcode1 = str(int(hashlib.sha1(mstr.encode()).hexdigest(), base=16))
    gr = [uint8(int(hashcode1[0:12])  % (2**8)),\
            uint8(int(hashcode1[12:24]) % (2**8)),\
            uint8(int(hashcode1[24:36]) % (2**8)),\
            uint8(int(hashcode1[36:48]) % (2**8))]
    return hash4Bytes(gr)

def hash_key(key):
    return hash_common(str(key))