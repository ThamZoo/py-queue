from dataclasses import dataclass
from py_queue import ShareMemoryStorage
from multiprocessing.managers import SharedMemoryManager
import time
import timeit
# import orjson
import struct
import json

NUM = int(1e5)
SIZE = int(1e3)
REPEAT = 5

# int_struct = struct.Struct("I")

# # now = time.time()

# def bench_storage(storage: ShareMemoryStorage) -> None:
#     for i in range(NUM):
#         storage.put(i, b"123")
#         storage.get(i)

# def bench_raw_storage(shm_buffer, capacity, itemsize):
#     for offset in range(NUM):
#         data = b"123"
#         data_len = len(data)
#         writing_pointer = (offset % capacity) * itemsize
#         int_struct.pack_into(shm_buffer, writing_pointer, data_len)
#         shm_buffer[writing_pointer + 4 : writing_pointer + data_len + 4] = data

# storage = ShareMemoryStorage.create()
# bench_time = timeit.timeit("bench_storage(storage)", number=REPEAT, globals=globals())
# print(f"ShareMemoryStorage: {bench_time} | avg ops: {NUM / bench_time: .0f} ops/s")

# bench_time = timeit.timeit("bench_raw_storage(storage.shm_buffer, storage.capacity, storage.itemsize)", number=REPEAT, globals=globals())
# print(f"Raw memoryview buffer: {bench_time} | avg ops: {NUM / bench_time: .0f} ops/s")

# def bench_derser_():
#     import orjson
#     for i in range(NUM):
#         orjson.loads(orjson.dumps({"abc": 1}))

# bench_time = timeit.timeit("bench_derser_()", number=REPEAT, globals=globals())
# print(f"raw orjson derser: {bench_time} | avg ops: {NUM / bench_time: .0f} ops/s")


# @dataclass
# class A:
#     abc: int

# a_struct = struct.Struct("q")

# def struct_pack(data: A) -> bytes:
#     return a_struct.pack(data.abc)

# def struct_unpack(data: bytes) -> A:
#     return A(*a_struct.unpack(data))

# def bench_struct():
#     a = A(1)
#     for i in range(NUM):
#         struct_unpack(struct_pack(a))

# bench_time = timeit.timeit("bench_struct()", number=REPEAT, globals=globals())
# print(f"simple struct: {bench_time} | avg ops: {NUM / bench_time: .0f} ops/s")

# def bench_derser_json():
#     for i in range(NUM):
#         json.loads(json.dumps({"abc": 1}))

# bench_time = timeit.timeit("bench_derser_json()", number=REPEAT, globals=globals())
# print(f"standard json: {bench_time} | avg ops: {NUM / bench_time: .0f} ops/s")


MEM_SIZE = 512 * 1024 * 1024
# MEM_SIZE = 4096

def bench_allocate_bytearray_x():
    manager = SharedMemoryManager()
    manager.start()
    shm = manager.SharedMemory(MEM_SIZE)
    shm.buf[:] = bytearray(MEM_SIZE)
    manager.shutdown()

bench_time = timeit.timeit("bench_allocate_bytearray_x()", number=REPEAT, globals=globals())
print(f"bench_allocate_bytearray_x: {bench_time} for {MEM_SIZE / 1024 / 1024:.0f} MB ")

def bench_allocate_bytes():
    manager = SharedMemoryManager()
    manager.start()
    shm = manager.SharedMemory(MEM_SIZE)
    shm.buf[:] = bytearray(b"\x00"*MEM_SIZE)
    manager.shutdown()

bench_time = timeit.timeit("bench_allocate_bytes()", number=REPEAT, globals=globals())
print(f"bench_allocate_bytes: {bench_time} for {MEM_SIZE / 1024 / 1024:.0f} MB ")

def bench_allocate_bytes_loop():
    manager = SharedMemoryManager()
    manager.start()
    shm = manager.SharedMemory(MEM_SIZE)
    a = bytearray(4096)
    i=-1
    for i in range(MEM_SIZE // 4096):
        shm.buf[i*4096:i*4096+4096] = a
    shm.buf[(i+1)*4096:] = bytearray(MEM_SIZE % 4096)
    manager.shutdown()

bench_time = timeit.timeit("bench_allocate_bytes_loop()", number=REPEAT, globals=globals())
print(f"bench_allocate_bytes_loop: {bench_time} for {MEM_SIZE / 1024 / 1024:.0f} MB ")