from multiprocessing.managers import SharedMemoryManager
from multiprocessing.shared_memory import ShareableList, SharedMemory
from struct import Struct
from ctypes import c_uint

class ShareMemoryStorage:
    capacity: int
    itemsize: int
    shm_manager: SharedMemoryManager
    shm_mem: SharedMemory
    shm_buffer: memoryview
    int_struct: Struct

    def __init__(self) -> None:
        super().__init__()

    @staticmethod
    def create(itemsize: int = 1024, mem: int = 128 * 1024 * 1024) -> "ShareMemoryStorage":
        instance = ShareMemoryStorage()
        instance.capacity = mem // itemsize
        instance.itemsize = itemsize
        instance.shm_manager = SharedMemoryManager()
        instance.shm_manager.start()
        instance.shm_mem = instance.shm_manager.SharedMemory(itemsize * instance.capacity)
        # instance.shm_mem = SharedMemory(create=True, size=itemsize * instance.capacity)
        instance.shm_buffer = instance.shm_mem.buf
        instance.shm_buffer[:] = bytearray(itemsize * instance.capacity)
        instance.int_struct = Struct("I")
        return instance
    
    def shutdown(self) -> None:
        self.shm_manager.shutdown()
        # self.shm_mem.close()
        # self.shm_mem.unlink()

    def put(self, offset: int, data: bytes) -> None:
        data_len = len(data)
        writing_pointer = (offset % self.capacity) * self.itemsize
        self.int_struct.pack_into(self.shm_buffer, writing_pointer, data_len)
        self.shm_buffer[writing_pointer + 4 : writing_pointer + data_len + 4] = data

    def get(self, offset: int) -> memoryview:
        reading_pointer = (offset % self.capacity) * self.itemsize
        data_length = c_uint.from_buffer(self.shm_buffer, reading_pointer).value
        return self.shm_buffer[reading_pointer + 4: reading_pointer + data_length + 4]