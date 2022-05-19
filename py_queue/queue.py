from multiprocessing import Process
from multiprocessing import RawValue, Value, Lock, RLock
from multiprocessing.shared_memory import SharedMemory
from multiprocessing.synchronize import Lock as LockType, RLock as RLockType
from multiprocessing.sharedctypes import Synchronized, SynchronizedBase
from orjson import dumps, loads
import time
from typing import List, Final, Callable, Optional, Union, Type
from ctypes import c_ulonglong, c_bool
from mypy_extensions import mypyc_attr
from struct import Struct

from .storage import ShareMemoryStorage

WRITE_SEQUENCE_START: Final = 0
READ_SEQUENCE_START: Final = 1
CONDITION_WAIT_DURATION: Final = 1e-12
# HandlerFn = Callable[["Context", int, object], None]
HandlerFn = Callable[..., None]
SetupFn = Callable[[], "Context"]

class Queue:
    offset: c_ulonglong
    capacity: int
    storage: ShareMemoryStorage
    consumers: List["Consumer"]

    def __init__(self, storage: ShareMemoryStorage) -> None:
        self.offset = RawValue(c_ulonglong, WRITE_SEQUENCE_START)
        self.storage = storage
        self.capacity = self.storage.capacity
        self.consumers = []

    def get(self, offset: int) -> object:
        return self._get(offset)

    def _get(self, offset: int) -> object:
        raw_data = self.storage.get(offset)
        return loads(raw_data)

    def put(self, data: object) -> None:
        expected_offset = self.offset.value + 1
        self._put(expected_offset, data)
        self.commit(expected_offset)

    def commit(self, offset: int) -> None:
        self.offset.value = offset

    def _put(self, expected_offset: int, data: object) -> None:
        is_full = True
        while is_full:
            slowest_consumer_offset = self.get_slowest_consumer_offset()
            is_full = (expected_offset - slowest_consumer_offset + 1) >= self.capacity
            
        self.storage.put(expected_offset, dumps(data))

    def register_handler(self, handler_fn: HandlerFn, setup_fn: Optional[SetupFn] = None) -> None:
        consumer = Consumer(self, handler_fn, setup_fn)
        self.consumers.append(consumer)

    def get_slowest_consumer_offset(self) -> int:
        slowest = self.consumers[0].offset.value
        for consumer in self.consumers[1:]:
            if slowest > consumer.offset.value:
                slowest = consumer.offset.value
        
        return slowest

    def start_consumers(self) -> None:
        for consumer in self.consumers:
            consumer.start()

    def stop(self) -> None:
        self._stop_consumers()

    def _stop_consumers(self) -> None:
        for consumer in self.consumers:
            consumer.stop()

class Consumer:
    queue: Queue
    offset: c_ulonglong
    proc: Process
    handler_fn: "staticmethod[None]"
    setup_fn: "staticmethod[Context]"
    ctx: "Context"

    def __init__(self, queue: Queue, handler_fn: HandlerFn, setup_fn: Optional[SetupFn] = None):
        self.queue = queue
        self.handler_fn = staticmethod(handler_fn).__func__
        if setup_fn:
            self.setup_fn = staticmethod(setup_fn).__func__
        self.offset = RawValue(c_ulonglong, READ_SEQUENCE_START)
        self.proc = Process(target=self._start)

    def poll(self) -> None:
        while True:
            while self.offset.value > self.queue.offset.value:
                time.sleep(CONDITION_WAIT_DURATION)
                pass
            producer_offset = self.queue.offset.value
            for i in range(self.offset.value, producer_offset + 1):
                data = self.queue.get(i)
                self.handler_fn(self.ctx, i, data)
                self.offset.value += 1
    
    def _start(self) -> None:
        if getattr(self, "setup_fn"):
            self.ctx = self.setup_fn()
        self.poll()

    def start(self) -> None:
        self.proc.start()

    def stop(self) -> None:
        self.proc.terminate()

@mypyc_attr(allow_interpreted_subclasses=True)
class Context:
    def __init__(self) -> None:
        pass

class MultiProducerQueue(Queue):
    multi_producer_offset: "Synchronized[int]"
    # multi_producer_offset: c_ulonglong
    # lock: LockType
    # lock: RLockType
    commit_list_shm: SharedMemory
    commit_list_buf: memoryview

    def __init__(self, storage: ShareMemoryStorage) -> None:
        super().__init__(storage)
        self.multi_producer_offset = Value(c_ulonglong, WRITE_SEQUENCE_START, lock=True) # type: ignore
        # self.multi_producer_offset = RawValue(c_ulonglong, WRITE_SEQUENCE_START)
        # self.lock = Lock()
        # self.lock = RLock()
        self.commit_list_shm = SharedMemory(create=True, size=self.capacity)
        self.commit_list_buf = self.commit_list_shm.buf
        self.commit_list_buf[:] = bytearray(self.capacity)
    
    def get(self, offset: int) -> object:
        # while not c_bool.from_buffer(self.commit_list_buf, offset).value:
        while not self.commit_list_buf[offset % self.capacity]:
            pass
        return self._get(offset)

    def put(self, data: object) -> None:
        with self.multi_producer_offset.get_lock():
        # with self.lock:
            # expected_offset = self.offset.value + 1
        # self._put(expected_offset, data)
            # self.multi_producer_offset.get_lock().acquire(block=True)
            # self.lock.acquire(block=True)
            expected_offset = self.multi_producer_offset.value + 1
            self.multi_producer_offset.value = expected_offset
            self.commit_list_buf[expected_offset % self.capacity] = 0
            # # self.multi_producer_offset.get_lock().release()
            # # self.lock.release()
        # self.commit_list_buf[expected_offset % self.capacity] = 0
        self._put(expected_offset, data)
        self.commit(expected_offset)

    def commit(self, offset: int) -> None:
        self.commit_list_buf[offset % self.capacity] = 1
        self.offset.value = offset

    def stop(self) -> None:
        self._stop_consumers()
        self.commit_list_shm.close()
        self.commit_list_shm.unlink()