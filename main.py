import time
from typing import Dict
import dis
from multiprocessing import current_process
from py_queue import Queue, ShareMemoryStorage, Context, MultiProducerQueue

consumer_start = time.time()
NUM = int(1e6)
NUM_PRODUCER = 2

# Context setup
class SingleProdCtx(Context):
    start_time: float
    last_val: int
    proc_name: str
    queue : MultiProducerQueue

class MultiProdCtx(Context):
    last_val: Dict
    start_time: float

# Queue setup
storage = ShareMemoryStorage.create(itemsize=128 ,mem=128*1024*1024)
single_prod_queue = Queue(storage)

storage2 = ShareMemoryStorage.create(itemsize=128, mem=128*1024*1024)
multi_prod_queue = MultiProducerQueue(storage2)

# Handler setup
def single_prod_handler_setup() -> SingleProdCtx:
    ctx = SingleProdCtx()
    ctx.queue = multi_prod_queue
    ctx.last_val = -1
    ctx.proc_name = current_process().name
    return ctx

def multi_prod_handler_setup() -> MultiProdCtx:
    ctx = MultiProdCtx()
    # ctx.last_val = {"Process-3": -1, "Process-4": -1}
    ctx.last_val = {}
    return ctx

# Handler function
def single_prod_handler(ctx: SingleProdCtx, offset: int, data: object) -> None:
    data["from"] = ctx.proc_name
    ctx.queue.put(data)
    if offset==1:
        ctx.start_time = time.time()
    if offset==NUM-1:
        duration = time.time() - ctx.start_time
        print(f"Single prod handler with multi producer downstream Done in {duration} | avg ops: {NUM/duration:.0f} ops/s")
    pass

def single_prod_handler_no_downstream(ctx: SingleProdCtx, offset: int, data: object) -> None:
    # ctx.queue.put(data)
    # assert ctx.last_val == data["abc"] - 1
    # ctx.last_val = data["abc"]
    if offset==1:
        ctx.start_time = time.time()
    if offset==NUM-1:
        duration = time.time() - ctx.start_time
        print(f"Single prod handler Done in {duration} | avg ops: {NUM/duration:.0f} ops/s")
    pass

def multi_prod_handler(ctx: MultiProdCtx, offset: int, data: object) -> None:
    # ctx.last_val.setdefault(data["from"], -1)
    # try:
    #     assert ctx.last_val[data["from"]] == data["abc"] - 1
    # except AssertionError:
    #     print(f"last_val: {ctx.last_val} | data: {data}")
    # ctx.last_val[data["from"]] = data["abc"]
    if offset==1:
        ctx.start_time = time.time()
    if offset==(NUM-1)*NUM_PRODUCER:
        duration = time.time() - ctx.start_time
        print(f"Multi prod handler | {NUM_PRODUCER} producers | Done in {duration} | avg ops: {NUM*NUM_PRODUCER/duration:.0f} ops/s")
    pass


# Handler registration
for i in range(NUM_PRODUCER):
    single_prod_queue.register_handler(single_prod_handler, single_prod_handler_setup)

single_prod_queue.register_handler(single_prod_handler_no_downstream, single_prod_handler_setup)

multi_prod_queue.register_handler(multi_prod_handler, multi_prod_handler_setup)
# multi_prod_queue.register_handler(multi_prod_handler, multi_prod_handler_setup)

# Start consumers
single_prod_queue.start_consumers()
multi_prod_queue.start_consumers()

# Start producing
data = {"abc": 123}
now = time.time()
for i in range(NUM):
    # data["abc"] = i
    # print("Single producer | producing ", i)
    single_prod_queue.put(data)
    # time.sleep(2)
    
duration = time.time() - now
print(f"Producer Done in {duration} | avg ops: {NUM/duration:.0f} ops/s")

while True:
    try:
        time.sleep(10)
    except KeyboardInterrupt:
        print("Stopping consumers")
        single_prod_queue.stop()
        multi_prod_queue.stop()
        print("Done killing consumers")
        print("Stop shared mem")
        storage.shutdown()
        storage2.shutdown()
        print("Done")
        break