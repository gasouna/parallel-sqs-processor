from services.asyncio_async_controller import AsyncController
from sqs_utils.sqs_utils import Queue
import concurrent.futures as cf
import time
import random
import asyncio


def callback_function(message):
    text, delay = message.split(":")
    print(f"Received message: {text}")
    time.sleep(delay)
    print(f"Processed message: {text}")
    return message + ' - Processed'

if __name__ == "__main__":

    input_queue = Queue(queue_name='input_queue_1.fifo')
    output_queue = Queue(queue_name='output_queue_1.fifo')

    async_controller = AsyncController(
        input_queue=input_queue,
        output_queue=output_queue,
        callback_function=callback_function
    )

    with cf.ThreadPoolExecutor(max_workers=10) as executor:
        for _ in range(10):
            executor.submit(asyncio.run(async_controller.start()))