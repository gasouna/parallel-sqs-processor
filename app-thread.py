from services.thread_async_controller import AsyncController
from sqs_utils.sqs_utils import Queue
import time
import random


def callback_function(message):
    text, delay = message.split(":")
    print(f"Received message: {text}")
    time.sleep(delay)
    print(f"Processed message: {text}")
    return message + ' - Processed'

if __name__ == "__main__":

    input_queue = Queue(queue_name='input_queue.fifo')
    output_queue = Queue(queue_name='output_queue.fifo')

    async_controller = AsyncController(
        input_queue=input_queue,
        output_queue=output_queue,
        callback_function=callback_function
    )

    async_controller.start(num_threads=10)