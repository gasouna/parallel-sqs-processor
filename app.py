from services.async_controller import AsyncController
from sqs_utils.sqs_utils import Queue
import time

def callback_function(message):
    print(f"Received message: {message}")
    time.sleep(10)
    return message + ' - Processed'

if __name__ == "__main__":

    input_queue = Queue(queue_name='input_queue.fifo')
    output_queue = Queue(queue_name='output_queue.fifo')

    async_controller = AsyncController(
        input_queue=input_queue,
        output_queue=output_queue,
        callback_function=callback_function
    )

    async_controller.start(num_threads=5)