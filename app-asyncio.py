from services.asyncio_async_controller import AsyncController
from sqs_utils.sqs_utils import AsyncQueue
import concurrent.futures as cf
import time
import asyncio

queue_attr = {
    'FifoQueue': 'true',
    'ContentBasedDeduplication': 'false',
    'VisibilityTimeout': '0'
}

def callback_function(message):
    text, delay = message.split(":")
    print(f"Received message: {text} \n Sleeping for {delay} seconds")
    time.sleep(int(delay))
    print(f"Processed message: {text}")
    return message + ' - Processed'

if __name__ == "__main__":

    input_queue = AsyncQueue(queue_name='input_queue_async.fifo', queue_attr=queue_attr)
    output_queue = AsyncQueue(queue_name='output_queue_async.fifo', queue_attr=queue_attr)

    async_controller = AsyncController(
        input_queue=input_queue,
        output_queue=output_queue,
        callback_function=callback_function
    )

    asyncio.run(async_controller.start())