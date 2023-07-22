import asyncio
import concurrent.futures as cf
import random

class AsyncController:

    def __init__(self, input_queue, output_queue, callback_function):
        self.in_queue = input_queue
        self.out_queue = output_queue
        self.callback = callback_function

    async def process_message(self):
        loop = asyncio.get_running_loop()

        input_message = self.in_queue.get_message()

        if "Messages" in input_message:
            message = input_message["Messages"][0]

            response = await loop.run_in_executor(
                None,
                self.callback,
                message['Body']
            )

            message_id = random.randint(5000,10000)

            self.out_queue.post_message(
                message_body=response,
                message_group_id=str(message_id), 
                message_deduplication_id=str(message_id)
            )

            self.in_queue.delete_message(message) 

    async def start(self):
        while True:
            task = asyncio.create_task(self.process_message())
            await asyncio.sleep(1)
