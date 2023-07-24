import asyncio
import concurrent.futures as cf
import random
import traceback

class AsyncController:

    def __init__(self, input_queue, output_queue, callback_function):
        self.in_queue = input_queue
        self.out_queue = output_queue
        self.callback = callback_function

    async def process_message(self, message):
        response = self.callback(message['Body'])

        message_id = random.randint(5000,10000)

        # TODO: implement processing time and compare to visibility timeout
        await self.in_queue.delete_message(message) 
        
        await self.out_queue.post_message(
            message_body=response,
            message_group_id=str(message_id), 
            message_deduplication_id=str(message_id)
        )

    async def start(self):
        while True:
            response = await self.in_queue.get_message(max_number_messages=10)  # 10 is the maximum amount of messages that can be fetched in a single request

            if 'Messages' in response:  # check if any messages were returned
                for msg in response['Messages']:
                    asyncio.create_task(self.process_message(msg))

            else:
                await asyncio.sleep(1)  # if no messages were found, wait for 1 second before polling the queue again
