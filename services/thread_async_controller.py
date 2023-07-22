import random
import concurrent.futures as ct

class AsyncController:

    def __init__(self, input_queue, output_queue, callback_function):
        self.in_queue = input_queue
        self.out_queue = output_queue
        self.callback = callback_function

    def process_message(self, message):
        response = self.callback(message['Body'])

        message_id = random.randint(15000,50000)

        self.out_queue.post_message(
            message_body=response,
            message_group_id=str(message_id), 
            message_deduplication_id=str(message_id)
        )
        
        self.in_queue.delete_message(message)

    def start(self, num_threads):
        with ct.ThreadPoolExecutor(num_threads) as executor:
            futures = []

            while True:
                if len(futures) < num_threads:
                    input_message = self.in_queue.get_message()
    
                    if "Messages" in input_message.keys():
                        future = executor.submit(self.process_message, input_message['Messages'][0])
                        futures.append(future)

                else:
                    _, futures = ct.wait(futures, return_when=ct.FIRST_COMPLETED)
                    futures = list(futures)