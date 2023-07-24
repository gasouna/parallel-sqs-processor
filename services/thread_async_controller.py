import random
import concurrent.futures as ct
import traceback

class AsyncController:

    def __init__(self, input_queue, output_queue, callback_function):
        self.in_queue = input_queue
        self.out_queue = output_queue
        self.callback = callback_function

    def process_message(self, message):
        response = self.callback(message['Body'])

        message_id = random.randint(15000,50000)

        # TODO: implement processing time and compare to visibility timeout
        self.in_queue.delete_message(message)

        self.out_queue.post_message(
            message_body=response,
            message_group_id=str(message_id), 
            message_deduplication_id=str(message_id)
        )

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
                    done, futures = ct.wait(futures, return_when=ct.FIRST_COMPLETED)
                    futures = list(futures)

                    for f in done:
                        try:
                            result = f.result()  # This will raise an exception if one occurred in the task
                        except Exception as e:
                            print(f'Error occurred in task: {e}')
                            traceback.print_tb(e.__traceback__)