from sqs_utils.sqs_utils import Queue
import random

messages = ['Testing ' + str(i) for i in range(20)]

in_queue = Queue(queue_name='input_queue.fifo')

for message in messages:
    message_id = random.randint(5000,6000)
    in_queue.post_message(
        message_body=message,
        message_group_id=str(message_id),
        message_deduplication_id=str(message_id)
    )