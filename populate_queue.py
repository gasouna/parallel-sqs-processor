from sqs_utils.sqs_utils import Queue
import random

messages = ['Testing ' + str(i) + ':' + str(random.randint(2,5)) for i in range(100)]

in_queue = Queue(queue_name='input_queue.fifo')
in_queue_1 = Queue(queue_name='input_queue_1.fifo')

for message in messages:
    message_id = random.randint(1000,10000)
    in_queue.post_message(
        message_body=message,
        message_group_id=str(message_id),
        message_deduplication_id=str(message_id)
    )
    in_queue_1.post_message(
        message_body=message,
        message_group_id=str(message_id),
        message_deduplication_id=str(message_id)
    )