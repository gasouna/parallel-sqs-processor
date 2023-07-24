from sqs_utils.sqs_utils import Queue
import random

messages = ['Testing ' + str(i) + ':' + str(random.randint(2,5)) for i in range(100)]

queue_attr = {
    'FifoQueue': 'true',
    'ContentBasedDeduplication': 'false',
    'VisibilityTimeout': '0'
}

in_queue = Queue(queue_name='input_queue.fifo', queue_attr=queue_attr)
in_queue_1 = Queue(queue_name='input_queue_1.fifo', queue_attr=queue_attr)
out_queue = Queue(queue_name='output_queue.fifo', queue_attr=queue_attr)
out_queue_1 = Queue(queue_name='output_queue_1.fifo', queue_attr=queue_attr)

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