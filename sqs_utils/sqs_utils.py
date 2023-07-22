import boto3

class Queue():

    def __init__(self, queue_name: str, queue_attr: dict = None):
        self.sqs_client = boto3.client('sqs')

        try:
            self.queue_url = self.sqs_client.get_queue_url(
                QueueName = queue_name
            )['QueueUrl']

        except self.sqs_client.exceptions.QueueDoesNotExist:
            print(f"The queue doesn't exist. Creating new queue with the name: {queue_name}")

            try:
                self.queue_url = self.sqs_client.create_queue(
                    QueueName = queue_name,
                    Attributes = queue_attr
                )['QueueUrl']
            except Exception as excp:
                print(f"Failed to create the SQS queue: {excp}")
            
        except Exception as excp:
            print(f"Not implemented exception: {excp}")

    def post_message(self, message_body: str, message_group_id: str, message_deduplication_id):
        return self.sqs_client.send_message(
            QueueUrl = self.queue_url,
            MessageBody = message_body,
            MessageGroupId = message_group_id,
            MessageDeduplicationId = message_deduplication_id
        )
    
    def get_message(self, max_number_messages  = 1, visibility_timeout = 20, wait_time = 10):
        return self.sqs_client.receive_message(
            QueueUrl = self.queue_url,
            MaxNumberOfMessages = max_number_messages,
            VisibilityTimeout = visibility_timeout,
            WaitTimeSeconds = wait_time
        )
    
    def delete_message(self, message: dict):
        return self.sqs_client.delete_message(
            QueueUrl = self.queue_url,
            ReceiptHandle = message['ReceiptHandle']
        )