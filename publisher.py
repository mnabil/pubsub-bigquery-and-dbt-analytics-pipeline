from google.cloud import pubsub_v1
import json
import pandas as pd

PROJECT_ID = "sylvan-altar-384802"  # Replace with your GCP project ID

class SharedClient:
    _publisher_client = None

    @classmethod
    def get_publisher_client(cls):
        if cls._publisher_client is None:
            cls._publisher_client = pubsub_v1.PublisherClient()
        return cls._publisher_client

class PubSubPublisher:
    def __init__(self, project_id, topic_name, file_path=None, publisher_client=None):
        self.project_id = project_id
        self.topic_name = topic_name
        self.publisher = publisher_client or SharedClient.get_publisher_client()
        self.topic_path = self.publisher.topic_path(project_id, topic_name)
        self.file_path = file_path
        self.data = None
        if file_path:
            if file_path.endswith('.json'):
                with open(file_path, 'r') as f:
                    self.data = json.load(f)
            elif file_path.endswith('.csv'):
                self.data = pd.read_csv(file_path)
                self.data = self.data.where(pd.notnull(self.data), None).to_dict(orient='records')
                # FIXME: REMOVE
                # import ipdb; ipdb.set_trace();

    def publish_events(self, extra_attributes={}):
        if not self.data:
            raise ValueError("No data to publish. Please provide a valid file path.")
        
        # Add file name as an attribute if file_path is provided
        extra_attributes['publisher'] = self.file_path.split('/')[-1]

        for event in self.data:
            event_data = json.dumps(event).encode('utf-8')
            future = self.publisher.publish(self.topic_path, event_data, **extra_attributes) #FIXME: add ordering key and order events
            print(f"Published message ID: {future.result()}")

if __name__ == "__main__":
    # Initialize shared publisher client
    shared_client = SharedClient.get_publisher_client()

    # publish clickstream events
    topic_name = "clickstream-events"
    clickstream_publisher = PubSubPublisher(PROJECT_ID, topic_name, file_path='Data/clickstream_events.json', publisher_client=shared_client)

    # publish transaction events
    topic_name = "transaction-events"
    transactions_publisher = PubSubPublisher(PROJECT_ID, topic_name, file_path='Data/transactions.csv', publisher_client=shared_client)

    # publish customer support events
    topic_name = "customer-support-tickets"
    customer_support_publisher = PubSubPublisher(PROJECT_ID, topic_name, file_path='Data/customer_support.json', publisher_client=shared_client)

    # publish all events
    clickstream_publisher.publish_events(extra_attributes={"event_source": "clickstream_publisher"})
    transactions_publisher.publish_events(extra_attributes={"event_source": "transaction_publisher"})
    customer_support_publisher.publish_events(extra_attributes={"event_source": "customer_support_publisher"})