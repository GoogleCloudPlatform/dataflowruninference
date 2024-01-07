from google.cloud import bigquery
from concurrent import futures
from google.cloud import pubsub_v1
from typing import Callable
from argparse import ArgumentParser
import sys


def run():
    parser = ArgumentParser()
    parser.add_argument("--project_id", dest="project_id",
                        help="projectId where bigquery dataset/table resides")
    parser.add_argument("--topic_id", dest="topic_id",
                        help="topicId of PubSub where to write the messages to")
    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        sys.exit(0)

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project_id, args.topic_id)
    publish_futures = []
    client = bigquery.Client()

    #Perform a query for a system lag example
    QUERY = (
        'SELECT text from summit2023.GOSU_AI_Dota2_game_chats '
        'WHERE match = 507332 ')


    #Perform a query for a positive sentiment game (LOL, gg) etc
    # QUERY = (
    #     'SELECT text from summit2023.GOSU_AI_Dota2_game_chats '
    #     'WHERE match = 617328 '
    #     'LIMIT 1000')

    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    for row in rows:
        publish_future = publisher.publish(topic_path, row.text.encode("utf-8"))
        print(row.text)
        # Non-blocking. Publish failures are handled in the callback function.
        publish_future.add_done_callback(get_callback(publish_future, row.text))
        publish_futures.append(publish_future)

    # Wait for all the publish futures to resolve before exiting.
    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

    print(f"Published messages with error handler to {topic_path}.")

def get_callback(
    publish_future: pubsub_v1.publisher.futures.Future, data: str
) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
    def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
        try:
            # Wait 60 seconds for the publish call to succeed.
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run()
