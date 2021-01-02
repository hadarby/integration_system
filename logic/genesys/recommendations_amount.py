import json
import logging

import PureCloudPlatformClientV2
import websockets
from PureCloudPlatformClientV2.rest import ApiException

from logic.recommendations_amount import RecommendationsAmount


class RecommendationsAmountGenesys(RecommendationsAmount):
    def __init__(self, recommendations_amount_topic, ratio=2, buffer=0):
        super(RecommendationsAmountGenesys, self).__init__(recommendations_amount_topic, ratio, buffer)
        # Authenticate with genesys cloud
        api_client = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('', '')
        # Create an instance of the Routing API and Analytics API
        self._routing_api = PureCloudPlatformClientV2.RoutingApi(api_client)
        self._analytics_api = PureCloudPlatformClientV2.AnalyticsApi(api_client)
        # Create an instance of the API class
        self._api_instance = PureCloudPlatformClientV2.AnalyticsApi(api_client)
        self._notifications_api = PureCloudPlatformClientV2.NotificationsApi(api_client)
        self._queue_id = ""
        self._channel = self.create_channel()
        self._conversations_topic_id = self.subscribe_to_conversation()

    def create_channel(self):
        """
        Create a new channel
        :return: new_channel
        """
        new_channel = None

        try:
            new_channel = self._notifications_api.post_notifications_channels()
            logging.info("Created a channel")
        except ApiException as e:
            logging.error("Exception when calling NotificationsApi->post_notifications_channels: {}".format(e))
        return new_channel

    def subscribe_to_conversation(self):
        """
        Subscribe to conversation notifications for the queue
        :return: conversation_topic_id
        """
        conversations_topic_id = "v2.routing.queues.{}.conversations".format(self._queue_id)
        channel_topic = PureCloudPlatformClientV2.ChannelTopic()
        channel_topic.id = conversations_topic_id
        try:
            # Subscribe to conversation notifications for the queue
            notification_subscription = self._notifications_api. \
                put_notifications_channel_subscriptions(self._channel.id, [channel_topic])
            logging.info(notification_subscription)
        except ApiException as ex:
            logging.error("Exception when calling NotificationsApi->put_notifications_channel_subscriptions: {}".format(ex))
        return conversations_topic_id

    async def start(self):
        """
        Open a new web socket using the connect Uri of the channel
        """
        async with websockets.connect(self._channel.connect_uri) as websocket:
            logging.info("Listening to websocket")
            # Message received
            async for message in websocket:
                message = json.loads(message)

                if message['topicName'].lower() == "channel.metadata":
                    continue
                if message['topicName'].lower() != self._conversations_topic_id:
                    logging.exception("Unexpected notification:", message)
                else:
                    # filter each incoming interactions
                    purpose = ([x for x in message['eventBody']['participants'] if x['purpose'] == 'customer'])[0][
                        'purpose']

                    if purpose == 'customer':
                        self.display_queue_observation()

    def display_queue_observation(self):
        """
        Query for queue observations: "oUserRoutingStatuses", "oWaiting".
        Send the results to self.send_recommendations_amount()
        """
        query = {
            "filter": {
                "type": "AND",
                "clauses": [
                    {
                        "type": "or",
                        "predicates": [
                            {
                                "dimension": "queueId",
                                "value": self._queue_id
                            }
                        ]
                    }
                ]
            },
            "metrics": ["oUserRoutingStatuses", "oWaiting"]
        }
        try:
            api_response = self._api_instance.post_analytics_queues_observations_query(query)
            dial = None
            available_agents = None

            for result in api_response.results:
                if result.group.get('mediaType') == 'callback':
                    dial = result.data[0].stats.count
                    logging.info("There are {} leads in DIAL mode".format(dial))

                    continue

                if result.group.get('mediaType', True):
                    for status in result.data:
                        if status.qualifier == 'IDLE':
                            available_agents = status.stats.count
                            logging.info("There are {} available agents".format(available_agents))

                            break

                if dial is not None and available_agents is not None:
                    break

            if dial is not None and available_agents is not None:
                self.send_recommendations_amount({'available_agents': available_agents, 'dial': dial})
        except ApiException as ex:
            logging.error("Exception when calling AnalyticsApi->post_analytics_queues_observations_query: {}".format(ex))


