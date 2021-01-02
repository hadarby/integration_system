import asyncio
import json
import logging

import PureCloudPlatformClientV2
import websockets
from PureCloudPlatformClientV2 import NotificationsApi, RoutingApi
from PureCloudPlatformClientV2.rest import ApiException

from logic.call_report import CallReport


class CallReportGenesys(CallReport):
    def __init__(self):
        super(CallReportGenesys, self).__init__()
        api_client = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('', '')
        self._queue_id = ""
        self._routing_api = RoutingApi(api_client)
        self._wrapup_codes = self.get_wrapup_code()
        self._notifications_api = NotificationsApi(api_client)
        self._channel = self.create_channel()
        self._pending_call_reports = set()  # OptimalQ get only 1 'pending' call report

    def get_wrapup_code(self):
        """
        Create a dictionary from wrapup code to the wrapup name
        :return: wrapup_codes_dict
        """
        wrapup_codes = self._routing_api.get_routing_wrapupcodes()
        wrapup_codes_detail_dict = wrapup_codes.to_dict()
        wrapup_codes_dict = {}

        for entity in wrapup_codes_detail_dict['entities']:
            wrapup_codes_dict[entity['id']] = entity['name']

        return wrapup_codes_dict

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
        conversations_topic_id = "v2.routing.queues.{}.conversations.calls".format(self._queue_id)
        channel_topic = PureCloudPlatformClientV2.ChannelTopic()
        channel_topic.id = conversations_topic_id
        notification_subscription = self._notifications_api. \
            put_notifications_channel_subscriptions(self._channel.id, [channel_topic])
        logging.info(notification_subscription)

        return conversations_topic_id

    async def listen_to_websocket(self, conversations_topic_id):
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
                elif message['topicName'].lower() != conversations_topic_id:
                    logging.exception("Unexpected notification:", message)
                else:
                    # filter each incoming interactions
                    if len(message['eventBody']['participants']) < 2:
                        # we don't have yet the contact id
                        continue

                    agent = message['eventBody']['participants'][0]
                    call_result = agent.get('wrapup', {}).get('code', 'pending')
                    message['call_result'] = call_result
                    conversation_id = message['eventBody']['id']

                    if conversation_id in self._pending_call_reports:
                        if call_result != 'pending':
                            self.manage_call_report(message)
                            self._pending_call_reports.remove(conversation_id)
                    else:
                        if call_result == 'pending':
                            self._pending_call_reports.add(conversation_id)
                            self.manage_call_report(message)

    def manage_call_report(self, message):
        """
        Change Genesys format to OptimalQ call report format and send it to self.send_call_report()
        :param message:
        """
        costumer = message['eventBody']['participants'][1]
        call_result = message['call_result']
        status = 'pending'
        lead_id = costumer['attributes']['dialerContactId']
        phone_number = costumer['address'][4:]
        conversation_id = message['eventBody']['id']

        if call_result in self._wrapup_codes:
            call_result = self._wrapup_codes[call_result]
            status = 'final'
        else:
            call_result = 'answer'

        call_report = {
            "externalId": lead_id,
            "phoneNumber": phone_number,
            "result": call_result,
            "status": status,
            "uniqueCallId": conversation_id
        }

        self.send_call_report(call_report)

    def start(self):
        """
        Subscribe to Genesys conversations topic
        async listening to the topic
        """
        conversations_topic_id = self.subscribe_to_conversation()
        grouped_async = asyncio.gather(self.listen_to_websocket(conversations_topic_id))
        asyncio.get_event_loop().run_until_complete(grouped_async)

