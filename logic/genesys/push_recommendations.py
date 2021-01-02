import json
import logging

import PureCloudPlatformClientV2

from logic.push_recommendations import PushRecommendations


class PushRecommendationsGenesys(PushRecommendations):
    def __init__(self, group, recommendations_topic):
        super(PushRecommendationsGenesys, self).__init__(group, recommendations_topic)
        # Authenticate with Genesys Cloud
        api_client = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('', '')
        # Create an instance of the API class
        self._outbound_api = PureCloudPlatformClientV2.OutboundApi(api_client)
        self._campaign_id = ''
        self._contact_list_id = ""

    def push_recommendations(self, recommendations):
        """
        Get OptimalQ recommendations and push them to the Genesys.
        :param recommendations:
        """
        optimal_leads = self.change_to_genesys_format(recommendations)
        self._outbound_api.post_outbound_contactlist_contacts(self._contact_list_id, body=optimal_leads)
        logging.info("Pushed Leads to Genesis")
        self._outbound_api.delete_outbound_campaign_progress(self._campaign_id)
        logging.info("Reset Genesys campaign")

    def change_to_genesys_format(self, recommendations):
        """
        Get OptimalQ response of optimal leads and change it to the right format for Genesys -
        list of dict for every lead
        The priority will be by the order of optimal leads from OptimalQ.
        :param recommendations:
        :return: optimal_leads_genesys_format
        """
        optimal_leads_genesys_format = []
        priority = 0

        for recommendation in recommendations['response']['leads']:
            lead_id = recommendation['externalId']
            phone_number = recommendation['phoneNumber']
            lead_info = json.loads(recommendation['comment'])
            optimal_leads_genesys_format.append({
                "id": lead_id,
                "data": {
                    "phonenumber": phone_number,
                    "name": lead_info['name'],
                    "zip_code": lead_info['zip_code'],
                    "age": lead_info['age'],
                    "Priority": priority
                }
            })
            priority += 1

        logging.info("Successfully changed recommendations format")

        return optimal_leads_genesys_format
