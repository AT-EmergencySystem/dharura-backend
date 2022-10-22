import json
from google.protobuf.json_format import MessageToJson

from .utils.generated.app_socket_pb2 import (
    AppToServerCommand,
    AppToServerCommandReply,
)
from .utils.generated.common_model_pb2 import (
    CustomerIndex,
    IndexMapping,
    CustomerNumberProvider,
)
from .utils.generated.messaging_model_pb2 import (
    MessagingChannel,
    MessagingConsentUpdate,
    MessageDeliveryStatus
)
from .utils.helpers import has_key, fill_in_outgoing_message, get_enum_value, get_enum_string


class Customer:
    """Customer class that handles a single customer instance, must have one of customer_id or customer_number.

        :param customer_id: The Elarian generated customer id
        :param customer_number: The customer phone number
    """

    _customer_id: str = None
    _customer_number: dict = None

    _identity_state: dict = None
    _messaging_state: dict = None
    _payment_state: dict = None
    _activity_state: dict = None

    _client = None

    def __init__(self, client, id=None, number=None, provider=None):
        self._client = client
        if id is not None:
            self._customer_id = id
        if number is not None:
            provider = (
                provider if provider is not None else 'CELLULAR'
            )
            self._customer_number = {"number": number, "provider": provider}
            self._client._loop.create_task(self._create_customer())

        if self._customer_id is None and self._customer_number is None:
            raise RuntimeError("Either id or number is required")

    def get_id(self):
        return self._customer_id

    def get_number(self):
        return self._customer_number

    async def _create_customer(self):
        req = AppToServerCommand()
        req.create_customer.customer_number.number = self._customer_number.get("number")
        req.create_customer.customer_number.provider = get_enum_value(
            CustomerNumberProvider,
            self._customer_number.get("provider", "CELLULAR"),
            "CUSTOMER_NUMBER_PROVIDER",
        )
        data = await self._send_command(req)
        res = self._parse_reply(data)['update_customer_state']
        if not res['status']:
            raise RuntimeError(res['description'])
        self._customer_id = res['customer_id']
        return res

    async def get_state(self):
        """Used to get the current customer state."""
        req = AppToServerCommand()

        if self._customer_number is not None and has_key("number", self._customer_number):
            req.get_customer_state.customer_number.number = self._customer_number.get("number")
            req.get_customer_state.customer_number.provider = get_enum_value(
                CustomerNumberProvider,
                self._customer_number.get("provider", "CELLULAR"),
                "CUSTOMER_NUMBER_PROVIDER",
            )
        elif self._customer_id is not None:
            req.get_customer_state.customer_id = self._customer_id

        else:
            raise RuntimeError("Invalid customer id and/or customer number")

        data = await self._send_command(req)
        res = self._parse_reply(data)['get_customer_state']

        if not res['status']:
            raise RuntimeError(res['description'])
        res = res['data']
        return res

    async def adopt_state(self, other_customer):
        """Used to adopt another customer's state.

        :param other_customer: Dictionary containing the other customer details
        """
        req = AppToServerCommand()

        if self._customer_id is None:
            await self.get_state()

        req.adopt_customer_state.customer_id = self._customer_id

        if other_customer.get("customer_id") is not None:
            req.adopt_customer_state.customer_id = other_customer.get("customer_id")
        elif other_customer.get("number") is not None and has_key(
            "number", other_customer
        ):
            req.adopt_customer_state.other_customer_number.number = (
                other_customer.get("number")
            )
            req.adopt_customer_state.other_customer_number.provider = get_enum_value(
                CustomerNumberProvider,
                other_customer.get("provider", "CELLULAR"),
                "CUSTOMER_NUMBER_PROVIDER",
            )
        else:
            raise RuntimeError("Invalid customer id and/or customer number")

        data = await self._send_command(req)
        res = self._parse_reply(data)['update_customer_state']

        if not res['status']:
            raise RuntimeError(res['description'])

        return res

    async def send_message(self, messaging_channel: dict, message: dict):
        """Used to send a message to this customer.

        :param messaging_channel: Dictionary containing the messaging channels
        :param message: Dictionary containg the message being sent
        """
        req = AppToServerCommand()
        req.send_message.channel_number.number = messaging_channel.get("number")
        req.send_message.channel_number.channel = get_enum_value(
            MessagingChannel,
            messaging_channel.get("channel", "UNSPECIFIED"),
            "MESSAGING_CHANNEL",
        )
        req.send_message.customer_number.number = self._customer_number.get("number")
        req.send_message.customer_number.provider = get_enum_value(
            CustomerNumberProvider,
            self._customer_number.get("provider", "CELLULAR"),
            "CUSTOMER_NUMBER_PROVIDER",
        )
        req.send_message.message.CopyFrom(fill_in_outgoing_message(message))
        data = await self._send_command(req)
        res = self._parse_reply(data)['send_message']
        res['status'] = get_enum_string(MessageDeliveryStatus, res['status'], 'MESSAGE_DELIVERY_STATUS')
        return res

    async def reply_to_message(self, message_id: str, message: dict):
        """Used to reply to a message from this customer.

        :param message_id: Specific message id being replied to
        :param message: Message being sent back
        """
        req = AppToServerCommand()
        req.reply_to_message.customer_id = self._customer_id
        req.reply_to_message.message_id = message_id
        req.reply_to_message.message.CopyFrom(fill_in_outgoing_message(message))
        data = await self._send_command(req)
        res = self._parse_reply(data)['send_message']
        res['status'] = get_enum_string(MessageDeliveryStatus, res['status'], 'MESSAGE_DELIVERY_STATUS')
        return res

    async def update_activity(self, source: str, activity: dict):
        """Used to update a customer's activity.

        :param source: Activity source
        :param activity: Dictionary containing the activities
        """
        req = AppToServerCommand()

        req.customer_activity.source = source
        req.customer_activity.customer_number.number = self._customer_number.get(
            "number"
        )
        req.customer_activity.customer_number.provider = get_enum_value(
            CustomerNumberProvider,
            self._customer_number.get("provider", "CELLULAR"),
            "CUSTOMER_NUMBER_PROVIDER",
        )

        req.customer_activity.key = activity.get("key")
        req.customer_activity.session_id = activity.get("session_id")
        req.customer_activity.properties['property'] = str(activity.get("properties"))

        data = await self._send_command(req)
        res = self._parse_reply(data)['customer_activity']

        if not res['status']:
            raise RuntimeError(res['description'])

        return res

    async def update_messaging_consent(
        self,
        messaging_channel: dict,
        action: str = 'ALLOW',
    ):
        """Used to update a customer's engagement consent on this channel.

        :param messaging_channel: Dictionary containing the messaging channels
        :param action: Choice of messaging constent. Default vallue: ALLOW
        """
        req = AppToServerCommand()

        req.update_messaging_consent.channel_number.number = messaging_channel.get(
            "number"
        )
        req.update_messaging_consent.channel_number.channel = get_enum_value(
            MessagingChannel,
            messaging_channel.get("channel", "UNSPECIFIED"),
            "MESSAGING_CHANNEL",
        )
        req.update_messaging_consent.customer_number.number = self._customer_number.get(
            "number"
        )
        req.update_messaging_consent.customer_number.provider = get_enum_value(
            CustomerNumberProvider,
            self._customer_number.get("provider", "CELLULAR"),
            "CUSTOMER_NUMBER_PROVIDER",
        )
        req.update_messaging_consent.update = get_enum_value(MessagingConsentUpdate, action, 'MESSAGING_CONSENT_UPDATE')
        data = await self._send_command(req)
        res = self._parse_reply(data)['update_messaging_consent']

        if not res['status']:
            raise RuntimeError(res['description'])

        return res

    async def lease_app_data(self):
        """Used to lease a customer's app data. """
        req = AppToServerCommand()

        if self._customer_id is not None:
            req.lease_customer_app_data.customer_id = self._customer_id
        elif self._customer_number is not None and has_key(
            "number", self._customer_number
        ):
            req.lease_customer_app_data.customer_number.number = (
                self._customer_number.get("number")
            )
            req.lease_customer_app_data.customer_number.provider = get_enum_value(
                CustomerNumberProvider,
                self._customer_number.get("provider", "CELLULAR"),
                "CUSTOMER_NUMBER_PROVIDER",
            )
        else:
            raise RuntimeError("Invalid customer id and/or customer number")

        data = await self._send_command(req)
        res = self._parse_reply(data, to_json=False).lease_customer_app_data

        if not res.status:
            raise RuntimeError(res.description)

        if res.value.HasField('string_val'):
            return json.loads(res.string_val)
        else:
            return res.value.bytes_val

    async def update_app_data(self, data):
        """Used to update a customer's app data.

        :param data: Dictionary of the data being updated
        """
        req = AppToServerCommand()

        if self._customer_id is not None:
            req.update_customer_app_data.customer_id = self._customer_id
        elif self._customer_number is not None and has_key(
            "number", self._customer_number
        ):
            req.update_customer_app_data.customer_number.number = (
                self._customer_number.get("number")
            )
            req.update_customer_app_data.customer_number.provider = get_enum_value(
                CustomerNumberProvider,
                self._customer_number.get("provider", "CELLULAR"),
                "CUSTOMER_NUMBER_PROVIDER",
            )
        else:
            raise RuntimeError("Invalid customer id and/or customer number")

        try:
            ro = data.decode()
            req.update_customer_app_data.update.bytes_val = data
        except (UnicodeDecodeError, AttributeError):
            req.update_customer_app_data.update.string_val = json.dumps(data)
            pass

        data = await self._send_command(req)
        res = self._parse_reply(data)['update_customer_app_data']

        if not res['status']:
            raise RuntimeError(res['description'])

        return res

    async def delete_app_data(self):
        """Used to remove a customer's app data. """
        req = AppToServerCommand()

        if self._customer_id is not None:
            req.delete_customer_app_data.customer_id = self._customer_id
        elif self._customer_number is not None and has_key(
            "number", self._customer_number
        ):
            req.delete_customer_app_data.customer_number.number = (
                self._customer_number.get("number")
            )
            req.delete_customer_app_data.customer_number.provider = get_enum_value(
                CustomerNumberProvider,
                self._customer_number.get("provider", "CELLULAR"),
                "CUSTOMER_NUMBER_PROVIDER",
            )
        else:
            raise RuntimeError("Invalid customer id and/or customer number")

        data = await self._send_command(req)
        res = self._parse_reply(data)['update_customer_app_data']

        if not res['status']:
            raise RuntimeError(res['description'])

        return res

    async def get_metadata(self):
        """Used to get the metadata of the customer.
        """
        state = await self.get_state()
        meta = dict()
        if has_key('identity_state', state):
            meta = state['identity_state'].get('metadata', dict())
        result = dict()
        for key in meta:
            result[key] = json.loads(meta[key]['string_val']) if has_key('string_val', meta[key]) else meta[key]['bytes_val']
        return result

    async def update_metadata(self, data: dict):
        """Used to update a customer's metadata.

        :param data: Dictionary containing the metadata being updated
        """
        req = AppToServerCommand()

        if self._customer_id is not None:
            req.update_customer_metadata.customer_id = self._customer_id
        elif self._customer_number is not None and has_key(
            "number", self._customer_number
        ):
            req.update_customer_metadata.customer_number.number = (
                self._customer_number.get("number")
            )
            req.update_customer_metadata.customer_number.provider = get_enum_value(
                CustomerNumberProvider,
                self._customer_number.get("provider", "CELLULAR"),
                "CUSTOMER_NUMBER_PROVIDER",
            )
        else:
            raise RuntimeError("Invalid customer id and/or customer number")

        for key in data.keys():
            val = data.get(key)
            try:
                data = val.decode()
                req.update_customer_metadata.updates[key].bytes_val = val
            except (UnicodeDecodeError, AttributeError):
                req.update_customer_metadata.updates[key].string_val = json.dumps(val)
                pass

        data = await self._send_command(req)
        res = self._parse_reply(data)['update_customer_state']

        if not res['status']:
            raise RuntimeError(res['description'])

        return res

    async def delete_metadata(self, keys: list):
        """Used to remove a customer's metadata.

        :param keys: List of keys being deleted
        """
        req = AppToServerCommand()

        if self._customer_id is not None:
            req.delete_customer_metadata.customer_id = self._customer_id
        elif self._customer_number is not None and has_key(
            "number", self._customer_number
        ):
            req.delete_customer_metadata.customer_number.number = (
                self._customer_number.get("number")
            )
            req.delete_customer_metadata.customer_number.provider = get_enum_value(
                CustomerNumberProvider,
                self._customer_number.get("provider", "CELLULAR"),
                "CUSTOMER_NUMBER_PROVIDER",
            )
        else:
            raise RuntimeError("Invalid customer id and/or customer number")

        req.delete_customer_metadata.deletions.extend(keys)
        data = await self._send_command(req)
        res = self._parse_reply(data)['update_customer_state']

        if not res['status']:
            raise RuntimeError(res['description'])

        return res

    async def get_secondary_ids(self):
        """Used to get the secondary ids of the customer."""
        state = await self.get_state()
        if has_key('identity_state', state):
            return state['identity_state']['secondary_ids'] or list()
        return list()

    async def update_secondary_ids(self, secondary_ids: list):
        """Used to update a customer's secondary ids.

        :param secondary_id: List of secondary ids being updated
        """
        req = AppToServerCommand()

        if self._customer_id is not None:
            req.update_customer_secondary_id.customer_id = self._customer_id
        elif self._customer_number is not None and has_key(
            "number", self._customer_number
        ):
            req.update_customer_secondary_id.customer_number.number = (
                self._customer_number.get("number")
            )
            req.update_customer_secondary_id.customer_number.provider = get_enum_value(
                CustomerNumberProvider,
                self._customer_number.get("provider", "CELLULAR"),
                "CUSTOMER_NUMBER_PROVIDER",
            )
        else:
            raise RuntimeError("Invalid customer id and/or customer number")

        for _id in secondary_ids:
            val = CustomerIndex()
            val.mapping.key = _id["key"]
            val.mapping.value.value = _id["value"]
            if _id["expires_at"] is not None:
                val.expires_at.seconds = _id["expires_at"]
            req.update_customer_secondary_id.updates.append(val)

        data = await self._send_command(req)
        res = self._parse_reply(data)['update_customer_state']

        if not res['status']:
            raise RuntimeError(res['description'])

        return res

    async def delete_secondary_ids(self, secondary_ids: list):
        """Used to remove a customer's secondary ids.

        :param secondary_ids: List of secondary ids being deleted
        """
        req = AppToServerCommand()

        if self._customer_id is not None:
            req.delete_customer_secondary_id.customer_id = self._customer_id
        elif self._customer_number is not None and has_key(
            "number", self._customer_number
        ):
            req.delete_customer_secondary_id.customer_number.number = (
                self._customer_number.get("number")
            )
            req.delete_customer_secondary_id.customer_number.provider = get_enum_value(
                CustomerNumberProvider,
                self._customer_number.get("provider", "CELLULAR"),
                "CUSTOMER_NUMBER_PROVIDER",
            )
        else:
            raise RuntimeError("Invalid customer id and/or customer number")

        for _id in secondary_ids:
            val = IndexMapping()
            val.key = _id.get("key")
            val.value.value = _id.get("value")
            req.delete_customer_secondary_id.deletions.append(val)

        data = await self._send_command(req)
        res = self._parse_reply(data)['update_customer_state']

        if not res['status']:
            raise RuntimeError(res['description'])

        return res

    async def get_tags(self):
        """Used to get the tags for a customer."""
        state = await self.get_state()
        if has_key('identity_state', state):
            return state['identity_state']['tags'] or list()
        return list()

    async def update_tags(self, tags: list):
        """Used to update a customer's tags.

        :param tags: list of tags being updated
        """
        req = AppToServerCommand()

        if self._customer_id is not None:
            req.update_customer_tag.customer_id = self._customer_id
        elif self._customer_number is not None and has_key(
            "number", self._customer_number
        ):
            req.update_customer_tag.customer_number.number = self._customer_number.get(
                "number"
            )
            req.update_customer_tag.customer_number.provider = get_enum_value(
                CustomerNumberProvider,
                self._customer_number.get("provider", "CELLULAR"),
                "CUSTOMER_NUMBER_PROVIDER",
            )
        else:
            raise RuntimeError("Invalid customer id and/or customer number")

        for _id in tags:
            val = CustomerIndex()
            val.mapping.key = _id.get("key")
            val.mapping.value.value = _id.get("value")
            if _id.get("expires_at") is not None:
                val.expires_at.seconds = _id.get("expires_at")
            req.update_customer_tag.updates.append(val)

        data = await self._send_command(req)
        res = self._parse_reply(data)['update_customer_state']

        if not res['status']:
            raise RuntimeError(res['description'])

        return res

    async def delete_tags(self, keys: list):
        """Used to remove a customer's tags.

        :param keys: list of tags being deleted
        """
        req = AppToServerCommand()

        if self._customer_id is not None:
            req.delete_customer_tag.customer_id = self._customer_id
        elif self._customer_number is not None and has_key(
            "number", self._customer_number
        ):
            req.delete_customer_tag.customer_number.number = self._customer_number.get(
                "number"
            )
            req.delete_customer_tag.customer_number.provider = get_enum_value(
                CustomerNumberProvider,
                self._customer_number.get("provider", "CELLULAR"),
                "CUSTOMER_NUMBER_PROVIDER",
            )
        else:
            raise RuntimeError("Invalid customer id and/or customer number")

        req.delete_customer_tag.deletions.extend(keys)

        data = await self._send_command(req)
        res = self._parse_reply(data)['update_customer_state']

        if not res['status']:
            raise RuntimeError(res['description'])

        return res

    async def add_reminder(self, reminder: dict):
        """Used to add a reminder.

        :param reminder: dictionary containing the details of the reminder
        """
        req = AppToServerCommand()

        if self._customer_id is not None:
            req.add_customer_reminder.customer_id = self._customer_id
        elif self._customer_number is not None and has_key(
            "number", self._customer_number
        ):
            req.add_customer_reminder.customer_number.number = self._customer_number.get(
                "number"
            )
            req.add_customer_reminder.customer_number.provider = get_enum_value(
                CustomerNumberProvider,
                self._customer_number.get("provider", "CELLULAR"),
                "CUSTOMER_NUMBER_PROVIDER",
            )
        else:
            raise RuntimeError("Invalid customer id and/or customer number")

        req.add_customer_reminder.reminder.key = reminder.get("key")
        req.add_customer_reminder.reminder.remind_at.seconds = round(
            reminder.get("remind_at")
        )
        req.add_customer_reminder.reminder.payload.value = reminder.get("payload")
        if reminder.get("interval") is not None:
            req.add_customer_reminder.reminder.interval.seconds = round(
                reminder.get("interval")
            )

        data = await self._send_command(req)
        res = self._parse_reply(data)['update_customer_app_data']

        if not res['status']:
            raise RuntimeError(res['description'])

        return res

    async def cancel_reminder(self, key: str):
        """Used to cancel a reminder based on keys.

        :param key: Reminder key
        """
        req = AppToServerCommand()

        if self._customer_id is not None:
            req.cancel_customer_reminder.customer_id = self._customer_id
        elif self._customer_number is not None and has_key(
            "number", self._customer_number
        ):
            req.cancel_customer_reminder.customer_number.number = (
                self._customer_number.get("number")
            )
            req.cancel_customer_reminder.customer_number.provider = get_enum_value(
                CustomerNumberProvider,
                self._customer_number.get("provider", "CELLULAR"),
                "CUSTOMER_NUMBER_PROVIDER",
            )
        else:
            raise RuntimeError("Invalid customer id and/or customer number")

        req.cancel_customer_reminder.key = key

        data = await self._send_command(req)
        res = self._parse_reply(data)['update_customer_app_data']

        if not res['status']:
            raise RuntimeError(res['description'])

        return res

    async def _send_command(self, data):
        return await self._client._send_command(data)

    @staticmethod
    def _parse_reply(payload, to_json=True):
        result = AppToServerCommandReply()
        result.ParseFromString(payload.data)
        if to_json:
            result = json.loads(MessageToJson(message=result, preserving_proto_field_name=True))
        return result
