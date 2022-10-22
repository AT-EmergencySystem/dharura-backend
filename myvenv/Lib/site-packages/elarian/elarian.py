import json
from google.protobuf.json_format import MessageToJson

from .client import Client
from .utils.helpers import fill_in_outgoing_message, has_key
from .utils.generated.app_socket_pb2 import (
    AppToServerCommand,
    AppToServerCommandReply,
    GenerateAuthTokenCommand,
)
from .utils.generated.common_model_pb2 import (
    CustomerNumberProvider,
)
from .utils.generated.payment_model_pb2 import (
    PaymentChannel,
    PaymentStatus,
)
from .utils.helpers import (
    get_enum_value,
    get_enum_string
)


class Elarian(Client):
    """Elarian class that allows setting of handlers to enable someone to deal with various situations.

        :param org_id: The organization id
        :param api_key: The generated API key from the dashboard
        :param app_id: The app id generated from the dashboard
    """

    def __init__(self, org_id, api_key, app_id, options=Client._default_options):
        super().__init__(
            org_id,
            api_key,
            app_id,
            [
                "reminder",
                "messaging_session_started",
                "messaging_session_renewed",
                "messaging_session_ended",
                "messaging_consent_update",
                "received_sms",
                "received_fb_messenger",
                "received_telegram",
                "received_whatsapp",
                "received_email",
                "voice_call",
                "ussd_session",
                "message_status",
                "sent_message_reaction",
                "received_payment",
                "payment_status",
                "customer_activity",
            ],
            options,
        )

    def set_on_reminder(self, handler):
        """Set the reminder handler.

        :param handler: Dedicated handler function
        """
        return self._on("reminder", handler)

    def set_on_messaging_session_started(self, handler):
        """Set the handler for session started notifications.

        :param handler: Dedicated handler function
        """
        return self._on("messaging_session_started", handler)

    def set_on_messaging_session_renewed(self, handler):
        """Set the handler for session renewed notifications.

        :param handler: Dedicated handler function
        """
        return self._on("messaging_session_renewed", handler)

    def set_on_messaging_session_ended(self, handler):
        """Set the handler for session ended notifications.

        :param handler: Dedicated handler function
        """
        return self._on("messaging_session_ended", handler)

    def set_on_messaging_consent_update(self, handler):
        """Set the handler for consent update notifications.

        :param handler: Dedicated handler function
        """
        return self._on("messaging_consent_update", handler)

    def set_on_received_sms(self, handler):
        """Set the handler for sms notifications.

        :param handler: Dedicated handler function
        """
        return self._on("received_sms", handler)

    def set_on_received_fb_messenger(self, handler):
        """Set the handler for messenger(facebook) notifications.

        :param handler: Dedicated handler function
        """
        return self._on("received_fb_messenger", handler)

    def set_on_received_telegram(self, handler):
        """Set the handler for telegram notifications.

        :param handler: Dedicated handler function
        """
        return self._on("received_telegram", handler)

    def set_on_received_whatsapp(self, handler):
        """Set the handler for whatsapp notifications.

        :param handler: Dedicated handler function
        """
        return self._on("received_whatsapp", handler)

    def set_on_received_email(self, handler):
        """Set the handler for email notifications.

        :param handler: Dedicated handler function
        """
        return self._on("received_email", handler)

    def set_on_voice_call(self, handler):
        """Set the handler for voice call notifications.

        :param handler: Dedicated handler function
        """
        return self._on("voice_call", handler)

    def set_on_ussd_session(self, handler):
        """Set the handler for ussd session notifications.

        :param handler: Dedicated handler function
        """
        return self._on("ussd_session", handler)

    def set_on_message_status(self, handler):
        """Set the handler for message status notifications.

        :param handler: Dedicated handler function
        """
        return self._on("message_status", handler)

    def set_on_sent_message_reaction(self, handler):
        """Set the handler for message reaction notifications.

        :param handler: Dedicated handler function
        """
        return self._on("sent_message_reaction", handler)

    def set_on_received_payment(self, handler):
        """Set the handler for payment notifications.

        :param handler: Dedicated handler function
        """
        return self._on("received_payment", handler)

    def set_on_payment_status(self, handler):
        """Set the handler for payment status notifications.

        :param handler: Dedicated handler function
        """
        return self._on("payment_status", handler)

    def set_on_customer_activity(self, handler):
        """Set the handler for customer activity notifications.

        :param handler: Dedicated handler function
        """
        return self._on("customer_activity", handler)

    async def generate_auth_token(self):
        """Generate an auth token to use in place of API keys"""
        req = AppToServerCommand()
        req.generate_auth_token.CopyFrom(GenerateAuthTokenCommand())
        data = await self._send_command(req)
        res = self._parse_reply(data).generate_auth_token
        return {"token": res.token, "lifetime": res.lifetime.seconds}

    async def add_customer_reminder_by_tag(self, tag: dict, reminder: dict):
        """Set a reminder to be triggered at the specified time for customers with a particular tag.

        :param tag: Dictionary of tags
        :param reminder: Dictionary of reminders
        """
        req = AppToServerCommand()
        req.add_customer_reminder_tag.tag.key = tag["key"]
        req.add_customer_reminder_tag.tag.value.value = tag["value"]
        req.add_customer_reminder_tag.reminder.key = reminder["key"]
        req.add_customer_reminder_tag.reminder.remind_at.seconds = round(
            reminder["remind_at"]
        )
        if has_key("interval", reminder):
            req.add_customer_reminder_tag.reminder.interval.seconds = round(
                reminder["interval"]
            )
        req.add_customer_reminder_tag.reminder.payload.value = reminder["payload"]
        data = await self._send_command(req)
        res = self._parse_reply(data, to_json=True)['tag_command']
        if not res['status']:
            raise RuntimeError(res['description'])
        return res

    async def cancel_customer_reminder_by_tag(self, tag: dict, key: str):
        """Cancel a previously set reminder using a tag and key.

        :param tag: Dictionary of tags
        :param reminder: Dictionary of reminders
        """
        req = AppToServerCommand()
        req.cancel_customer_reminder_tag.key = key
        req.cancel_customer_reminder_tag.tag.key = tag["key"]
        req.cancel_customer_reminder_tag.tag.value.value = tag["value"]
        data = await self._send_command(req)
        res = self._parse_reply(data, to_json=True)['tag_command']
        if not res['status']:
            raise RuntimeError(res['description'])
        return res

    async def send_message_by_tag(
        self, tag: dict, messaging_channel: dict, message: dict
    ):
        """Send a message to customers with a specific tag.

        :param messaging_channel: Dictionary with the channles to be used
        :param message: Dictionary with the messages to be sent
        """
        req = AppToServerCommand()
        req.send_message_tag.channel_number.number = messaging_channel["number"]
        req.send_message_tag.channel_number.channel = messaging_channel["channel"].value
        req.send_message_tag.tag.key = tag["key"]
        req.send_message_tag.tag.value.value = tag["value"]
        req.send_message_tag.message.CopyFrom(fill_in_outgoing_message(message))
        data = await self._send_command(req)
        res = self._parse_reply(data, to_json=True)['tag_command']
        if not res['status']:
            raise RuntimeError(res['description'])
        return res

    async def initiate_payment(
        self, debit_party: dict, credit_party: dict, value: dict, narration: str
    ):
        """Initiate a payment transaction.

        :param debit_party: Dictionary containing the details of the customer the money is coming from
        :param credit_party: Dictionary containing the details of the customer the money is going to
        :param value: Dictionary containing the amount and currency
        :param narration: String containing the payment narration/description
        """
        req = AppToServerCommand()

        if has_key("purse", debit_party):
            req.initiate_payment.debit_party.purse.purse_id = debit_party["purse"]["purse_id"]
        elif has_key("customer", debit_party):
            req.initiate_payment.debit_party.customer.customer_number.number = (
                debit_party["customer"]["customer_number"]["number"]
            )
            req.initiate_payment.debit_party.customer.customer_number.provider = get_enum_value(
                CustomerNumberProvider,
                debit_party["customer"]["customer_number"]["provider"],
                "CUSTOMER_NUMBER_PROVIDER",
            )
            if has_key("partition", debit_party["customer"]["customer_number"]):
                req.initiate_payment.debit_party.customer.customer_number.partition = (
                    debit_party["customer"]["customer_number"]["partition"]
                )
            req.initiate_payment.debit_party.customer.channel_number.number = (
                debit_party["customer"]["channel_number"]["number"]
            )
            req.initiate_payment.debit_party.customer.channel_number.channel = get_enum_value(
                PaymentChannel,
                debit_party["customer"]["channel_number"]["channel"],
                "PAYMENT_CHANNEL",
            )
        elif has_key("wallet", debit_party):
            req.initiate_payment.debit_party.wallet.wallet_id = debit_party["wallet"]["wallet_id"]
            req.initiate_payment.debit_party.wallet.customer_id = debit_party["wallet"]["customer_id"]
        elif has_key("channel", debit_party):
            req.initiate_payment.debit_party.channel.channel_number.number = (
                debit_party["channel"]["channel_number"]["number"]
            )
            req.initiate_payment.debit_party.channel.channel_number.channel = get_enum_value(
                PaymentChannel,
                debit_party["customer"]["channel_number"]["channel"],
                "PAYMENT_CHANNEL",
            )
            req.initiate_payment.debit_party.channel.channel_code = debit_party["channel"]["network_code"]
            req.initiate_payment.debit_party.channel.account.value = debit_party["channel"]["account"].value

        if has_key("purse", credit_party):
            req.initiate_payment.credit_party.purse.purse_id = credit_party["purse"][
                "purse_id"
            ]
        elif has_key("customer", credit_party):
            req.initiate_payment.credit_party.customer.customer_number.number = (
                credit_party["customer"]["customer_number"]["number"]
            )
            req.initiate_payment.credit_party.customer.customer_number.provider = get_enum_value(
                CustomerNumberProvider,
                credit_party["customer"]["customer_number"]["provider"],
                "CUSTOMER_NUMBER_PROVIDER",
            )
            if has_key("partition", credit_party["customer"]["customer_number"]):
                req.initiate_payment.credit_party.customer.customer_number.partition = (
                    credit_party["customer"]["customer_number"]["partition"]
                )
            req.initiate_payment.credit_party.customer.channel_number.number = (
                credit_party["customer"]["channel_number"]["number"]
            )
            req.initiate_payment.credit_party.customer.channel_number.channel = get_enum_value(
                PaymentChannel,
                credit_party["customer"]["channel_number"]["channel"],
                "PAYMENT_CHANNEL",
            )
        elif has_key("wallet", credit_party):
            req.initiate_payment.credit_party.wallet.wallet_id = credit_party["wallet"][
                "wallet_id"
            ]
            req.initiate_payment.credit_party.wallet.customer_id = credit_party["wallet"]["customer_id"]
        elif has_key("channel", credit_party):
            req.initiate_payment.credit_party.channel.channel_number.number = (
                credit_party["channel"]["channel_number"]["number"]
            )
            req.initiate_payment.credit_party.channel.channel_number.channel = get_enum_value(
                PaymentChannel,
                credit_party["customer"]["channel_number"]["channel"],
                "PAYMENT_CHANNEL",
            )
            req.initiate_payment.credit_party.channel.channel_code = credit_party[
                "channel"
            ]["network_code"]
            req.initiate_payment.credit_party.channel.account.value = credit_party[
                "channel"
            ]["account"].value

        req.initiate_payment.value.amount = value["amount"]
        req.initiate_payment.value.currency_code = value["currency_code"]
        req.initiate_payment.narration = narration
        data = await self._send_command(req)
        res = self._parse_reply(data, to_json=True)['initiate_payment']
        res['status'] = get_enum_string(PaymentStatus, res['status'], 'PAYMENT_STATUS')
        return res

    @staticmethod
    def _parse_reply(payload, to_json=False):
        result = AppToServerCommandReply()
        result.ParseFromString(payload.data)
        if to_json:
            result = json.loads(MessageToJson(message=result, preserving_proto_field_name=True))
        return result
