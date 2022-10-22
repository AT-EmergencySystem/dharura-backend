import json
from google.protobuf.json_format import MessageToJson

from .client import Client

from .utils.generated.simulator_socket_pb2 import (
    SimulatorToServerCommand,
    SimulatorToServerCommandReply,
)
from .utils.generated.messaging_model_pb2 import (
    InboundMessageBody,
    MessagingChannel,
)

from .utils.generated.common_model_pb2 import (
    MediaType
)

from .utils.generated.payment_model_pb2 import (
    PaymentChannel,
    PaymentStatus
)
from .utils.helpers import has_key, get_enum_value


class Simulator(Client):
    """Client class that simulates a connection to the elarian backend

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
                "send_message",
                "make_voice_call",
                "send_customer_payment",
                "send_channel_payment",
                "checkout_payment",
            ],
            options,
        )
        self._is_simulator = True

    def set_on_send_message(self, handler):
        """Set the handler for messages received on the simulator

           :param handler: Dedicated handler function
        """
        return self._on("send_message", handler)

    def set_on_make_voice_call(self, handler):
        """Set the handler for voice calls received on the simulator

           :param handler: Dedicated handler function
        """
        return self._on("make_voice_call", handler)

    def set_on_send_customer_payment(self, handler):
        """Set the handler for customer payments received on the simulator

           :param handler: Dedicated handler function
        """
        return self._on("send_customer_payment", handler)

    def set_on_send_channel_payment(self, handler):
        """Set the handler for channel payments received on the simulator

           :param handler: Dedicated handler function
        """
        return self._on("send_channel_payment", handler)

    def set_on_checkout_payment(self, handler):
        """Set the handler for customer checkout received on the simulator

           :param handler: Dedicated handler function
        """
        return self._on("checkout_payment", handler)

    async def receive_message(
        self,
        phone_number: str,
        messaging_channel: dict,
        session_id: str,
        message_parts: list,
        cost: dict = None
    ):
        """Simulate sending a message. i.e. tell the simulated gateway to receive a message

           :param phone_number: Phone number that sent the message
           :param messaging_channel: Dictionary containing the details of the messaging channel used
           :param session_id: Session id of the simulation
           :param message_parts: List containing the message parts
        """
        req = SimulatorToServerCommand()

        if cost is None:
            cost = {
                "currency_code": "KES",
                "amount": 0
            }
        req.receive_message.session_id.value = session_id
        req.receive_message.customer_number = phone_number
        req.receive_message.channel_number.number = messaging_channel["number"]
        req.receive_message.cost.amount = cost["amount"]
        req.receive_message.cost.currency_code = cost["currency_code"]
        req.receive_message.channel_number.channel = get_enum_value(
            MessagingChannel,
            messaging_channel["channel"],
            "MESSAGING_CHANNEL",
        )

        parts = req.receive_message.parts
        for part in message_parts:
            _part = InboundMessageBody()

            if has_key("text", part):
                _part.text = part["text"]

            if has_key("ussd", part):
                _part.ussd.value = part["ussd"]

            if has_key("media", part):
                _part.media.url = part["media"]["url"]
                _part.media.media = get_enum_value(
                    MediaType,
                    part["media"]["type"],
                    "MEDIA_TYPE",
                )

            if has_key("location", part):
                _part.location.latitude = part["location"]["latitude"]
                _part.location.longitude = part["location"]["longitude"]
                _part.location.label.value = part["location"]["label"]
                _part.location.address.value = part["location"]["address"]

            if has_key("email", part):
                _part.email.subject = part["email"]["subject"]
                _part.email.body_plain = part["email"]["plain"]
                _part.email.body_html = part["email"]["html"]
                _part.email.cc_list.extend(part["email"]["cc"])
                _part.email.bcc_list.extend(part["email"]["bcc"])
                _part.email.attachments.extend(part["email"]["attachments"])

            if has_key("voice", part):
                _part.voice.direction = part["voice"]["direction"].value
                _part.voice.status = part["voice"]["status"].value
                _part.voice.started_at.seconds = part["voice"]["started_at"]
                _part.voice.hangup_cause = part["voice"]["hangup_cause"].value
                _part.voice.dtmf_digits.value = part["voice"]["dtmf_digits"]
                _part.voice.recording_url.value = part["voice"]["recording_url"]

                _part.voice.dial_data.destination_number = part["voice"]["dial_data"][
                    "destination_number"
                ]
                _part.voice.dial_data.started_at.seconds = part["voice"]["dial_data"][
                    "started_at"
                ]
                _part.voice.dial_data.duration.seconds = part["voice"]["dial_data"][
                    "duration"
                ]

                _part.voice.queue_data.enqueued_at.seconds = part["voice"][
                    "queue_data"
                ]["enqueued_at"]
                _part.voice.queue_data.dequeued_at.seconds = part["voice"][
                    "queue_data"
                ]["dequeued_at"]
                _part.voice.queue_data.dequeued_to_number.value = part["voice"][
                    "queue_data"
                ]["dequeued_to_number"]
                _part.voice.queue_data.dequeued_to_sessionId.value = part["voice"][
                    "queue_data"
                ]["dequeued_to_sessionId"]
                _part.voice.queue_data.queue_duration.seconds = part["voice"][
                    "queue_data"
                ]["queue_duration"]

            parts.append(_part)
        data = await self._send_command(req)
        res = self._parse_reply(data)
        return res

    async def receive_payment(
        self,
        phone_number: str,
        payment_channel: dict,
        transaction_id: str,
        value: dict,
        status: str,
    ):
        """Used to simulate the receiving of a payment

           :param phone_number: Phone number that sent the payment
           :param payment_channel: Dictionary containing the details of the payment channel used
           :param transaction_id: Transaction id of the simulation
           :param value: Dictionary containing the transaction value and currency
           :param status: Status of the transaction
           """
        req = SimulatorToServerCommand()
        req.receive_payment.transaction_id = transaction_id
        req.receive_payment.customer_number = phone_number
        req.receive_payment.status = get_enum_value(
            PaymentStatus,
            status,
            "PAYMENT_STATUS",
        )
        req.receive_payment.value.amount = value["amount"]
        req.receive_payment.value.currency_code = value["currency_code"]
        req.receive_payment.channel_number.number = payment_channel["number"]
        req.receive_payment.channel_number.channel = get_enum_value(
            PaymentChannel,
            payment_channel["channel"],
            "PAYMENT_CHANNEL",
        )
        data = await self._send_command(req)
        res = self._parse_reply(data)
        return res

    async def update_payment_status(self, transaction_id: str, status: str):
        """Used to simulate the updating of a payment status

           :param transaction_id: Transaction id of the simulation
           :param status: Status of the transaction
        """
        req = SimulatorToServerCommand()
        req.update_payment_status.transaction_id = transaction_id
        req.update_payment_status.status = get_enum_value(
            PaymentStatus,
            status,
            "PAYMENT_STATUS",
        )
        data = await self._send_command(req)
        res = self._parse_reply(data)
        return res

    @staticmethod
    def _parse_reply(payload, to_json=True):
        result = SimulatorToServerCommandReply()
        result.ParseFromString(payload.data)
        if to_json:
            result = json.loads(MessageToJson(message=result, preserving_proto_field_name=True))
        return result
