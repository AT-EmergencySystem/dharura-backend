import asyncio
import json
import traceback
from rsocket import Payload, BaseRequestHandler
from elarian.customer import Customer
from google.protobuf.json_format import MessageToJson
from .generated.messaging_model_pb2 import MessagingChannel,\
    MessagingSessionEndReason,\
    MessageReaction,\
    MessageDeliveryStatus
from .generated.payment_model_pb2 import PaymentChannel, PaymentStatus
from .generated.app_socket_pb2 import \
    ServerToAppNotification,\
    ServerToAppNotificationReply
from .generated.simulator_socket_pb2 import (
    ServerToSimulatorNotification,
    ServerToSimulatorNotificationReply
)
from .generated.common_model_pb2 import (
    CustomerNumberProvider,
)
from .helpers import (
    fill_in_outgoing_message,
    get_enum_string,
    has_key
)


class _RequestHandler(BaseRequestHandler):
    _handlers = dict()
    _is_simulator = False
    _client = None

    def register_handler(self, event, handler):
        self._handlers[event] = handler

    def handle(self, event, *args):
        if event in self._handlers.keys():
            handler = self._handlers[event]
            if asyncio.iscoroutinefunction(handler):
                loop = asyncio.get_event_loop()
                loop.create_task(handler(*args))
            else:
                handler(*args)

    def get_handlers(self):
        return self._handlers

    @staticmethod
    async def _default_handler(notif, customer, app_data, callback):
        print("No handler for notification:", notif)
        callback(data_update=app_data, response=None)

    async def request_response(self, payload: Payload) -> asyncio.Future:
        future = asyncio.Future()
        try:
            data = ServerToSimulatorNotification() if self._is_simulator else ServerToAppNotification()
            data.ParseFromString(payload.data)

            if data.WhichOneof("entry") == 'customer':
                data = data.customer
            elif data.WhichOneof("entry") == 'purse':
                data = data.purse

            event = data.WhichOneof("entry")
            notif = getattr(data, event)

            customer = None

            incoming_app_data = data.app_data if data.HasField('app_data') else None
            app_data = data.app_data if data.HasField('app_data') else None
            if app_data is not None:
                app_data = json.loads(app_data.string_val) if app_data.HasField('string_val') else app_data.bytes_val

            data = json.loads(MessageToJson(message=data, preserving_proto_field_name=True))
            notif = json.loads(MessageToJson(message=notif, preserving_proto_field_name=True))

            customer_number = notif.get('customer_number', None)

            # TODO: Complete notification parsing to match sdk spec

            if event in [
                'messaging_session_started',
                'messaging_session_renewed',
                'messaging_session_ended',
                'messaging_consent_update',
                'sent_message_reaction'
            ]:
                notif['channel_number']['channel'] = get_enum_string(
                    MessagingChannel,
                    notif['channel_number']['channel'],
                    'MESSAGING_CHANNEL'
                )
                if has_key('reason', notif):
                    notif['reason'] = get_enum_string(
                        MessagingSessionEndReason,
                        notif['reason'],
                        'MESSAGING_SESSION_END_REASON'
                    )
                if has_key('reaction', notif):
                    notif['reaction'] = get_enum_string(
                        MessageReaction,
                        notif['reaction'],
                        'MESSAGE_REACTION'
                    )

            if event == 'message_status':
                notif['status'] = get_enum_string(MessageDeliveryStatus, notif['status'], 'MESSAGE_DELIVERY_STATUS')

            if event in ['payment_status', 'received_payment']:
                notif['status'] = get_enum_string(PaymentStatus, notif['status'], 'PAYMENT_STATUS')

                if has_key('channel_number', notif):
                    notif['channel_number']['channel'] = get_enum_string(
                        PaymentChannel,
                        notif['channel_number']['channel'],
                        'PAYMENT_CHANNEL'
                    )

                if has_key('customer_number', notif):
                    notif['customer_number']['provider'] = get_enum_string(
                        CustomerNumberProvider,
                        customer_number['provider'],
                        'CUSTOMER_NUMBER_PROVIDER')

            if event == 'received_message':
                notif['channel_number']['channel'] = get_enum_string(
                    MessagingChannel,
                    notif['channel_number']['channel'],
                    'MESSAGING_CHANNEL'
                )
                notif['customer_number']['provider'] = get_enum_string(
                            CustomerNumberProvider,
                            customer_number['provider'],
                            'CUSTOMER_NUMBER_PROVIDER')

                for part in  notif['parts']:
                    if has_key('text', part):
                        notif['text'] = part['text']
                    if has_key('ussd', part):
                        notif['input'] = part['ussd']
                    if has_key('media', part):
                        notif['media'] = part['media']
                    if has_key('location', part):
                        notif['location'] = part['location']
                    if has_key('email', part):
                        notif['email'] = part['email']
                    if has_key('voice', part):
                        notif['voice'] = part['voice']
                
                del notif['parts']    
                
                channel = notif['channel_number']['channel'].lower()
                if channel == 'sms':
                    event = 'received_sms'
                if channel == 'voice':
                    event = 'voice_call'
                if channel == 'ussd':
                    event = 'ussd_session'
                if channel == 'fb_messenger':
                    event = 'received_fb_messenger'
                if channel == 'telegram':
                    event = 'received_telegram'
                if channel == 'whatsapp':
                    event = 'received_whatsapp'
                if channel == 'email':
                    event = 'received_email'

            if event in self._handlers.keys():
                handler = self._handlers[event]
            else:
                handler = self._default_handler

            if not self._is_simulator:
                customer = Customer(client=self._client, id=data['customer_id'])
                if customer_number is not None:
                    customer = Customer(
                        client=self._client,
                        id=data['customer_id'],
                        number=customer_number['number'],
                        provider=customer_number['provider']
                    )
                # TODO: Fetch customer number from state if not available?

                notif['org_id'] = data['org_id']
                notif['app_id'] = data['app_id']
                notif['customer_id'] = customer.get_id()
                notif['created_at'] = data['created_at']

            def callback(response=None, data_update=None):
                res = ServerToSimulatorNotificationReply() if self._is_simulator else ServerToAppNotificationReply()
                if not self._is_simulator:
                    if response is not None:
                        res.message.CopyFrom(fill_in_outgoing_message(response))
                    if data_update is not None:
                        try:
                            ro = data_update.decode()
                            res.data_update.data.bytes_val = data_update
                        except (UnicodeDecodeError, AttributeError):
                            res.data_update.data.string_val = json.dumps(data_update)
                            pass
                    else:
                        res.data_update.data.CopyFrom(incoming_app_data)
                if not future.done():
                    future.set_result(Payload(data=res.SerializeToString(), metadata=bytes()))

            async def callback_timeout():
                await asyncio.sleep(15)
                if not future.done():
                    res = ServerToSimulatorNotificationReply() if self._is_simulator else ServerToAppNotificationReply()
                    if not self._is_simulator and incoming_app_data is not None:
                        res.data_update.data.CopyFrom(incoming_app_data)
                    future.set_result(Payload(data=res.SerializeToString(), metadata=bytes()))

            future.get_loop().create_task(handler(notif, customer, app_data or dict(), callback))
            future.get_loop().create_task(callback_timeout())

        except Exception as ex:
            traceback.print_exc()
            future.set_exception(ex)

        return future

