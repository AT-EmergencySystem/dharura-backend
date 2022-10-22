from .generated.common_model_pb2 import (
    CustomerNumber,
    MediaType,
)
from .generated.messaging_model_pb2 import (
    OutboundMessage,
    VoiceCallAction,
    RecordSessionCallAction,
    RejectCallAction,
    PromptMessageReplyAction,
    PromptMessageMenuItemBody,
    TextToSpeechVoice,
    MessagingChannel
)


def has_key(key: str, data: dict):
    return key in data.keys()


def fill_in_outgoing_message(message: dict):
    _message = OutboundMessage()
    _message.labels.extend(message.get('labels', []))
    _message.provider_tag.value = message.get('provider_tag', '')
    _message.reply_token.value = message.get('reply_token', '')
    if has_key('reply_prompt', message):
        _message.reply_prompt.action = get_enum_value(
            PromptMessageReplyAction,
            message.get('reply_prompt', {}).get('action', 'UNKNOWN'),
            'PROMPT_MESSAGE_REPLY_ACTION'
        )

        def make_prompt_item(item):
            re = PromptMessageMenuItemBody()
            if has_key('text', item):
                re.text = item['text']
            elif has_key('media', item):
                re.media.url = item['media']['url']
                re.media.media = get_enum_value(MediaType, item['media']['type'], 'MEDIA_TYPE')
            return re
        _message.reply_prompt.menu.extend(list(
            map(make_prompt_item, message.get('reply_prompt', {}).get('menu', []))
        ))

    body = message.get('body', {})

    if has_key('text', body):
        _message.body.text = body['text']

    if has_key('url', body):
        _message.body.url = body['url']

    if has_key('ussd', body):
        _message.body.ussd.text = body['ussd']['text']
        _message.body.ussd.is_terminal = body['ussd']['is_terminal']

    if has_key('media', body):
        _message.body.media.url = body['media']['url']
        _message.body.media.media = get_enum_value(MediaType, body['media']['type'], 'MEDIA_TYPE')

    if has_key('location', body):
        _message.body.location.latitude = body['location']['latitude']
        _message.body.location.longitude = body['location']['longitude']
        _message.body.location.label.value = body['location'].get('label')
        _message.body.location.address.value = body['location'].get('address')

    if has_key('template', body):
        _message.body.template.id = body['template']['id']
        for k in body['template']['params']:
            _message.body.template.params[k] = body['template']['params'][k]

    if has_key('email', body):
        _message.body.email.subject = body['email'].get('subject')
        _message.body.email.body_plain = body['email'].get('plain')
        _message.body.email.body_html = body['email'].get('html')
        _message.body.email.cc_list.extend(body['email'].get('cc', []))
        _message.body.email.bcc_list.extend(body['email'].get('bcc', []))
        _message.body.email.attachments.extend(body['email'].get('attachments', []))

    if has_key('voice', body):
        for action in body['voice']:
            _action = VoiceCallAction()

            if has_key('say', action):
                _action.say.text = action['say']['text']
                _action.say.play_beep = action['say'].get('play_beep', False)
                _action.say.voice = get_enum_value(
                    TextToSpeechVoice,
                    action['say'].get('voice', 'FEMALE'),
                    'TEXT_TO_SPEECH_VOICE'
                )

            if has_key('play', action):
                _action.play.url = action['play']['url']

            if has_key('get_digits', action):
                _action.get_digits.num_digits.value = action['get_digits'].get('num_digits')
                _action.get_digits.timeout.seconds = action['get_digits'].get('timeout', 0)
                _action.get_digits.finish_on_key.value = action['get_digits'].get('finish_on_key', '#')

                if has_key('say', action.get_digits):
                    _action.get_digits.say.text = action['get_digits']['say']['text']
                    _action.get_digits.say.play_beep = action['get_digits']['say'].get('play_beep', False)
                    _action.get_digits.say.voice = get_enum_value(
                        TextToSpeechVoice,
                        action['get_digits']['say'].get('voice', 'FEMALE'),
                        'TEXT_TO_SPEECH_VOICE'
                    )

                if has_key('play', action.get_digits):
                    _action.get_digits.play.url = action['get_digits']['play']['url']

            if has_key('get_recording', action):
                _action.get_recording.timeout.seconds = action['get_recording'].get('timeout', 0)
                _action.get_recording.max_length.seconds = action['get_recording'].get('max_length', 3600)
                _action.get_recording.finish_on_key.value = action['get_recording'].get('finish_on_key', '#')
                _action.get_recording.play_beep = action['get_recording'].get('play_beep', False)
                _action.get_recording.trim_silence = action['get_recording'].get('trim_silence', True)

                if has_key('say', action.get_recording):
                    _action.get_recording.say.text = action['get_recording']['say']['text']
                    _action.get_recording.say.play_beep = action['get_recording']['say'].get('play_beep', False)
                    _action.get_recording.say.voice = get_enum_value(
                        TextToSpeechVoice,
                        action['get_recording']['say'].get('voice', 'FEMALE'),
                        'TEXT_TO_SPEECH_VOICE'
                    )

                if has_key('play', action.get_recording):
                    _action.get_recording.play.url = action['get_recording']['play']['url']

            if has_key('dial', action):
                _action.dial.record = action['dial'].get('record', False)
                _action.dial.sequential = action['dial'].get('sequential', True)
                _action.dial.ringback_tone.value = action['dial'].get('ringback_tone', None)
                _action.dial.caller_id.value = action['dial']['caller_id']
                _action.dial.max_duration.value = action['dial'].get('max_duration', 3600)
                numbers = []
                for num in action['dial']['customer_numbers']:
                    _num = CustomerNumber()
                    _num.number = num['number']
                    _num.provider = num['provider'].value
                    if has_key('partition', num):
                        _num.partition.value = num['partition']
                    numbers.append(num)
                _action.dial.customer_numbers.extend(numbers)

            if has_key('record_session', action):
                _action.record_session.CopyFrom(RecordSessionCallAction())

            if has_key('enqueue', action):
                _action.enqueue.hold_music.value = action['enqueue']['hold_music']
                _action.enqueue.queue_name.value = action['enqueue']['queue_name']

            if has_key('dequeue', action):
                _action.dequeue.record = action['dequeue'].get('record', False)
                _action.dequeue.queue_name.value = action['dequeue']['queue_name']
                _action.dequeue.channel_number.number = action['dequeue']['channel']['number']
                _action.dequeue.channel_number.channel = get_enum_value(
                    MessagingChannel,
                    action['dequeue']['channel']['channel'],
                    'MESSAGING_CHANNEL'
                )

            if has_key('reject', action):
                _action.reject.CopyFrom(RejectCallAction())

            if has_key('redirect', action):
                _action.redirect.url = action['redirect']['url']

            _message.body.voice.actions.append(_action)

    return _message


def get_valid_keys(target, prefix):
    """Lists the valid keys to be used on specific proto"""
    keys = map(lambda key: key.replace(f'{prefix}_', ''), target.keys())
    return list(filter(lambda item: item != 'UNSPECIFIED', list(keys)))


def get_enum_value(target, key, prefix):
    """Used to get the provider given the enum"""
    try:
        return target.Value(f"{prefix}_{key.upper()}")
    except Exception:
        raise RuntimeError(f"Invalid key {key}")


def get_enum_string(target, value, prefix):
    """Used to get the provider value given the enum"""
    try:
        keys = target.keys()
        if value in keys:
            return value.replace(f"{prefix}_", "")
        raise ValueError
    except Exception:
        raise RuntimeError(f"Invalid value {value} for {target}")
