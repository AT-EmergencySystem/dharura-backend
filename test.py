import africastalking

africastalking.initialize(
    username="justinecodez",
    api_key="ef66a07e76f50f4e0093645e6e1897589e74304bb7e9ddc02a30579a5df2fc13"
)

sms = africastalking.SMS.send(
    "Hello WORLD",
    ["+255758405095"]
)