import africastalking

africastalking.initialize(
    username="sandbox",
    api_key="73e380fcbb80fd5190ccfa9c43e0a65c57a307479982e202bc2614bc102c40dc"
)

sms = africastalking.SMS


def send_subscription_alert(recipient):
    message = "You Have Successfuly Subscribed To Dharura System"
    sender = "32721"
    recp = [recipient]
    try:
        response = sms.send(message, recp, sender)
        print(response)
    except Exception as e:
        print(e)

