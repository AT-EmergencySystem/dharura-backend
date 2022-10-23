import africastalking
import sqlite3
from model import SubscriberModel

africastalking.initialize(
    username="sandbox",
    api_key="XXXX"
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


def subscriber_pull(title, description):
    # msisdns = SubscriberModel.query.order_by(SubscriberModel.msisdn.desc()).all()
    # return SubscriberModel.fs_dict_list(msisdns)
    con = sqlite3.connect('instance/sample_db.db')
    cur = con.cursor()
    nums = cur.execute('SELECT msisdn FROM subscribers;')

    msisdns = cur.fetchall()

    sender = "32721"

    # for i in msisdns:
    #     try:
    #         message = f"{title}, {description}"
    #         response = sms.send(message, i[0], sender)
    #         print(response)
    #     except Exception as e:
    #         print(e)

    try:
        messages = f"{title}, {description}"
        sender = "32721"
        recipient = ["+255758405095"]
        response = sms.send(messages, recipient, sender)
        print(response)
    except Exception as e:
        print(e)

def send_notifications_all(title, description):
    msisdns = SubscriberModel.query.order_by(SubscriberModel.msisdn.desc()).all()
