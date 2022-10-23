from flask import Flask, request, redirect, jsonify, abort
from model import SubscriberModel, db
from modules import send_subscription_alert, subscriber_pull

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = 'sqlite:///sample_db.db'
app.config["SQLALCHEMY_TRACK_MODIFICATION"] = False
db.init_app(app)

@app.before_first_request
def create_tables():
    db.create_all()


@app.route("/subscribe", methods=['POST'])
def subscriber_register():
    if request.method == 'POST':
        msisdn = request.form['phoneNumber']
        occupation = request.form['Occupation']
        if msisdn[:1] == "+" and msisdn[:4] == "+255":
            if occupation == "Staff" or occupation == "Student":
                if SubscriberModel.query.filter_by(msisdn=msisdn).first() is None:
                    subscriber = SubscriberModel(msisdn=msisdn, occupation=occupation)
                    db.session.add(subscriber)
                    db.session.commit()
                    send_subscription_alert(msisdn)

                    return redirect('https://emergency-system.netlify.app/')
                else:
                    return jsonify({"STAT": "Occupation Not Clear"})
            else:
                return jsonify({"STAT": "Subscriber Registered"})
        else:
            return jsonify({"STAT": "MSISDN/Phone Number Not Clear"})
    else:
        return abort(403)


@app.route("/push_notification", methods=['POST'])
def push_notification():
    title = request.form['title']
    description = request.form['description']

    subscriber_pull(title, description)

    return redirect('https://emergency-system.netlify.app/')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)