from flask import Flask, request, redirect, jsonify, abort
from model import SubscriberModel, db

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = 'sqlite:///sample_db.db'
app.config["SQLALCHEMY_TRACK_MODIFICATION"] = False
db.init(app)

@app.before_first_request()
def create_tables():
    db.create_all()


@app.route("/subscribe", methods=['POST'])
def subscriber_register():
    if request.method == 'POST':
        msisdn = request.form['phoneNumber']
        occupation = request.form['Occupation']
        if msisdn[:1] == "+" and msisdn[:4] == "+255":
            if occupation == "Staff" or occupation == "Student":
                subscriber = SubscriberModel(msisdn=msisdn, occupation=occupation)
                db.session.add(subscriber)
                db.session.commit()
                return jsonify({"STAT": "Subscriber Successfully"})
            else:
                return jsonify({"STAT": "Occupation Not Clear"})
        else:
            return jsonify({"STAT": "MSISDN/Phone Number Not Clear"})
    else:
        return abort(403)
