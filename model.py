from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class SubscriberModel(db.Model):
    __tablename__ = "subscriber_model"

    id = db.Column(db.Integer, primary_key=True)
    msisdn = db.Column(db.String(13), unique=True)
    occupation = db.Column(db.String(80))

