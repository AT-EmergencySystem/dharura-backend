from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class SubscriberModel(db.Model):
    __tablename__ = "subscribers"

    id = db.Column(db.Integer, primary_key=True)
    msisdn = db.Column(db.String(13), unique=True)
    occupation = db.Column(db.String(80))


class EmergencyModel(db.Model):
    __tablename__ = "emergencies"

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(80))
    description = db.Column(db.String())
    date_reported = db.Column(db.String(80))

