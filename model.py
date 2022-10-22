from flask_sqlalchemy import SQLAlchemy
from flask_serialize import FlaskSerialize

db = SQLAlchemy()
fs_mixin = FlaskSerialize(db)


class SubscriberModel(db.Model, fs_mixin):
    __tablename__ = "subscribers"

    id = db.Column(db.Integer, primary_key=True)
    msisdn = db.Column(db.String(13), unique=True)
    occupation = db.Column(db.String(80))

    __fs_create_fields__ = __fs_update_fields__ = ['msisdn', 'occupation']


class EmergencyModel(db.Model):
    __tablename__ = "emergencies"

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(80))
    description = db.Column(db.String())
    date_reported = db.Column(db.String(80))

