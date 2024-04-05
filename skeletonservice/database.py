from flask import g, current_app
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


def get_db():
    if 'db' not in g:
        g.db = SQLAlchemy(current_app)
        g.ma = Marshmallow(current_app)
    return g.db, g.ma
