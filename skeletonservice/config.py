# Define the application directory
import os
from flask_sqlalchemy import SQLAlchemy
from skeletonservice.datasets.models import Base
import json


class BaseConfig(object):
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    # Statement for enabling the development environment
    DEBUG = True
    # Define the database - we are working with
    # SQLite for this example
    SQLALCHEMY_DATABASE_URI = "postgres://postgres:postgres@localhost:5432/datasets"

    DATABASE_CONNECT_OPTIONS = {}
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    # Application threads. A common general assumption is
    # using 2 per available processor cores - to handle
    # incoming requests using one and performing background
    # operations using the other.
    THREADS_PER_PAGE = 2

    # Enable protection agains *Cross-site Request Forgery (CSRF)*
    CSRF_ENABLED = True

    # Use a secure, unique and absolutely secret key for
    # signing the data.
    CSRF_SESSION_KEY = "SECRETSESSION"

    # Secret key for signing cookies
    SECRET_KEY = b"SECRETKEY"

    NEUROGLANCER_URL = "https://neuroglancer-demo.appspot.com"
    if os.environ.get("DAF_CREDENTIALS", None) is not None:
        with open(os.environ.get("DAF_CREDENTIALS"), "r") as f:
            AUTH_TOKEN = json.load(f)["token"]
    else:
        AUTH_TOKEN = ""


config = {
    "development": "skeletonservice.config.BaseConfig",
    "testing": "skeletonservice.config.BaseConfig",
    "default": "skeletonservice.config.BaseConfig",
}


def configure_app(app):
    config_name = os.getenv("FLASK_CONFIGURATION", "default")
    # object-based default configuration
    app.config.from_object(config[config_name])
    if os.environ.get("SKELETONSERVICE_SETTINGS", None) is not None:
        config_file = os.environ.get("SKELETONSERVICE_SETTINGS")
        if os.path.exists(config_file):
            app.config.from_envvar("SKELETONSERVICE_SETTINGS")
    else:
        # instance-folders configuration
        app.config.from_pyfile("config.cfg", silent=True)
    db = SQLAlchemy(model_class=Base)
    from .datasets.schemas import ma

    db.init_app(app)
    ma.init_app(app)
    return app
