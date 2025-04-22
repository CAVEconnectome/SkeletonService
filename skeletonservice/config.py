# Define the application directory
import logging
import os
# from skeletonservice.datasets.models import Base
import json
import sys
from flask.logging import default_handler

from .datasets.service_skvn1 import SkeletonService_skvn1
from .datasets.service_skvn2 import SkeletonService_skvn2
from .datasets.service_skvn3 import SkeletonService_skvn3
from .datasets.service_skvn4 import SkeletonService_skvn4


class BaseConfig(object):
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    # Statement for enabling the development environment
    DEBUG = True

    LOGGING_LEVEL = logging.WARNING

    # Application threads. A common general assumption is
    # using 2 per available processor cores - to handle
    # incoming requests using one and performing background
    # operations using the other.
    THREADS_PER_PAGE = 2

    # Enable protection against *Cross-site Request Forgery (CSRF)*
    CSRF_ENABLED = True

    # Use a secure, unique and absolutely secret key for
    # signing the data.
    CSRF_SESSION_KEY = "SECRETSESSION"

    # Secret key for signing cookies
    SECRET_KEY = b"SECRETKEY"

    # I'm not sure this needs to be here, even as a to-be-filled-in-later stub
    SKELETON_CACHE_BUCKET = "CONFIGURE_ME"  # "gs://keith-dev/"
    SKELETON_CACHE_LOW_PRIORITY_EXCHANGE = "CONFIGURE_ME"  # "ltv5_SKELETON_CACHE"
    SKELETON_CACHE_HIGH_PRIORITY_EXCHANGE = "CONFIGURE_ME"  # "ltv5_SKELETON_CACHE"

    LIMITER_CATEGORIES = "CONFIGURE_ME"
    LIMITER_URI = "CONFIGURE_ME"

    NEUROGLANCER_URL = "https://neuroglancer-demo.appspot.com"
    if os.environ.get("DAF_CREDENTIALS", None) is not None:
        with open(os.environ.get("DAF_CREDENTIALS"), "r") as f:
            AUTH_TOKEN = json.load(f)["token"]
    else:
        AUTH_TOKEN = ""

    SKELETON_VERSION_ENGINES = {
        1: SkeletonService_skvn1,
        2: SkeletonService_skvn2,
        3: SkeletonService_skvn3,
        4: SkeletonService_skvn4,
    }

    # GLOBAL_SERVER_URL = os.environ.get("GLOBAL_SERVER_URL")
    # LOCAL_SERVER_URL = os.environ.get("LOCAL_SERVER_URL")
    # ANNO_ENDPOINT = f"{LOCAL_SERVER_URL}/annotation/"
    # INFOSERVICE_ENDPOINT = f"{GLOBAL_SERVER_URL}/info"
    # AUTH_URI = f"{GLOBAL_SERVER_URL}/auth"

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
    # from .datasets.schemas import ma

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(app.config["LOGGING_LEVEL"])
    app.logger.removeHandler(default_handler)
    app.logger.addHandler(handler)
    app.logger.setLevel(app.config["LOGGING_LEVEL"])
    app.logger.propagate = False

    # ma.init_app(app)
    return app
