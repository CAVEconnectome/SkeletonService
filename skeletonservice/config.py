# Define the application directory
import os
# from skeletonservice.datasets.models import Base
import json

from skeletonservice.datasets.service_skvn1 import SkeletonService_skvn1
from skeletonservice.datasets.service_skvn2 import SkeletonService_skvn2


class BaseConfig(object):
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    # Statement for enabling the development environment
    DEBUG = True

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
    SKELETON_CACHE_EXCHANGE = "CONFIGURE_ME"  # "ltv5_SKELETON_CACHE"

    NEUROGLANCER_URL = "https://neuroglancer-demo.appspot.com"
    if os.environ.get("DAF_CREDENTIALS", None) is not None:
        with open(os.environ.get("DAF_CREDENTIALS"), "r") as f:
            AUTH_TOKEN = json.load(f)["token"]
    else:
        AUTH_TOKEN = ""

    SKELETON_VERSION_ENGINES = {
        1: SkeletonService_skvn1,
        2: SkeletonService_skvn2,
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

    # ma.init_app(app)
    return app
