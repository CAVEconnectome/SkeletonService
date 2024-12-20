from flask import Flask, jsonify, url_for, redirect, Blueprint
from skeletonservice.config import configure_app

# from skeletonservice.database import Base
from skeletonservice.utils import get_instance_folder_path
from skeletonservice.datasets.api import api_bp
from skeletonservice.datasets.views import views_bp
from skeletonservice.datasets.service import SKELETON_DEFAULT_VERSION_PARAMS, SKELETON_VERSION_PARAMS
from flask_restx import Api
from flask_cors import CORS
import logging

# from flask_migrate import Migrate
from werkzeug.routing import BaseConverter

from werkzeug.middleware.proxy_fix import ProxyFix
from middle_auth_client import auth_required

__version__ = "0.11.0"

# migrate = Migrate()


class BoolConverter(BaseConverter):
    def to_python(self, value):
        if value.lower() in ["true", "1"]:
            return True
        elif value.lower() in ["false", "0"]:
            return False
        else:
            return None

    def to_url(self, value):
        return str(value).lower()


def has_no_empty_params(rule):
    defaults = rule.defaults if rule.defaults is not None else ()
    arguments = rule.arguments if rule.arguments is not None else ()
    return len(defaults) >= len(arguments)


def create_app(test_config=None):
    # Define the Flask Object
    app = Flask(
        __name__,
        instance_path=get_instance_folder_path(),
        instance_relative_config=True,
        static_url_path="/skeletoncache/static",
        static_folder="../static",
    )
    CORS(app, expose_headers="WWW-Authenticate")
    # app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_port=1, x_for=1, x_host=1, x_prefix=1)
    # app.wsgi_app = ReverseProxied(app.wsgi_app)
    logging.basicConfig(level=logging.DEBUG)

    # Boolean parameter aren't natively supported. They require a custom converter.
    app.url_map.converters["bool"] = BoolConverter

    # load configuration (from test_config if passed)
    if test_config is None:
        app = configure_app(app)
    else:
        app.config.update(test_config)

    baseapi_bp = Blueprint("api", __name__, url_prefix="/skeletoncache/api")
    # CORS(baseapi_bp, expose_headers="WWW-Authenticate")
    # CORS(api_bp, expose_headers="WWW-Authenticate")

    @auth_required
    @baseapi_bp.route("/version")
    def version():
        # Until indicated otherwise, I will use the latest GIT tag as the version
        # (but I was just as likely forget to maintain it here and it will fall out-of-date, sigh)
        return jsonify(__version__), 200

    @auth_required
    @baseapi_bp.route("/versions")
    def versions():
        return jsonify(SKELETON_DEFAULT_VERSION_PARAMS + list(SKELETON_VERSION_PARAMS.keys())), 200

    with app.app_context():
        app.register_blueprint(views_bp, url_prefix="/skeletoncache")
        api = Api(
            baseapi_bp, title="Skeletonservice API", version=__version__, doc="/doc"
        )
        api.add_namespace(api_bp, path="/v1")

        app.register_blueprint(baseapi_bp)

    @app.route("/skeletoncache/health")
    def health():
        return jsonify("healthy"), 200

    @auth_required
    @app.route("/skeletoncache/site-map")
    def site_map():
        links = []
        for rule in app.url_map.iter_rules():
            # Filter out rules we can't navigate to in a browser
            # and rules that require parameters
            if "GET" in rule.methods and has_no_empty_params(rule):
                url = url_for(rule.endpoint, **(rule.defaults or {}))
                links.append((url, rule.endpoint))
        # links is now a list of url, endpoint tuples
        return jsonify(links)

    return app
