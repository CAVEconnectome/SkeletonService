from flask import jsonify, render_template, current_app, make_response, Blueprint, g
from skeletonservice.datasets.service import (
    SkeletonService,
)
from skeletonservice.datasets.base_spelunker import spelunker_state
from middle_auth_client import (
    auth_required,
    user_has_permission,
    auth_requires_permission,
)
from caveclient import CAVEclient
import flask
import os
import numpy as np

__version__ = "0.16.6"

views_bp = Blueprint("skeletons", __name__, url_prefix="/skeletons")
