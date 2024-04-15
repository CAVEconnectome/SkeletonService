# Import flask dependencies
from flask import jsonify, render_template, current_app, make_response, Blueprint
from flask_accepts import accepts, responds
from flask_restx import Namespace, Resource
from werkzeug.routing import BaseConverter
from meshparty import skeleton
from skeletonservice.datasets import schemas
from skeletonservice.datasets.service import (
   SkeletonService,
)
from typing import List, Dict
from middle_auth_client import (
    auth_required,
    auth_requires_permission,
    user_has_permission,
)

__version__ = "3.17.1"


authorizations = {
    "apikey": {"type": "apiKey", "in": "query", "name": "middle_auth_token"}
}

api_bp = Namespace(
    "Skeletonservice", authorizations=authorizations, description="Infoservice"
)


@api_bp.route("/skeletons")
@api_bp.doc("get skeletons", security="apikey")
class SkeletonsResource(Resource):
    """Skeletons"""

    @auth_required
    def get(self) -> List:
        """Get all skeletons"""
        skeletons = [
            a
            for a in SkeletonService.get_all()
            if True  # user_has_permission("view", a.name, "skeleton")
        ]

        return [sk["name"] for sk in skeletons]


# I can't override default parameters! AUGH!
# I can create optional parameters that don't have to be supplied, and I can create default values to populate them,
# but when I then supply overriding values in the web UI, those values never arrive by the time get() is called.
# Only the defaults defined in route() are ever received. This is driving me crazy.
@api_bp.route("/default_parameter_overriding_error_investigation/<int:foo>/", defaults={'bar': 123})  # Only this default value is ever received in get(), below.
@api_bp.route("/default_parameter_overriding_error_investigation/<int:foo>/<int:bar>")
@api_bp.param("foo", "foo")
@api_bp.param("bar", "bar")
class ParameterBugInvestigation(Resource):
    """ParameterBugInvestigation"""

    @api_bp.doc("foobar", security="apikey")
    def get(self, foo: int, bar: int):
        """Get foobar"""
        # bar will always be received as 123 (the default defined above). Providing an overrided value in the web UI makes no difference.
        # The URL encodes optional parameters as named args (kwargs) instead of enumerated args (args).
        # For example, if I enter 888 for foo and 999 for bar in the web UI, the URL will be as follows:
        # http://localhost:5000/properties/api/v1/skeleton2/888/?bar=999
        print(f"Foo: {foo}, Bar: {bar}")
        return {'foo': foo, 'bar': bar}


# The skeletonization defaults were taken from https://caveconnectome.github.io/pcg_skel/tutorial/
@api_bp.route("/skeleton/<int:rid>/<string:output_format>/", defaults={'sid': 0, 'datastack': 'minnie65_public', 'materialize_version': 795,
                                                'root_res_x': 1, 'root_res_y': 1, 'root_res_z': 1, 'collapse_soma': True, 'collapse_radius': 7500})
@api_bp.route("/skeleton/<int:rid>/<string:output_format>/<int:sid>/<string:datastack>/<int:materialize_version>/<int:root_res_x>/<int:root_res_y>/<int:root_res_z>/<bool:collapse_soma>/<int:collapse_radius>")
@api_bp.param("rid", "Skeleton Root Id")
@api_bp.param("sid", "Skeleton Nucleus Id")
@api_bp.param("datastack", "Datastack")
@api_bp.param("materialize_version", "Materialize version")
@api_bp.param("root_res_x", "Root resolution X in nm")
@api_bp.param("root_res_y", "Root resolution Y in nm")
@api_bp.param("root_res_z", "Root resolution Z in nm")
@api_bp.param("collapse_soma", "Whether to collapse the soma")
@api_bp.param("collapse_radius", "Collapse radius")
class SkeletonResource(Resource):
    """Skeletons"""

    # @responds(schema=schemas.SkeletonSchema)
    @api_bp.doc("get skeleton", security="apikey")
    # @auth_requires_permission(
    #     "view", table_arg="skeleton", resource_namespace="skeleton"
    # )
    def get(self, rid: int, output_format: str, sid: int, datastack: str, materialize_version: int,
            root_res_x: float, root_res_y: float, root_res_z: float, collapse_soma: bool, collapse_radius: int) -> skeleton:
        """Get skeleton By Root ID"""

        # TODO: I made most parameters optional, as shown above, and the web UI correctly exposes two endpoints, one without the parameters and one with them.
        # If I enter overriding values in the web UI, they are passed into the URL as named arguments (not as enumerated arguments), which I suppose is fine,
        # but the overrided values are ignored by the time this function is called. Instead, the default values created in the route decorator above are received here.

        # return {'rid': rid, 'collapse_soma': 'True' if collapse_soma else 'False', 'collapse_radius': collapse_radius}
        return SkeletonService.get_skeleton_by_rid_sid(rid, output_format, sid, datastack, materialize_version,
                                                       [root_res_x, root_res_y, root_res_z], collapse_soma, collapse_radius)
