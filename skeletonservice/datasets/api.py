# Import flask dependencies
from flask import jsonify, render_template, current_app, make_response, Blueprint
from flask_accepts import accepts, responds
from flask_restx import Namespace, Resource
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
        """Get all Skeletons"""
        skeletons = [
            a
            for a in SkeletonService.get_all()
            if True  # user_has_permission("view", a.name, "skeleton")
        ]

        return [sk["name"] for sk in skeletons]


@api_bp.route("/skeleton/<int:rid>/<int:sid>/<string:datastack>/<int:materialize_version>")
@api_bp.route("/skeleton/<int:rid>/", defaults={'sid': 0, 'datastack': 'minnie65_public', 'materialize_version': 795})
@api_bp.param("rid", "Skeleton Root Id")
@api_bp.param("sid", "Skeleton Nucleus Id")
@api_bp.param("datastack", "Datastack")
@api_bp.param("materialize_version", "Materialize version")
class SkeletonResource(Resource):
    """Skeleton by Rid/Sid"""

    # @responds(schema=schemas.SkeletonSchema)
    @api_bp.doc("get skeleton", security="apikey")
    # @auth_requires_permission(
    #     "view", table_arg="skeleton", resource_namespace="skeleton"
    # )
    def get(self, rid: int, sid: int, datastack: str, materialize_version: int) -> skeleton:
        """Get Skeleton By Name"""

        # TODO: I made SID optional, as shown above, and the web UI correctly exposes two endpoints, one without SID,
        # but that web endpoint offers the SID as available (that's fair, I suppose), but if I enter a value in the web UI,
        # it is passed into the URL as a named argument (not an enumerated argument) and it is then ignored by the time this function is called.
        # The default value created in the 'route' above is received here instead.

        return SkeletonService.get_skeleton_by_rid_sid(rid, sid, datastack, materialize_version)
