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


@api_bp.route("/skeleton/<int:rid>/<int:sid>")
@api_bp.param("rid", "Skeleton Root Id")
@api_bp.param("sid", "Skeleton Nucleus Id")
class SkeletonNameResource(Resource):
    """Skeleton by Rid/Sid"""

    # @responds(schema=schemas.SkeletonSchema)
    @api_bp.doc("get skeleton", security="apikey")
    # @auth_requires_permission(
    #     "view", table_arg="skeleton", resource_namespace="skeleton"
    # )
    def get(self, rid: int, sid: int) -> skeleton:
        """Get Skeleton By Name"""

        return SkeletonService.get_skeleton_by_rid_sid(rid, sid)
