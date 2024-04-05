# Import flask dependencies
from flask import jsonify, render_template, current_app, make_response, Blueprint
from flask_accepts import accepts, responds
from flask_restx import Namespace, Resource
from skeletonservice.datasets import schemas
from skeletonservice.datasets.service import (
   SkeletonService,
)
from typing import List
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


@api_bp.route("/skeleton/<string:skeleton>")
@api_bp.param("skeleton", "Skeleton Name")
class SkeletonNameResource(Resource):
    """Skeleton by Name"""

    @responds(schema=schemas.SkeletonSchema)
    @api_bp.doc("get skeleton", security="apikey")
    # @auth_requires_permission(
    #     "view", table_arg="skeleton", resource_namespace="skeleton"
    # )
    def get(self, skeleton: str) -> schemas.SkeletonSchema:
        """Get Skeleton By Name"""

        return SkeletonService.get_skeleton_by_name(skeleton)
