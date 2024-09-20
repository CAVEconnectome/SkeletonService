import os
# Import flask dependencies
from flask import jsonify, render_template, current_app, make_response, Blueprint
from flask_accepts import accepts, responds
from flask_restx import Namespace, Resource
from werkzeug.routing import BaseConverter
from meshparty import skeleton
from skeletonservice.datasets import schemas
from skeletonservice.datasets.service import SKELETON_VERSION_PARAMS, SkeletonService

from middle_auth_client import (
    auth_required,
    auth_requires_permission,
    user_has_permission,
)

from typing import List, Dict

__version__ = "0.2.5"


authorizations = {
    "apikey": {"type": "apiKey", "in": "query", "name": "middle_auth_token"}
}

api_bp = Namespace(
    "Skeletonservice", authorizations=authorizations, description="Skeleton Service"
)

@api_bp.route("/skeletons")
@api_bp.doc("get skeletons", security="apikey")
class SkeletonResource1(Resource):
    """Skeletons"""

    @auth_required
    def get(self, datastack_name: str) -> List:
        """Get all skeletons"""
        skeletons = [
            s
            for s in SkeletonService.get_all()
            # See AnnotationFrameworkInfoService for an example of how to use user_has_permission.
            # I have disabled it until I can figure out how to get the user's permissions for skeleton objects.
            if True  # user_has_permission("view", s.name, "skeleton")
        ]

        return [sk["name"] for sk in skeletons]


# I can't override default parameters! AUGH!
# I can create optional parameters that don't have to be supplied, and I can create default values to populate them,
# but when I then supply overriding values in the web UI, those values never arrive by the time get() is called.
# Only the defaults defined in route() are ever received. This is driving me crazy.
# @api_bp.route("/default_parameter_overriding_error_investigation/<int:foo>/", defaults={'bar': 123})  # Only this default value is ever received in get(), below.
# @api_bp.route("/default_parameter_overriding_error_investigation/<int:foo>/<int:bar>")
# @api_bp.param("foo", "foo")
# @api_bp.param("bar", "bar")
# class SkeletonResource2(Resource):
#     """ParameterBugInvestigation"""

#     @auth_required
#     @api_bp.doc("foobar", security="apikey")
#     def get(self, foo: int, bar: int):
#         """Get foobar"""
#         # bar will always be received as 123 (the default defined above). Providing an overrided value in the web UI makes no difference.
#         # The URL encodes optional parameters as named args (kwargs) instead of enumerated args (args).
#         # For example, if I enter 888 for foo and 999 for bar in the web UI, the URL will be as follows:
#         # http://localhost:5000/properties/api/v1/skeleton2/888/?bar=999
#         print(f"Foo: {foo}, Bar: {bar}")
#         return {'foo': foo, 'bar': bar}



@api_bp.route("/<string:datastack_name>/precomputed")
class SkeletonResource3(Resource):
    """PrecomputedResource"""

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("PrecomputedResource", security="apikey")
    def get(self, datastack_name: str):
        """Get precomputed"""

        return {
            "datastack_name": datastack_name,
        }

@api_bp.route("/<string:datastack_name>/precomputed/info")
class SkeletonResource4(Resource):
    """PrecomputedInfoResource"""

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("PrecomputedInfoResource", security="apikey")
    def get(self, datastack_name: str):
        """Get precomputed info"""
        
        # TODO: I presume that since having added datastack_name to the route, I should be using it here in some fashion.
        # For example, notice that "data_dir" below seems very datastack-specific.

        return {'app': {'supported_api_versions': [0, 1]},
            'chunks_start_at_voxel_offset': True,
            'data_dir': 'gs://minnie65_pcg/ws',
            'data_type': 'uint64',
            'graph': {'bounding_box': [2048, 2048, 512],
            'chunk_size': [256, 256, 512],
            'cv_mip': 0,
            'n_bits_for_layer_id': 8,
            'n_layers': 12,
            'spatial_bit_masks': {'1': 10,
            '10': 2,
            '11': 1,
            '12': 1,
            '2': 10,
            '3': 9,
            '4': 8,
            '5': 7,
            '6': 6,
            '7': 5,
            '8': 4,
            '9': 3}},
            'mesh': 'graphene_meshes',
            'mesh_metadata': {'uniform_draco_grid_size': 21.0,
            'unsharded_mesh_dir': 'dynamic'},
            'num_channels': 1,
            'scales': [{'chunk_sizes': [[256, 256, 32]],
            'compressed_segmentation_block_size': [8, 8, 8],
            'encoding': 'compressed_segmentation',
            'key': '8_8_40',
            'locked': True,
            'resolution': [8, 8, 40],
            'size': [192424, 131051, 13008],
            'voxel_offset': [26385, 30308, 14850]},
            {'chunk_sizes': [[256, 256, 32]],
            'compressed_segmentation_block_size': [8, 8, 8],
            'encoding': 'compressed_segmentation',
            'key': '16_16_40',
            'locked': True,
            'resolution': [16, 16, 40],
            'size': [96212, 65526, 13008],
            'voxel_offset': [13192, 15154, 14850]},
            {'chunk_sizes': [[256, 256, 32]],
            'compressed_segmentation_block_size': [8, 8, 8],
            'encoding': 'compressed_segmentation',
            'key': '32_32_40',
            'locked': True,
            'resolution': [32, 32, 40],
            'size': [48106, 32763, 13008],
            'voxel_offset': [6596, 7577, 14850]},
            {'chunk_sizes': [[256, 256, 32]],
            'compressed_segmentation_block_size': [8, 8, 8],
            'encoding': 'compressed_segmentation',
            'key': '64_64_40',
            'locked': True,
            'resolution': [64, 64, 40],
            'size': [24053, 16382, 13008],
            'voxel_offset': [3298, 3788, 14850]},
            {'chunk_sizes': [[128, 128, 16]],
            'compressed_segmentation_block_size': [8, 8, 8],
            'encoding': 'compressed_segmentation',
            'key': '128_128_80',
            'resolution': [128, 128, 80],
            'size': [12027, 8191, 6504],
            'voxel_offset': [1649, 1894, 7425]},
            {'chunk_sizes': [[128, 128, 16]],
            'compressed_segmentation_block_size': [8, 8, 8],
            'encoding': 'compressed_segmentation',
            'key': '256_256_160',
            'resolution': [256, 256, 160],
            'size': [6014, 4096, 3252],
            'voxel_offset': [824, 947, 3712]},
            {'chunk_sizes': [[128, 128, 16]],
            'compressed_segmentation_block_size': [8, 8, 8],
            'encoding': 'compressed_segmentation',
            'key': '512_512_320',
            'resolution': [512, 512, 320],
            'size': [3007, 2048, 1626],
            'voxel_offset': [412, 473, 1856]},
            {'chunk_sizes': [[128, 128, 16]],
            'encoding': 'raw',
            'key': '1024_1024_640',
            'resolution': [1024, 1024, 640],
            'size': [1504, 1024, 813],
            'voxel_offset': [206, 236, 928]},
            {'chunk_sizes': [[128, 128, 16]],
            'encoding': 'raw',
            'key': '2048_2048_1280',
            'resolution': [2048, 2048, 1280],
            'size': [752, 512, 407],
            'voxel_offset': [103, 118, 464]},
            {'chunk_sizes': [[128, 128, 16]],
            'encoding': 'raw',
            'key': '4096_4096_2560',
            'resolution': [4096, 4096, 2560],
            'size': [376, 256, 204],
            'voxel_offset': [51, 59, 232]}],
            'sharded_mesh': True,
            'skeletons': 'skeleton',
            'type': 'segmentation',
            'verify_mesh': False}



@api_bp.route("/<string:datastack_name>/precomputed/skeleton/info")
class SkeletonResource5(Resource):
    """SkeletonInfoResource"""

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonInfoResource", security="apikey")
    def get(self, datastack_name: str):
        """Get skeleton info"""
        
        # TODO: I presume that since having added datastack_name to the route, I should be using it here in some fashion

        return SKELETON_VERSION_PARAMS[sorted(SKELETON_VERSION_PARAMS.keys())[-1]]



@api_bp.route("/<string:datastack_name>/precomputed/skeleton/info/<int:skvn>")
class SkeletonResource5a(Resource):
    """SkeletonInfoResource"""

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonInfoResource", security="apikey")
    def get(self, datastack_name: str, skvn: int):
        """Get skeleton info"""
        
        # TODO: I presume that since having added datastack_name to the route, I should be using it here in some fashion

        if skvn not in current_app.config['SKELETON_VERSION_ENGINES'].keys():
            skvn = sorted(current_app.config['SKELETON_VERSION_ENGINES'].keys())[-1]
        return SKELETON_VERSION_PARAMS[skvn]


@api_bp.route("/<string:datastack_name>/precomputed/skeleton/<int:rid>")
class SkeletonResource6(Resource):
    """SkeletonResource"""

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, rid: int):
        """Get skeleton by rid"""
        
        # Use the latest version
        skvn = sorted(current_app.config['SKELETON_VERSION_ENGINES'].keys())[-1]
        SkelClassVsn = current_app.config['SKELETON_VERSION_ENGINES'][skvn]

        return SkelClassVsn.get_skeleton_by_datastack_and_rid(
        # return SkeletonService.get_skeleton_by_datastack_and_rid(
            datastack_name=datastack_name,
            rid=rid,
            output_format='precomputed',
            bucket=current_app.config["SKELETON_CACHE_BUCKET"],
            root_resolution=[1, 1, 1],
            collapse_soma=True,
            collapse_radius=7500,
            skeleton_version=0,
            verbose_level_=-1,
        )



@api_bp.route("/<string:datastack_name>/precomputed/skeleton/<int:skvn>/<int:rid>")
class SkeletonResource6a(Resource):
    """SkeletonResource"""

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, skvn: int, rid: int):
        """Get skeleton by rid"""

        # If no skeleton version is specified or an illegal version is specified, then use the latest version
        if skvn not in current_app.config['SKELETON_VERSION_ENGINES'].keys():
            skvn = sorted(current_app.config['SKELETON_VERSION_ENGINES'].keys())[-1]
        SkelClassVsn = current_app.config['SKELETON_VERSION_ENGINES'][skvn]

        return SkelClassVsn.get_skeleton_by_datastack_and_rid(
        # return SkeletonService.get_skeleton_by_datastack_and_rid(
            datastack_name=datastack_name,
            rid=rid,
            output_format='precomputed',
            bucket=current_app.config["SKELETON_CACHE_BUCKET"],
            root_resolution=[1, 1, 1],
            collapse_soma=True,
            collapse_radius=7500,
            skeleton_version=skvn,
            verbose_level_=1,
        )



@api_bp.route("/<string:datastack_name>/precomputed/skeleton/<int:skvn>/<int:rid>/<string:output_format>")
class SkeletonResource6b(Resource):
    """SkeletonResource"""

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, skvn: int, rid: int, output_format: str):
        """Get skeleton by rid"""

        if output_format == 'none':
            return SkeletonResource7a.process(datastack_name, skvn, rid)

        # If no skeleton version is specified or an illegal version is specified, then use the latest version
        if skvn not in current_app.config['SKELETON_VERSION_ENGINES'].keys():
            skvn = sorted(current_app.config['SKELETON_VERSION_ENGINES'].keys())[-1]
        SkelClassVsn = current_app.config['SKELETON_VERSION_ENGINES'][skvn]

        return SkelClassVsn.get_skeleton_by_datastack_and_rid(
        # return SkeletonService.get_skeleton_by_datastack_and_rid(
            datastack_name=datastack_name,
            rid=rid,
            output_format=output_format,
            bucket=current_app.config["SKELETON_CACHE_BUCKET"],
            root_resolution=[1, 1, 1],
            collapse_soma=True,
            collapse_radius=7500,
            skeleton_version=skvn,
            verbose_level_=1,
        )



@api_bp.route("/<string:datastack_name>/precomputed_via_msg/skeleton/<int:rid>")
class SkeletonResource7(Resource):
    """SkeletonResource"""

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, rid: int):
        """Get skeleton by rid"""
        return SkeletonResource7a.process(datastack_name, 0, rid)
    
        # from messagingclient import MessagingClient

        # payload = b""
        # attributes = {
        #     "skeleton_params_rid": f"{rid}",
        #     "skeleton_params_bucket": current_app.config["SKELETON_CACHE_BUCKET"],
        #     "skeleton_params_datastack_name": datastack_name,
        #     "skeleton_params_root_resolution": "1 1 1",
        #     "skeleton_params_collapse_soma": "True",
        #     "skeleton_params_collapse_radius": "7500",
        #     "skeleton_version": "0",
        #     "verbose_level": "1",
        # }

        # c = MessagingClient()
        # exchange = os.getenv("SKELETON_CACHE_EXCHANGE", "skeleton")
        # c.publish(exchange, payload, attributes)

        # return f"Message has been dispatched to {exchange}: {datastack_name} {rid} {current_app.config['SKELETON_CACHE_BUCKET']}"



@api_bp.route("/<string:datastack_name>/precomputed_via_msg/skeleton/<int:skvn>/<int:rid>")
class SkeletonResource7a(Resource):
    """SkeletonResource"""

    @staticmethod
    def process(datastack_name: str, skvn: int, rid: int):
        from messagingclient import MessagingClient

        payload = b""
        attributes = {
            "skeleton_params_rid": f"{rid}",
            "skeleton_params_bucket": current_app.config["SKELETON_CACHE_BUCKET"],
            "skeleton_params_datastack_name": datastack_name,
            "skeleton_params_root_resolution": "1 1 1",
            "skeleton_params_collapse_soma": "True",
            "skeleton_params_collapse_radius": "7500",
            "skeleton_version": f"{skvn}",
            "verbose_level": "1",
        }

        c = MessagingClient()
        exchange = os.getenv("SKELETON_CACHE_EXCHANGE", "skeleton")
        c.publish(exchange, payload, attributes)

        return f"Message has been dispatched to {exchange}: {datastack_name} {rid} skvn{skvn} {current_app.config['SKELETON_CACHE_BUCKET']}"


    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, skvn: int, rid: int):
        """Get skeleton by rid"""
        return self.process(datastack_name, skvn, rid)

        # from messagingclient import MessagingClient

        # payload = b""
        # attributes = {
        #     "skeleton_params_rid": f"{rid}",
        #     "skeleton_params_bucket": current_app.config["SKELETON_CACHE_BUCKET"],
        #     "skeleton_params_datastack_name": datastack_name,
        #     "skeleton_params_root_resolution": "1 1 1",
        #     "skeleton_params_collapse_soma": "True",
        #     "skeleton_params_collapse_radius": "7500",
        #     "skeleton_version": f"{skvn}",
        #     "verbose_level": "1",
        # }

        # c = MessagingClient()
        # exchange = os.getenv("SKELETON_CACHE_EXCHANGE", "skeleton")
        # c.publish(exchange, payload, attributes)

        # return f"Message has been dispatched to {exchange}: {datastack_name} {rid} skvn{skvn} {current_app.config['SKELETON_CACHE_BUCKET']}"
