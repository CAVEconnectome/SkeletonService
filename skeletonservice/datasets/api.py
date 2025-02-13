import logging
import os
# Import flask dependencies
from flask import jsonify, render_template, current_app, request, make_response, Blueprint
from flask_accepts import accepts, responds
from flask_restx import Namespace, Resource, reqparse
from werkzeug.routing import BaseConverter
from meshparty import skeleton
from skeletonservice.datasets import schemas
from skeletonservice.datasets.service import NEUROGLANCER_SKELETON_VERSION, SKELETON_DEFAULT_VERSION_PARAMS, SKELETON_VERSION_PARAMS, SkeletonService

from middle_auth_client import (
    auth_required,
    auth_requires_permission,
    user_has_permission,
)

from typing import List, Dict

__version__ = "0.14.4"


authorizations = {
    "apikey": {"type": "apiKey", "in": "query", "name": "middle_auth_token"}
}

api_bp = Namespace(
    "Skeletonservice", authorizations=authorizations, description="Skeleton Service"
)

bulk_async_parser = reqparse.RequestParser()
bulk_async_parser.add_argument(
    "root_ids",
    type=List,
    default=True,
    location="args",
    help="list of root ids",
)
bulk_async_parser.add_argument(
    "skeleton_version",
    type=int,
    default=True,
    location="args",
    help="skeleton version",
)

@api_bp.route("/skeletons")
@api_bp.doc("get skeletons", security="apikey")
class SkeletonResource__get_skeletons(Resource):
    """Skeletons"""

    @auth_required
    def get(self) -> List:
        return ["Placeholder for /skeletons endpoint, of an as-yet undetermined usefulness."]


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
class SkeletonResource__datastack(Resource):
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
class SkeletonResource__precomputed_info(Resource):
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
class SkeletonResource__skeleton_version_info_A(Resource):
    """SkeletonInfoResource"""

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonInfoResource", security="apikey")
    def get(self, datastack_name: str):
        """Get skeleton info"""
        
        # TODO: I presume that since having added datastack_name to the route, I should be using it here in some fashion

        return SkeletonResource__skeleton_version_info_B.process(NEUROGLANCER_SKELETON_VERSION)



@api_bp.route("/<string:datastack_name>/precomputed/skeleton/<int(signed=True):skvn>/info")
class SkeletonResource__skeleton_version_info_B(Resource):
    """SkeletonInfoResource"""

    @staticmethod
    def process(skvn):
        if skvn not in current_app.config['SKELETON_VERSION_ENGINES'].keys():
            raise ValueError(f"Invalid skeleton version: v{skvn}. Valid versions: {list(SKELETON_VERSION_PARAMS.keys())}")
        return SKELETON_VERSION_PARAMS[skvn]

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonInfoResource", security="apikey")
    def get(self, datastack_name: str, skvn: int):
        """Get skeleton info"""
        
        # TODO: I presume that since having added datastack_name to the route, I should be using it here in some fashion
        return self.process(skvn)



@api_bp.route("/<string:datastack_name>/bulk/skeleton/info")
class SkeletonResource__bulk_skeleton_info(Resource):
    """SkeletonInfoResource"""

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonInfoResource", security="apikey")
    def get(self, datastack_name: str):
        """Get skeleton info"""
        
        # TODO: I presume that since having added datastack_name to the route, I should be using it here in some fashion

        return {
            "bulk skeletons": "Use the bulk skeletons endpoint to get skeletons for multiple root ids at once.",
        }

# NOTE: Use of this endpoint has been removed from CAVEclient:SkeletonService, but it can't be removed from here if there are any older clients in the wild that might access it.
@api_bp.route("/<string:datastack_name>/precomputed/skeleton/query_cache/<string:root_id_prefixes>/<int:limit>")
class SkeletonResource__query_cache_A(Resource):
    """SkeletonResource"""

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, root_id_prefixes: str, limit: int):
        """
        Get skeletons in cache by root_id_prefix
        
        root_id_prefixes could be a single int (as a string), a single string (i.e. one int as a string), or a comma-separated list of strings (i.e. multiple ints as a single string).
        """
        logging.warning("The endpoint for getting skeletons without specifying a skeleton version will be deprecated in the future. Please specify a skeleton version.")
        
        # Use the NeuroGlancer compatible version\
        return SkeletonResource__query_cache_B.process(NEUROGLANCER_SKELETON_VERSION, root_id_prefixes, limit)


@api_bp.route("/<string:datastack_name>/precomputed/skeleton/query_cache/<int(signed=True):skvn>/<string:root_id_prefixes>/<int:limit>")
class SkeletonResource__query_cache_B(Resource):
    """SkeletonResource"""

    @staticmethod
    def process(skvn: int, root_id_prefixes: str, limit: int):
        SkelClassVsn = SkeletonService.get_version_specific_handler(skvn)
        
        return SkelClassVsn.get_cache_contents(
            bucket=current_app.config["SKELETON_CACHE_BUCKET"],
            skeleton_version=skvn,
            rid_prefixes=[int(v) for v in root_id_prefixes.split(',')],
            limit=limit,
            verbose_level_=0,
        )

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, skvn: int, root_id_prefixes: str, limit: int):
        """
        Get skeletons in cache by root_id_prefix
        
        root_id_prefixes could be a single int (as a string), a single string (i.e. one int as a string), or a comma-separated list of strings (i.e. multiple ints as a single string).
        """
        return self.process(skvn, root_id_prefixes, limit)

# NOTE: Use of this endpoint has been removed from CAVEclient:SkeletonService, but it can't be removed from here if there are any older clients in the wild that might access it.
@api_bp.route("/<string:datastack_name>/precomputed/skeleton/exists/<string:root_ids>")
class SkeletonResource__skeleton_exists_A(Resource):
    """SkeletonResource"""

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, root_ids: str):
        """
        Determine whether skeletons exist in the cache for a set of root ids
        
        root_ids could be a single int (as a string), a single string (i.e. one int as a string), or a comma-separated list of strings (i.e. multiple ints as a single string).
        """
        logging.warning("The endpoint for checking the existence of skeletons without specifying a skeleton version will be deprecated in the future. Please specify a skeleton version.")
        
        # Use the NeuroGlancer compatible version
        return SkeletonResource__skeleton_exists_B.process(NEUROGLANCER_SKELETON_VERSION, root_ids)


@api_bp.route("/<string:datastack_name>/precomputed/skeleton/exists/<int(signed=True):skvn>/<string:root_ids>")
class SkeletonResource__skeleton_exists_B(Resource):
    """SkeletonResource"""

    @staticmethod
    def process(skvn: int, root_ids: str):
        SkelClassVsn = SkeletonService.get_version_specific_handler(skvn)

        root_ids = [int(v) for v in root_ids.split(',')]
        if len(root_ids) == 1:
            # If requesting a single root_id, then return the single root_id as an int, not as a list of one int
            root_ids = int(root_ids[0])
        
        return SkelClassVsn.skeletons_exist(
            bucket=current_app.config["SKELETON_CACHE_BUCKET"],
            skeleton_version=skvn,
            rids=root_ids,
            verbose_level_=0,
        )

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, skvn: int, root_ids: str):
        """
        Determine whether skeletons exist in the cache for a set of root ids
        
        root_ids could be a single int (as a string), a single string (i.e. one int as a string), or a comma-separated list of strings (i.e. multiple ints as a single string).
        """
        return self.process(skvn, root_ids)


@api_bp.route("/<string:datastack_name>/precomputed/skeleton/exists")
class SkeletonResource__skeleton_exists_C(Resource):
    """SkeletonResource"""

    @staticmethod
    def process(skvn: int, root_ids: List):
        SkelClassVsn = SkeletonService.get_version_specific_handler(skvn)

        if len(root_ids) == 1:
            # If requesting a single root_id, then return the single root_id as an int, not as a list of one int
            root_ids = int(root_ids[0])
        
        return SkelClassVsn.skeletons_exist(
            bucket=current_app.config["SKELETON_CACHE_BUCKET"],
            skeleton_version=skvn,
            rids=root_ids,
            verbose_level_=0,
        )

    @api_bp.expect(bulk_async_parser)
    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def post(self, datastack_name: str):
        """
        Determine whether skeletons exist in the cache for a set of root ids
        
        root_ids could be a single int (as a string), a single string (i.e. one int as a string), or a comma-separated list of strings (i.e. multiple ints as a single string).
        """
        # data = request.parsed_obj  # Doesn't work, doesn't exist
        
        data = request.json
        rids = data['root_ids']
        skvn = data['skeleton_version']
        response = self.process(skvn, rids)
        return response, 200


@api_bp.route("/<string:datastack_name>/precomputed/skeleton/<int:rid>")
class SkeletonResource__get_skeleton_A(Resource):
    """SkeletonResource"""

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, rid: int):
        """Get skeleton by rid"""
        logging.warning("The endpoint for getting skeletons without specifying a skeleton version will be deprecated in the future. Please specify a skeleton version.")
        
        # Use the NeuroGlancer compatible version
        return SkeletonResource__get_skeleton_B.process(datastack_name, NEUROGLANCER_SKELETON_VERSION, rid)



@api_bp.route("/<string:datastack_name>/precomputed/skeleton/<int(signed=True):skvn>/<int:rid>")
class SkeletonResource__get_skeleton_B(Resource):
    """SkeletonResource"""

    @staticmethod
    def process(datastack_name: str, skvn: int, rid: int):
        return SkeletonResource__get_skeleton_C.process(datastack_name, skvn, rid, 'precomputed')

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, skvn: int, rid: int):
        """Get skeleton by rid"""
        return SkeletonResource__get_skeleton_C.process(datastack_name, skvn, rid, 'precomputed')



@api_bp.route("/<string:datastack_name>/precomputed/skeleton/<int(signed=True):skvn>/<int:rid>/<string:output_format>")
class SkeletonResource__get_skeleton_C(Resource):
    """SkeletonResource"""

    @staticmethod
    def process(datastack_name: str, skvn: int, rid: int, output_format: str):
        SkelClassVsn = SkeletonService.get_version_specific_handler(skvn)

        try:
            # return SkelClassVsn.get_skeleton_by_datastack_and_rid(
            return SkelClassVsn.get_skeleton_by_datastack_and_rid_async(
                datastack_name=datastack_name,
                rid=rid,
                output_format=output_format,
                bucket=current_app.config["SKELETON_CACHE_BUCKET"],
                root_resolution=[1, 1, 1],
                collapse_soma=True,
                collapse_radius=7500,
                skeleton_version=skvn,
                verbose_level_=0,
            )
        except ValueError as e:
            return {"Error": str(e)}, 400
        except Exception as e:
            return {"Error": str(e)}, 500

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, skvn: int, rid: int, output_format: str):
        """Get skeleton by rid"""

        if output_format == 'none':
            return SkeletonResource__gen_skeletons_via_msg_B.process(datastack_name, skvn, rid)

        return self.process(datastack_name, skvn, rid, output_format)


# TODO: These two functions are used for the "none" output in functions further below.
# I'm not sure they are really necessary, and even if they are, I'm not sure they need to be maintained as
# Flask endpoints. I don't believe they are used in a Flask fashion anywhere.
# @api_bp.route("/<string:datastack_name>/precomputed_via_msg/skeleton/<int:rid>")
class SkeletonResource__gen_skeletons_via_msg_A(Resource):
    """SkeletonResource"""

    # @auth_required
    # @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    # @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, rid: int):
        """Get skeleton by rid"""
        logging.warning("The endpoint for generating skeletons without specifying a skeleton version will be deprecated in the future. Please specify a skeleton version.")
        
        return SkeletonResource__gen_skeletons_via_msg_B.process(datastack_name, 0, rid)



# @api_bp.route("/<string:datastack_name>/precomputed_via_msg/skeleton/<int(signed=True):skvn>/<int:rid>")
class SkeletonResource__gen_skeletons_via_msg_B(Resource):
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
        exchange = os.getenv("SKELETON_CACHE_HIGH_PRIORITY_EXCHANGE", "skeleton")
        print(f"SkeletonService sending payload for rid {rid} to exchange {exchange}")
        c.publish(exchange, payload, attributes)

        print(f"Message has been dispatched to {exchange}: {datastack_name} {rid} skvn:{skvn} {current_app.config['SKELETON_CACHE_BUCKET']}")
        return f"Message has been dispatched to {exchange}: {datastack_name} {rid} skvn:{skvn} {current_app.config['SKELETON_CACHE_BUCKET']}"

    # @auth_required
    # @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    # @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, skvn: int, rid: int):
        """Get skeleton by rid"""
        return self.process(datastack_name, skvn, rid)



@api_bp.route("/<string:datastack_name>/bulk/get_skeletons/<string:output_format>/<bool:gms>/<string:rids>")
class SkeletonResource__get_skeletons_bulk_A(Resource):
    """SkeletonResource"""

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, output_format: str, gms: bool, rids: str):
        return SkeletonResource__get_skeletons_bulk_B.process(datastack_name, 0, output_format, gms, rids)



@api_bp.route("/<string:datastack_name>/bulk/get_skeletons/<int(signed=True):skvn>/<string:output_format>/<bool:gms>/<string:rids>")
class SkeletonResource__get_skeletons_bulk_B(Resource):
    """SkeletonResource"""

    @staticmethod
    def process(datastack_name: str, skvn: int, output_format: str, gms: bool, rids: str):
        SkelClassVsn = SkeletonService.get_version_specific_handler(skvn)

        return SkelClassVsn.get_skeletons_bulk_by_datastack_and_rids(
            datastack_name,
            rids=list(map(int, rids.split(','))),
            bucket=current_app.config["SKELETON_CACHE_BUCKET"],
            root_resolution=[1, 1, 1],
            collapse_soma=True,
            collapse_radius=7500,
            skeleton_version=skvn,
            output_format=output_format,
            generate_missing_skeletons=gms,
            verbose_level_=0,
        )

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, skvn: int, output_format: str, gms: bool, rids: str):
        return self.process(datastack_name, skvn, output_format, gms, rids)


@api_bp.route("/<string:datastack_name>/async/get_skeleton/<int:rid>")
class SkeletonResource__get_skeleton_async_A(Resource):
    """SkeletonResource"""

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, rid: int):
        """Get skeleton by rid"""
        logging.warning("The endpoint for getting skeletons without specifying a skeleton version will be deprecated in the future. Please specify a skeleton version.")
        
        # Use the NeuroGlancer compatible version
        return SkeletonResource__get_skeleton_async_B.process(datastack_name, NEUROGLANCER_SKELETON_VERSION, rid)



@api_bp.route("/<string:datastack_name>/async/get_skeleton/<int(signed=True):skvn>/<int:rid>")
class SkeletonResource__get_skeleton_async_B(Resource):
    """SkeletonResource"""

    @staticmethod
    def process(datastack_name: str, skvn: int, rid: int):
        return SkeletonResource__get_skeleton_async_C.process(datastack_name, skvn, rid, 'precomputed')

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, skvn: int, rid: int):
        """Get skeleton by rid"""
        return SkeletonResource__get_skeleton_async_C.process(datastack_name, skvn, rid, 'precomputed')



@api_bp.route("/<string:datastack_name>/async/get_skeleton/<int(signed=True):skvn>/<int:rid>/<string:output_format>")
class SkeletonResource__get_skeleton_async_C(Resource):
    """SkeletonResource"""

    @staticmethod
    def process(datastack_name: str, skvn: int, rid: int, output_format: str):
        SkelClassVsn = SkeletonService.get_version_specific_handler(skvn)

        try:
            return SkelClassVsn.get_skeleton_by_datastack_and_rid_async(
                datastack_name=datastack_name,
                rid=rid,
                output_format=output_format,
                bucket=current_app.config["SKELETON_CACHE_BUCKET"],
                root_resolution=[1, 1, 1],
                collapse_soma=True,
                collapse_radius=7500,
                skeleton_version=skvn,
                verbose_level_=0,
            )
        except ValueError as e:
            return {"Error": str(e)}, 400
        except Exception as e:
            return {"Error": str(e)}, 500

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, skvn: int, rid: int, output_format: str):
        """Get skeleton by rid"""

        if output_format == 'none':
            return SkeletonResource__gen_skeletons_via_msg_B.process(datastack_name, skvn, rid)

        return self.process(datastack_name, skvn, rid, output_format)



# NOTE: Use of this endpoint is only supported by CAVEclient:SkeletonService when applied to older versions of this server code,
# and likewise it can't be removed from here if there are any older clients in the wild that might access it.
@api_bp.route("/<string:datastack_name>/bulk/gen_skeletons/<string:rids>")
class SkeletonResource__gen_skeletons_bulk_async_A(Resource):
    """SkeletonResource"""

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, rids: str):
        return SkeletonResource__gen_skeletons_bulk_async_B.process(datastack_name, 0, rids)


# NOTE: Use of this endpoint is only supported by CAVEclient:SkeletonService when applied to older versions of this server code,
# and likewise it can't be removed from here if there are any older clients in the wild that might access it.
@api_bp.route("/<string:datastack_name>/bulk/gen_skeletons/<int(signed=True):skvn>/<string:rids>")
class SkeletonResource__gen_skeletons_bulk_async_B(Resource):
    """SkeletonResource"""

    @staticmethod
    def process(datastack_name: str, skvn: int, rids: str):
        SkelClassVsn = SkeletonService.get_version_specific_handler(skvn)

        return SkelClassVsn.generate_skeletons_bulk_by_datastack_and_rids_async(
            datastack_name,
            rids=list(map(int, rids.split(','))),
            bucket=current_app.config["SKELETON_CACHE_BUCKET"],
            root_resolution=[1, 1, 1],
            collapse_soma=True,
            collapse_radius=7500,
            skeleton_version=skvn,
            verbose_level_=0,
        )

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, skvn: int, rids: str):
        return self.process(datastack_name, skvn, rids)



@api_bp.route("/<string:datastack_name>/bulk/gen_skeletons")
class SkeletonResource__gen_skeletons_bulk_async_C(Resource):
    """SkeletonResource"""

    @staticmethod
    def process(datastack_name: str, skvn: int, rids: List):
        SkelClassVsn = SkeletonService.get_version_specific_handler(skvn)

        return SkelClassVsn.generate_skeletons_bulk_by_datastack_and_rids_async(
            datastack_name,
            rids=rids,
            bucket=current_app.config["SKELETON_CACHE_BUCKET"],
            root_resolution=[1, 1, 1],
            collapse_soma=True,
            collapse_radius=7500,
            skeleton_version=skvn,
            verbose_level_=0,
        )

    @api_bp.expect(bulk_async_parser)
    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def post(self, datastack_name: str):
        # I don't understand why other examples (AnnotationEngine, MaterializationEngine)
        # use request.parsed_obj to retrieve post data,
        # which doesn't exist in this case, instead of request.json, which does exist.
        
        # data = request.parsed_obj  # Doesn't work, doesn't exist
        
        data = request.json
        rids = data['root_ids']
        skvn = data['skeleton_version']
        response = self.process(datastack_name, skvn, rids)
        return response, 200
