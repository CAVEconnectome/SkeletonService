import copy
import logging
import os
# Import flask dependencies
from flask import jsonify, render_template, current_app, request, make_response, Blueprint
from flask_accepts import accepts, responds
from flask_restx import Namespace, Resource, reqparse
from werkzeug.routing import BaseConverter
from meshparty import skeleton
from skeletonservice.datasets import schemas
from skeletonservice.datasets.limiter import limit_by_category
from skeletonservice.datasets.service import NEUROGLANCER_SKELETON_VERSION, SKELETON_DEFAULT_VERSION_PARAMS, SKELETON_VERSION_PARAMS, SkeletonService

from middle_auth_client import (
    auth_required,
    auth_requires_permission,
    user_has_permission,
)

from typing import List, Dict

__version__ = "0.19.0"


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
            raise ValueError(f"Invalid skeleton version: v{skvn}. Valid versions: {SKELETON_DEFAULT_VERSION_PARAMS + list(SKELETON_VERSION_PARAMS.keys())}")
        if skvn == 2:
            # TODO: This hard-coded duplicated code has solved a problem in which the vertex_attributes dictionary received
            # by the client would mysteriously accumulate repetitions of the compartment attribute. copy.deepcopy() didn't
            # work, and I don't know why, so I resorted to this hard-coded solution. The underlying cause needs to be found and solved.
            return {'@type': 'neuroglancer_skeletons',
                'transform': [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0],
                'vertex_attributes': [{
                    # TODO: Due to a Neuroglancer limitation, the compartment must be encoded as a float.
                    # Note that this limitation is also encoded in service.py where skel.vertex_properties['compartment'] is assigned.
                    'id': 'radius',
                    'data_type': 'float32',
                    'num_components': 1,
                },{
                    'id': 'compartment',
                    'data_type': 'float32',
                    'num_components': 1,
                }]}
        return copy.deepcopy(SKELETON_VERSION_PARAMS[skvn])

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
    method_decorators = [
        limit_by_category("request"),
        auth_requires_permission("view", table_arg="datastack_name"),
    ]

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, root_id_prefixes: str, limit: int):
        """
        Get skeletons in cache by root_id_prefix
        
        root_id_prefixes could be a single int (as a string), a single string (i.e. one int as a string), or a comma-separated list of strings (i.e. multiple ints as a single string).
        """
        logging.warning("The endpoint for getting skeletons without specifying a skeleton version will be deprecated in the future. Please specify a skeleton version.")
        
        # Use the NeuroGlancer compatible version
        verbose_level = int(request.args.get('verbose_level')) if 'verbose_level' in request.args else 0
        return SkeletonResource__query_cache_B.process(datastack_name, NEUROGLANCER_SKELETON_VERSION, root_id_prefixes, limit, verbose_level)


@api_bp.route("/<string:datastack_name>/precomputed/skeleton/query_cache/<int(signed=True):skvn>/<string:root_id_prefixes>/<int:limit>")
class SkeletonResource__query_cache_B(Resource):
    """SkeletonResource"""
    method_decorators = [
        limit_by_category("request"),
        auth_requires_permission("view", table_arg="datastack_name"),
    ]

    @staticmethod
    def process(datastack_name: str, skvn: int, root_id_prefixes: str, limit: int, verbose_level: int=0):
        SkelClassVsn = SkeletonService.get_version_specific_handler(skvn)
        
        return SkelClassVsn.get_cache_contents(
            bucket=current_app.config["SKELETON_CACHE_BUCKET"],
            datastack_name=datastack_name,
            skeleton_version=skvn,
            rid_prefixes=[int(v) for v in root_id_prefixes.split(',')],
            limit=limit,
            session_timestamp_=SkeletonService.get_session_timestamp(),
            verbose_level_=verbose_level,
        )

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, skvn: int, root_id_prefixes: str, limit: int):
        """
        Get skeletons in cache by root_id_prefix
        
        root_id_prefixes could be a single int (as a string), a single string (i.e. one int as a string), or a comma-separated list of strings (i.e. multiple ints as a single string).
        """
        verbose_level = int(request.args.get('verbose_level')) if 'verbose_level' in request.args else 0
        return self.process(datastack_name, skvn, root_id_prefixes, limit, verbose_level)


# NOTE: Use of this endpoint has been removed from CAVEclient:SkeletonService, but it can't be removed from here if there are any older clients in the wild that might access it.
@api_bp.route("/<string:datastack_name>/precomputed/skeleton/exists/<string:root_ids>")
class SkeletonResource__skeleton_exists_A(Resource):
    """SkeletonResource"""
    method_decorators = [
        limit_by_category("request"),
        auth_requires_permission("view", table_arg="datastack_name"),
    ]

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
        verbose_level = int(request.args.get('verbose_level')) if 'verbose_level' in request.args else 0
        return SkeletonResource__skeleton_exists_B.process(datastack_name, NEUROGLANCER_SKELETON_VERSION, root_ids, verbose_level)


# NOTE: Use of this endpoint has been removed from CAVEclient:SkeletonService, but it can't be removed from here if there are any older clients in the wild that might access it.
@api_bp.route("/<string:datastack_name>/precomputed/skeleton/exists/<int(signed=True):skvn>/<string:root_ids>")
class SkeletonResource__skeleton_exists_B(Resource):
    """SkeletonResource"""
    method_decorators = [
        limit_by_category("request"),
        auth_requires_permission("view", table_arg="datastack_name"),
    ]

    @staticmethod
    def process(datastack_name: str, skvn: int, root_ids: str, verbose_level=0):
        return SkeletonResource__skeleton_exists_C.process(datastack_name, skvn, root_ids, verbose_level)

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, skvn: int, root_ids: str):
        """
        Determine whether skeletons exist in the cache for a set of root ids
        
        root_ids could be a single int (as a string), a single string (i.e. one int as a string), or a comma-separated list of strings (i.e. multiple ints as a single string).
        """
        verbose_level = int(request.args.get('verbose_level')) if 'verbose_level' in request.args else 0
        return self.process(datastack_name, skvn, root_ids, verbose_level)


@api_bp.route("/<string:datastack_name>/precomputed/skeleton/exists")
class SkeletonResource__skeleton_exists_C(Resource):
    """SkeletonResource"""
    method_decorators = [
        limit_by_category("request"),
        auth_requires_permission("view", table_arg="datastack_name"),
    ]

    @staticmethod
    def process(datastack_name: str, skvn: int, root_ids: List, verbose_level: int=0):
        SkelClassVsn = SkeletonService.get_version_specific_handler(skvn)

        if len(root_ids) == 1:
            # If requesting a single root_id, then return the single root_id as an int, not as a list of one int
            root_ids = int(root_ids[0])
        
        return SkelClassVsn.skeletons_exist(
            bucket=current_app.config["SKELETON_CACHE_BUCKET"],
            datastack_name=datastack_name,
            skeleton_version=skvn,
            rids=root_ids,
            session_timestamp_=SkeletonService.get_session_timestamp(),
            verbose_level_=verbose_level,
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
        verbose_level = data['verbose_level'] if 'verbose_level' in data else 0
        verbose_level = max(int(request.args.get('verbose_level')), verbose_level) if 'verbose_level' in request.args else verbose_level

        response = self.process(datastack_name, skvn, rids, verbose_level)
        return response, 200


@api_bp.route("/<string:datastack_name>/precomputed/meshwork/<int:rid>")
class SkeletonResource__get_meshwork_A(Resource):
    """SkeletonResource"""

    @staticmethod
    def process(datastack_name: str, rid: int, verbose_level: int=0):
        try:
            skvn = -1
            SkelClassVsn = SkeletonService.get_version_specific_handler(skvn)
            
            return SkelClassVsn.get_meshwork_by_datastack_and_rid_async(
                datastack_name=datastack_name,
                rid=rid,
                bucket=current_app.config["SKELETON_CACHE_BUCKET"],
                root_resolution=[1, 1, 1],
                collapse_soma=True,
                collapse_radius=7500,
                session_timestamp_=SkeletonService.get_session_timestamp(),
                verbose_level_=verbose_level,
            )
        except ValueError as e:
            return {"Error": str(e)}, 400
        except Exception as e:
            return {"Error": str(e)}, 500

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, rid: int):
        """Get meshwork by rid"""
        verbose_level = int(request.args.get('verbose_level')) if 'verbose_level' in request.args else 0
        return self.process(datastack_name, rid, verbose_level)


@api_bp.route("/<string:datastack_name>/precomputed/skeleton/<int:rid>")
class SkeletonResource__get_skeleton_A(Resource):
    """SkeletonResource"""
    method_decorators = [
        limit_by_category("request"),
        auth_requires_permission("view", table_arg="datastack_name"),
    ]

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, rid: int):
        """Get skeleton by rid"""
        logging.warning("The endpoint for getting skeletons without specifying a skeleton version will be deprecated in the future. Please specify a skeleton version.")
        
        # Use the NeuroGlancer compatible version
        verbose_level = int(request.args.get('verbose_level')) if 'verbose_level' in request.args else 0
        return SkeletonResource__get_skeleton_B.process(datastack_name, NEUROGLANCER_SKELETON_VERSION, rid, verbose_level)


# NOTE: Use of this endpoint has been removed from CAVEclient:SkeletonService, but it can't be removed from here if there are any older clients in the wild that might access it.
@api_bp.route("/<string:datastack_name>/precomputed/skeleton/<int(signed=True):skvn>/<int:rid>")
class SkeletonResource__get_skeleton_B(Resource):
    """SkeletonResource"""
    method_decorators = [
        limit_by_category("request"),
        auth_requires_permission("view", table_arg="datastack_name"),
    ]

    @staticmethod
    def process(datastack_name: str, skvn: int, rid: int, verbose_level: int=0):
        return SkeletonResource__get_skeleton_C.process(datastack_name, skvn, rid, 'precomputed', verbose_level)

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, skvn: int, rid: int):
        """Get skeleton by rid"""
        verbose_level = int(request.args.get('verbose_level')) if 'verbose_level' in request.args else 0
        return SkeletonResource__get_skeleton_C.process(datastack_name, skvn, rid, 'precomputed', verbose_level)


@api_bp.route("/<string:datastack_name>/precomputed/skeleton/<int(signed=True):skvn>/<int:rid>/<string:output_format>")
class SkeletonResource__get_skeleton_C(Resource):
    """SkeletonResource"""
    method_decorators = [
        limit_by_category("request"),
        auth_requires_permission("view", table_arg="datastack_name"),
    ]

    @staticmethod
    def process(datastack_name: str, skvn: int, rid: int, output_format: str, verbose_level: int=0):
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
                session_timestamp_=SkeletonService.get_session_timestamp(),
                verbose_level_=verbose_level,
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

        verbose_level = int(request.args.get('verbose_level')) if 'verbose_level' in request.args else 0

        if output_format == 'none':
            return SkeletonResource__gen_skeletons_via_msg_B.process(datastack_name, skvn, rid, verbose_level)

        return self.process(datastack_name, skvn, rid, output_format, verbose_level)


# These two functions are used for the "none" output in functions further below.
# I've commented out the first version because I don't believe it is used, but I'm unsure about older versions of the code so I've left it in place for clarity.

# class SkeletonResource__gen_skeletons_via_msg_A(Resource):
#     """SkeletonResource"""

#     # @auth_required
#     # @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
#     # @api_bp.doc("SkeletonResource", security="apikey")
#     def get(self, datastack_name: str, rid: int):
#         """Get skeleton by rid"""
#         logging.warning("The endpoint for generating skeletons without specifying a skeleton version will be deprecated in the future. Please specify a skeleton version.")
        
#         verbose_level = int(request.args.get('verbose_level')) if 'verbose_level' in request.args else 0
#         return SkeletonResource__gen_skeletons_via_msg_B.process(datastack_name, 0, rid, verbose_level)


class SkeletonResource__gen_skeletons_via_msg_B(Resource):
    """SkeletonResource"""

    @staticmethod
    def process(datastack_name: str, skvn: int, rid: int, verbose_level: int=0):
        SkeletonService.publish_skeleton_request(
            datastack_name,
            rid,
            "none",
            current_app.config["SKELETON_CACHE_BUCKET"],
            [1, 1, 1],
            True,
            7500,
            skvn,
            True,
            verbose_level,
        )

        # from messagingclient import MessagingClient

        # payload = b""
        # attributes = {
        #     "skeleton_params_rid": f"{rid}",
        #     "skeleton_params_output_format": "none",
        #     "skeleton_params_bucket": current_app.config["SKELETON_CACHE_BUCKET"],
        #     "skeleton_params_datastack_name": datastack_name,
        #     "skeleton_params_root_resolution": "1 1 1",
        #     "skeleton_params_collapse_soma": "True",
        #     "skeleton_params_collapse_radius": "7500",
        #     "skeleton_version": f"{skvn}",
        #     "verbose_level": str(verbose_level),
        # }

        # c = MessagingClient()
        exchange = os.getenv("SKELETON_CACHE_HIGH_PRIORITY_EXCHANGE", "skeleton")
        # # print(f"SkeletonService sending payload for rid {rid} to exchange {exchange}")
        # c.publish(exchange, payload, attributes)

        # print(f"Message has been dispatched to {exchange}: {datastack_name} {rid} skvn:{skvn} {current_app.config['SKELETON_CACHE_BUCKET']}")
        return f"Message has been dispatched to {exchange}: {datastack_name} {rid} skvn:{skvn} {current_app.config['SKELETON_CACHE_BUCKET']}"

    # @auth_required
    # @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    # @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, skvn: int, rid: int):
        """Get skeleton by rid"""
        verbose_level = int(request.args.get('verbose_level')) if 'verbose_level' in request.args else 0
        return self.process(datastack_name, skvn, rid, verbose_level)


# I don't believe the following route is used by the CAVEclient, but I'm unsure if a past version might have used it, so it should be left in place.
@api_bp.route("/<string:datastack_name>/bulk/get_skeletons/<string:output_format>/<bool:gms>/<string:rids>")
class SkeletonResource__get_skeletons_bulk_A(Resource):
    """SkeletonResource"""
    method_decorators = [
        limit_by_category("request"),
        auth_requires_permission("view", table_arg="datastack_name"),
    ]

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, output_format: str, gms: bool, rids: str):
        verbose_level = int(request.args.get('verbose_level')) if 'verbose_level' in request.args else 0
        return SkeletonResource__get_skeletons_bulk_B.process(datastack_name, 0, output_format, gms, rids, verbose_level)


@api_bp.route("/<string:datastack_name>/bulk/get_skeletons/<int(signed=True):skvn>/<string:output_format>/<bool:gms>/<string:rids>")
class SkeletonResource__get_skeletons_bulk_B(Resource):
    """SkeletonResource"""
    method_decorators = [
        limit_by_category("request"),
        auth_requires_permission("view", table_arg="datastack_name"),
    ]

    @staticmethod
    def process(datastack_name: str, skvn: int, output_format: str, gms: bool, rids: str, verbose_level: int=0):
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
            session_timestamp_=SkeletonService.get_session_timestamp(),
            verbose_level_=verbose_level,
        )

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, skvn: int, output_format: str, gms: bool, rids: str):
        verbose_level = int(request.args.get('verbose_level')) if 'verbose_level' in request.args else 0
        return self.process(datastack_name, skvn, output_format, gms, rids, verbose_level)


# I'm unsure if a past version of CAVEclient used this, so it should be left in place. It hasn't been used in recent versions however.
@api_bp.route("/<string:datastack_name>/async/get_skeleton/<int:rid>")
class SkeletonResource__get_skeleton_async_A(Resource):
    """SkeletonResource"""
    method_decorators = [
        limit_by_category("request"),
        auth_requires_permission("view", table_arg="datastack_name"),
    ]

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, rid: int):
        """Get skeleton by rid"""
        logging.warning("The endpoint for getting skeletons without specifying a skeleton version will be deprecated in the future. Please specify a skeleton version.")
        
        # Use the NeuroGlancer compatible version
        verbose_level = int(request.args.get('verbose_level')) if 'verbose_level' in request.args else 0
        return SkeletonResource__get_skeleton_async_B.process(datastack_name, NEUROGLANCER_SKELETON_VERSION, rid, verbose_level)


# I'm unsure if a past version of CAVEclient used this, so it should be left in place. It hasn't been used in recent versions however.
@api_bp.route("/<string:datastack_name>/async/get_skeleton/<int(signed=True):skvn>/<int:rid>")
class SkeletonResource__get_skeleton_async_B(Resource):
    """SkeletonResource"""
    method_decorators = [
        limit_by_category("request"),
        auth_requires_permission("view", table_arg="datastack_name"),
    ]

    @staticmethod
    def process(datastack_name: str, skvn: int, rid: int, verbose_level: int=0):
        return SkeletonResource__get_skeleton_async_C.process(datastack_name, skvn, rid, 'precomputed', verbose_level)

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, skvn: int, rid: int):
        """Get skeleton by rid"""
        verbose_level = int(request.args.get('verbose_level')) if 'verbose_level' in request.args else 0
        return self.process(datastack_name, skvn, rid, verbose_level)


@api_bp.route("/<string:datastack_name>/async/get_skeleton/<int(signed=True):skvn>/<int:rid>/<string:output_format>")
class SkeletonResource__get_skeleton_async_C(Resource):
    """SkeletonResource"""
    method_decorators = [
        limit_by_category("request"),
        auth_requires_permission("view", table_arg="datastack_name"),
    ]

    @staticmethod
    def process(datastack_name: str, skvn: int, rid: int, output_format: str, verbose_level: int=0):
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
                session_timestamp_=SkeletonService.get_session_timestamp(),
                verbose_level_=verbose_level,
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

        verbose_level = int(request.args.get('verbose_level')) if 'verbose_level' in request.args else 0

        if output_format == 'none':
            return SkeletonResource__gen_skeletons_via_msg_B.process(datastack_name, skvn, rid, verbose_level)

        return self.process(datastack_name, skvn, rid, output_format, verbose_level)


@api_bp.route("/<string:datastack_name>/bulk/gen_meshworks")
class SkeletonResource__gen_meshworks_bulk_async_A(Resource):
    """SkeletonResource"""

    @staticmethod
    def process(datastack_name: str, rids: List, verbose_level: int=0):
        skvn = -1
        SkelClassVsn = SkeletonService.get_version_specific_handler(skvn)

        response = SkelClassVsn.generate_meshworks_bulk_by_datastack_and_rids_async(
            datastack_name,
            rids=rids,
            bucket=current_app.config["SKELETON_CACHE_BUCKET"],
            root_resolution=[1, 1, 1],
            collapse_soma=True,
            collapse_radius=7500,
            session_timestamp_=SkeletonService.get_session_timestamp(),
            verbose_level_=verbose_level,  # DEBUG
        )
    
        return response

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
        verbose_level = data['verbose_level'] if 'verbose_level' in data else 0
        verbose_level = max(int(request.args.get('verbose_level')), verbose_level) if 'verbose_level' in request.args else verbose_level

        response = self.process(datastack_name, rids, verbose_level)
        return response, 200


# NOTE: Use of this endpoint is only supported by CAVEclient:SkeletonService when applied to older versions of this server code,
# and likewise it can't be removed from here if there are any older clients in the wild that might access it.
@api_bp.route("/<string:datastack_name>/bulk/gen_skeletons/<string:rids>")
class SkeletonResource__gen_skeletons_bulk_async_A(Resource):
    """SkeletonResource"""
    method_decorators = [
        limit_by_category("request"),
        auth_requires_permission("view", table_arg="datastack_name"),
    ]

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, rids: str):
        verbose_level = int(request.args.get('verbose_level')) if 'verbose_level' in request.args else 0
        return SkeletonResource__gen_skeletons_bulk_async_B.process(datastack_name, 0, rids, verbose_level)


# NOTE: Use of this endpoint is only supported by CAVEclient:SkeletonService when applied to older versions of this server code,
# and likewise it can't be removed from here if there are any older clients in the wild that might access it.
@api_bp.route("/<string:datastack_name>/bulk/gen_skeletons/<int(signed=True):skvn>/<string:rids>")
class SkeletonResource__gen_skeletons_bulk_async_B(Resource):
    """SkeletonResource"""
    method_decorators = [
        limit_by_category("request"),
        auth_requires_permission("view", table_arg="datastack_name"),
    ]

    @staticmethod
    def process(datastack_name: str, skvn: int, rids: str, verbose_level: int=0):
        return SkeletonResource__gen_skeletons_bulk_async_C.process(datastack_name, skvn, rids, verbose_level)

    @auth_required
    @auth_requires_permission("view", table_arg="datastack_name", resource_namespace="datastack")
    @api_bp.doc("SkeletonResource", security="apikey")
    def get(self, datastack_name: str, skvn: int, rids: str):
        verbose_level = int(request.args.get('verbose_level')) if 'verbose_level' in request.args else 0
        return self.process(datastack_name, skvn, rids, verbose_level)


@api_bp.route("/<string:datastack_name>/bulk/gen_skeletons")
class SkeletonResource__gen_skeletons_bulk_async_C(Resource):
    """SkeletonResource"""
    method_decorators = [
        limit_by_category("request"),
        auth_requires_permission("view", table_arg="datastack_name"),
    ]

    @staticmethod
    def process(datastack_name: str, skvn: int, rids: List, verbose_level: int=0):
        SkelClassVsn = SkeletonService.get_version_specific_handler(skvn)

        return SkelClassVsn.generate_skeletons_bulk_by_datastack_and_rids_async(
            datastack_name,
            rids=rids,
            bucket=current_app.config["SKELETON_CACHE_BUCKET"],
            root_resolution=[1, 1, 1],
            collapse_soma=True,
            collapse_radius=7500,
            skeleton_version=skvn,
            session_timestamp_=SkeletonService.get_session_timestamp(),
            verbose_level_=verbose_level,
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
        verbose_level = data['verbose_level'] if 'verbose_level' in data else 0
        verbose_level = max(int(request.args.get('verbose_level')), verbose_level) if 'verbose_level' in request.args else verbose_level

        response = self.process(datastack_name, skvn, rids, verbose_level)
        return response, 200
