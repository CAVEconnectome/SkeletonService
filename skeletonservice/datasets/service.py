from io import BytesIO
from typing import List, Dict
import os
import numpy as np
import pandas as pd
import json
import gzip
from flask import send_file, Response, request
from .skeleton_io_from_meshparty import SkeletonIO
from meshparty import skeleton
import caveclient
import pcg_skel
from cloudfiles import CloudFiles, compression
import cloudvolume
import gzip
import os

from skeletonservice.datasets.models import (
    Skeleton,
)
# from skeletonservice.datasets.schemas import (
#     SkeletonSchema,
# )

DEBUG_SKELETON_CACHE_LOC = "/Users/keith.wiley/Work/Code/SkeletonService/skeletons/"
# SKELETON_CACHE_LOC = "gs://keith-dev/"
COMPRESSION = 'gzip'  # Valid values mirror cloudfiles.CloudFiles.put() and put_json(): None, 'gzip', 'br' (brotli), 'zstd'

class SkeletonService:
    def minimize_json_skeleton_for_easier_debugging(skeleton_json):
        '''
        The web UI won't show large JSON content, so to assist debugging I'm just returning the smaller data (not lists, etc.)
        '''
        skeleton_json['branch_points'] = 0
        skeleton_json['branch_points_undirected'] = 0
        skeleton_json['distance_to_root'] = 0
        skeleton_json['edges'] = 0
        skeleton_json['end_points'] = 0
        skeleton_json['end_points_undirected'] = 0
        skeleton_json['hops_to_root'] = 0
        skeleton_json['indices_unmasked'] = 0
        skeleton_json['mesh_index'] = 0
        skeleton_json['mesh_to_skel_map'] = 0
        skeleton_json['mesh_to_skel_map_base'] = 0
        skeleton_json['meta'] = 0
        skeleton_json['node_mask'] = 0
        skeleton_json['segment_map'] = 0
        skeleton_json['topo_points'] = 0
        skeleton_json['vertices'] = 0
        return skeleton_json

    # Placeholder. I'm not sure there is a need for a "get all" or even a "get many" interaction regarding skeletons.
    @staticmethod
    def get_all() -> List[Skeleton]:
        return [{"name": "Skeleton #1"}]  # Skeleton.query.all()
    
    @staticmethod
    def retrieve_sid_for_rid(rid, datastack_name, materialize_version):
        '''
        Given a root id, find the nucleus id (aka soma id)
        '''
        client = caveclient.CAVEclient(datastack_name)
        client.materialize.version = materialize_version
        proof = client.materialize.query_table('proofreading_status_public_release')
        rid2 = proof[proof['valid_id']==rid].iloc[0]['pt_root_id']
        neurons = client.materialize.query_table('nucleus_ref_neuron_svm', desired_resolution=[1000, 1000, 1000])
        nid = neurons[neurons['pt_root_id']==rid2].iloc[0]['id_ref']  # target_id seems to be an equivalent column option here
        
        return nid
    
    @staticmethod
    def get_skeleton_filename(rid, bucket, datastack_name, materialize_version, root_resolution, collapse_soma, collapse_radius, format, include_compression=True):
        '''
        Build a filename for a skeleton file based on the parameters.
        The format and optional compression will be appended as extensions as necessary.
        '''
        file_name = f"skeleton__rid-{rid}__ds-{datastack_name}__mv-{materialize_version}__res-{root_resolution[0]}x{root_resolution[1]}x{root_resolution[2]}__cs-{collapse_soma}__cr-{collapse_radius}"
        
        assert format == 'json' or format == 'precomputed' or format == 'h5' or format == 'swc'
        file_name += f".{format}"
        
        if include_compression:
            if COMPRESSION == 'gzip':
                file_name += ".gz"
            elif COMPRESSION == 'br':
                file_name += ".br"
            elif COMPRESSION == 'zstd':
                file_name += ".zst"
        
        return file_name

    @staticmethod
    def get_skeleton_location(params, format):
        '''
        Build a location for a skeleton file based on the parameters and the cache location (likely a Google bucket).
        '''
        bucket = params[1]
        return f"{bucket}{SkeletonService.get_skeleton_filename(*params, format)}"

    @staticmethod
    def retrieve_skeleton_from_local(params, format):
        '''
        This is a debugging function that reads a skeleton from a local file instead of
        computing a skeleton from scratch or retrieving one from a Google bucket.
        '''
        file_name = SkeletonService.get_skeleton_filename(*params, 'h5', include_compression=False)
        if not os.path.exists(DEBUG_SKELETON_CACHE_LOC + file_name):
            return None
        
        skeleton = SkeletonIO.read_skeleton_h5(DEBUG_SKELETON_CACHE_LOC + file_name)

        if format == 'json':
            skeleton = SkeletonService.skeleton_to_json(skeleton)
        elif format == 'precomputed':
            cv_skeleton = cloudvolume.Skeleton(
                vertices=skeleton.vertices,
                edges=skeleton.edges, 
                radii=skeleton.radius,
                space='voxel',
                extra_attributes=[{
                    'id': 'radius',
                    'data_type': 'float32',
                    'num_components': 1
                }],
            )
            skeleton = cv_skeleton.to_precomputed()
        elif format == 'swc':
            file_name = SkeletonService.get_skeleton_filename(*params, format, False)
            SkeletonIO.export_to_swc(skeleton, file_name)
            file_content = open(file_name, 'rb').read()
            skeleton = file_content
        else:  # format == 'h5'
            # It's already in H5 format, so just return the bytes as-is.
            pass

        return skeleton

    @staticmethod
    def retrieve_skeleton_from_cache(params, format):
        '''
        If the requested format is JSON or PRECOMPUTED, then read the skeleton and return it as native content.
        But if the requested format is H5 or SWC, then return the location of the skeleton file.
        '''
        file_name = SkeletonService.get_skeleton_filename(*params, format)
        bucket = params[1]
        cf = CloudFiles(bucket)
        if cf.exists(file_name):
            if format == 'json':
                return cf.get_json(file_name)
            elif format == 'precomputed':
                return cf.get(file_name)
            else:  # format == 'h5' or 'swc'
                return SkeletonService.get_skeleton_location(params, format)
        return None

    @staticmethod
    def retrieve_h5_skeleton_from_cache(params):
        '''
        Fully read H5 skeleton as opposed to just returning the location of the file.
        See retrieve_skeleton_from_cache() for comparison.
        '''
        file_name = SkeletonService.get_skeleton_filename(*params, 'h5')
        bucket = params[1]
        cf = CloudFiles(bucket)
        if cf.exists(file_name):
            skeleton_bytes = cf.get(file_name)
            
            # Write the bytes to a file and then immediately read them back in to build a skeleton object.
            # There has GOT to be a more efficient way to do this! Some sort of BytesIO object or even a RAMDisk for heaven's sake.
            # file_name = file_name[:-3]
            # with open(file_name, 'wb') as f:
            #     f.write(skeleton_bytes)
            # return SkeletonIO.read_skeleton_h5(file_name)
            
            return SkeletonIO.read_skeleton_h5(skeleton_bytes)
        return None
    
    @staticmethod
    def cache_skeleton(params, skeleton, format):
        '''
        Cache the skeleton in the requested format to the indicated location (likely a Google bucket).
        '''
        file_name = SkeletonService.get_skeleton_filename(*params, format)
        bucket = params[1]
        cf = CloudFiles(bucket)
        if format == 'json':
            cf.put_json(file_name, skeleton, COMPRESSION)
        else:  # format == 'precomputed' or 'h5' or 'swc'
            cf.put(file_name, skeleton, compress=COMPRESSION)

    @staticmethod
    def generate_skeleton(rid, bucket, datastack_name, materialize_version, root_resolution, collapse_soma, collapse_radius):
        '''
        From https://caveconnectome.github.io/pcg_skel/tutorial/
        '''
        client = caveclient.CAVEclient(datastack_name)
        client.materialize.version = materialize_version # Ensure we will always use this data release
        
        # Get the location of the soma from nucleus detection:
        print(f"generate_skeleton {rid} {datastack_name} {materialize_version} {root_resolution} {collapse_soma} {collapse_radius}")
        print(f"CAVEClient version: {caveclient.__version__}")
        soma_df = client.materialize.views.nucleus_detection_lookup_v1(
            pt_root_id = rid
            ).query(
                desired_resolution = root_resolution
            )
        soma_location = soma_df['pt_position'].values[0]

        # Use the above parameters in the skeletonization:
        skel = pcg_skel.coord_space_skeleton(
            rid,
            client,
            root_point=soma_location,
            root_point_resolution=root_resolution,
            collapse_soma=collapse_soma,
            collapse_radius=collapse_radius,
        )

        return skel
    
    @staticmethod
    def skeleton_metadata_to_json(skeleton_metadata):
        '''
        Used by skeleton_to_json().
        '''
        return {
            'root_id': skeleton_metadata.root_id,
            'soma_pt_x': skeleton_metadata.soma_pt_x,
            'soma_pt_y': skeleton_metadata.soma_pt_y,
            'soma_pt_z': skeleton_metadata.soma_pt_z,
            'soma_radius': skeleton_metadata.soma_radius,
            'collapse_soma': skeleton_metadata.collapse_soma,
            'collapse_function': skeleton_metadata.collapse_function,
            'invalidation_d': skeleton_metadata.invalidation_d,
            'smooth_vertices': skeleton_metadata.smooth_vertices,
            'compute_radius': skeleton_metadata.compute_radius,
            'shape_function': skeleton_metadata.shape_function,
            'smooth_iterations': skeleton_metadata.smooth_iterations,
            'smooth_neighborhood': skeleton_metadata.smooth_neighborhood,
            'smooth_r': skeleton_metadata.smooth_r,
            'cc_vertex_thresh': skeleton_metadata.cc_vertex_thresh,
            'remove_zero_length_edges': skeleton_metadata.remove_zero_length_edges,
            'collapse_params': skeleton_metadata.collapse_params,
            'timestamp': skeleton_metadata.timestamp,
            'skeleton_type': skeleton_metadata.skeleton_type,
            'meta': {
                'datastack': skeleton_metadata.meta.datastack,
                'space': skeleton_metadata.meta.space,
            },
        }
    
    @staticmethod
    def skeleton_to_json(skel):
        '''
        Convert a skeleton object to a JSON object.
        '''
        sk_json = {
            'jsonification_version': '1.0',
        }
        if skel.branch_points is not None:
            sk_json['branch_points'] = skel.branch_points.tolist()
        if skel.branch_points_undirected is not None:
            sk_json['branch_points_undirected'] = skel.branch_points_undirected.tolist()
        if skel.distance_to_root is not None:
            sk_json['distance_to_root'] = skel.distance_to_root.tolist()
        if skel.edges is not None:
            sk_json['edges'] = skel.edges.tolist()
        if skel.end_points is not None:
            sk_json['end_points'] = skel.end_points.tolist()
        if skel.end_points_undirected is not None:
            sk_json['end_points_undirected'] = skel.end_points_undirected.tolist()
        if skel.hops_to_root is not None:
            sk_json['hops_to_root'] = skel.hops_to_root.tolist()
        if skel.indices_unmasked is not None:
            sk_json['indices_unmasked'] = skel.indices_unmasked.tolist()
        if skel.mesh_index is not None:
            sk_json['mesh_index'] = skel.mesh_index.tolist()
        if skel.mesh_to_skel_map is not None:
            sk_json['mesh_to_skel_map'] = skel.mesh_to_skel_map.tolist()
        if skel.mesh_to_skel_map_base is not None:
            sk_json['mesh_to_skel_map_base'] = skel.mesh_to_skel_map_base.tolist()
        if skel.meta is not None:
            sk_json['meta'] = SkeletonService.skeleton_metadata_to_json(skel.meta)
        if skel.node_mask is not None:
            sk_json['node_mask'] = skel.node_mask.tolist()
        if skel.radius is not None:
            sk_json['radius'] = skel.radius
        if skel.root is not None:
            sk_json['root'] = skel.root.tolist()
        if skel.root_position is not None:
            sk_json['root_position'] = skel.root_position.tolist()
        if skel.segment_map is not None:
            sk_json['segment_map'] = skel.segment_map.tolist()
        if skel.topo_points is not None:
            sk_json['topo_points'] = skel.topo_points.tolist()
        if skel.unmasked_size is not None:
            sk_json['unmasked_size'] = skel.unmasked_size
        if skel.vertex_properties is not None:
            sk_json['vertex_properties'] = skel.vertex_properties
        if skel.vertices is not None:
            sk_json['vertices'] = skel.vertices.tolist()
        if skel.voxel_scaling is not None:
            sk_json['voxel_scaling'] = skel.voxel_scaling
        return sk_json
    
    @staticmethod
    def json_to_skeleton(json):
        '''
        Convert a JSON description of a skeleton to a skeleton object.
        Most of the skeleton fields can't be populated because they are mere properties, not true member variables, lacking associated setter functions.
        '''
        sk = skeleton.Skeleton(vertices=np.array(json['vertices']),
                               edges=np.array(json['edges']),
                               mesh_to_skel_map=np.array(json['mesh_to_skel_map']),
                               mesh_index=np.array(json['mesh_index']),
                               vertex_properties=json['vertex_properties'],
                               node_mask=np.array(json['node_mask']),
                               voxel_scaling=json['voxel_scaling'],
                            #    meta=skeleton.SkeletonMetadata(json),
                               )
        # sk.branch_points = json['branch_points']
        # sk.branch_points_undirected = json['branch_points_undirected']
        # sk.distance_to_root = json['distance_to_root']
        # sk.end_points = json['end_points']
        # sk.end_points_undirected = json['end_points_undirected']
        # sk.hops_to_root = json['hops_to_root']
        # sk.indices_unmasked = json['indices_unmasked']
        # sk.mesh_to_skel_map_base = json['mesh_to_skel_map_base']
        # sk.n_branch_points = json['n_branch_points']
        # sk.n_end_points = json['n_end_points']
        # sk.n_vertices = json['n_vertices']
        # sk.radius = json['radius']
        # sk.root = json['root']
        # sk.root_position = json['root_position']
        # sk.segment_map = json['segment_map']
        # sk.topo_points = json['topo_points']
        # sk.unmasked_size = json['unmasked_size']
        return sk

    @staticmethod
    def response_headers():
        '''
        Build Flask Response header for a requested skeleton object.
        '''
        return {
            'access-control-allow-credentials': 'true',
            'access-control-expose-headers': 'Cache-Control, Content-Disposition, Content-Encoding, Content-Length, Content-Type, Date, ETag, Server, Vary, X-Content-Type-Options, X-Frame-Options, X-Powered-By, X-XSS-Protection',
            'content-disposition': 'attachment',
        }

    @staticmethod
    def after_request(response):
        '''
        Optionally gzip a response. Alternatively, return the response unaltered.
        Copied verbatim from materializationengine.blueprints.client.utils.py
        '''
        accept_encoding = request.headers.get("Accept-Encoding", "")

        if "gzip" not in accept_encoding.lower():
            return response

        response.direct_passthrough = False

        if (
            response.status_code < 200
            or response.status_code >= 300
            or "Content-Encoding" in response.headers
        ):
            return response

        response.data = compression.gzip_compress(response.data)

        response.headers["Content-Encoding"] = "gzip"
        response.headers["Vary"] = "Accept-Encoding"
        response.headers["Content-Length"] = len(response.data)

        return response

    @staticmethod
    def get_skeleton_by_datastack_and_rid(datastack_name: str, rid: int, materialize_version: int, output_format: str, sid: int, bucket: str,
                                root_resolution: List, collapse_soma: bool, collapse_radius: int):
        '''
        Get a skeleton by root id (with optional associated soma id).
        If the requested format already exists in the cache, then return it.
        If not, then generate the skeleton from its cached H5 format and return it.
        If the H5 format also doesn't exist yet, then generate and cache the H5 version before generating and returning the requested format.
        '''
        # DEBUG
        if datastack_name == "0" or rid == 0:  # Flags indicating that a default hard-coded datastack_name and rid should be used for dev and debugging
            # From https://caveconnectome.github.io/pcg_skel/tutorial/
            rid = 864691135397503777
            datastack_name = 'minnie65_public'
            materialize_version = 795
        if materialize_version == 0:
            materialize_version = 795
        
        # print(f"get_skeleton_by_rid_sid() rid: {rid}, sid: {sid}, datastack_name: {datastack_name}, materialize_version: {materialize_version},",
        #       f" root_resolution: {root_resolution}, collapse_soma: {collapse_soma}, collapse_radius: {collapse_radius}, output_format: {output_format}")
        
        # print(f"Bucket: {bucket}")
        params = [rid, bucket, datastack_name, materialize_version, root_resolution, collapse_soma, collapse_radius]

        skeleton_return = SkeletonService.retrieve_skeleton_from_cache(params, output_format)
        # print(f"Cache query result: {skeleton_return}")

        # See minimize_json_skeleton_for_easier_debugging() for explanation.
        debug_minimize_json_skeleton = False  # DEBUG

        # if os.path.exists(DEBUG_SKELETON_CACHE_LOC):
        #     skeleton_return = SkeletonService.retrieve_skeleton_from_local(params, output_format)

        if skeleton_return:
            # skeleton_return will be JSON or PRECOMPUTED content, or H5 or SWC file location
            if output_format == 'precomputed':
                response = Response(skeleton_return, mimetype='application/octet-stream')
                response.headers.update(SkeletonService.response_headers())
                return SkeletonService.after_request(response)
            if output_format == 'json' and debug_minimize_json_skeleton:  # DEBUG
                skeleton_return = SkeletonService.minimize_json_skeleton_for_easier_debugging(skeleton_return)
            return skeleton_return 
        else:
            # If the requested format was JSON or SWC or PRECOMPUTED (and getting to this point implies no file already exists),
            # check for an H5 version before generating a new skeleton and use it to build a skeleton object if found.
            skeleton = None
            if output_format == 'json' or output_format == 'swc' or output_format == 'precomputed':
                skeleton = SkeletonService.retrieve_h5_skeleton_from_cache(params)
            # print(f"H5 cache query result: {skeleton}")

            # If no H5 skeleton was found, generate a new skeleton.
            # Note that the skeleton for any given set of parameters will only ever be generated once, regardless of the multiple formats offered.
            # H5 will be used to generate all the other formats as needed.
            save_h5 = False
            if not skeleton:
                try:
                    # First attempt a debugging retrieval to bypass computing a skeleton from scratch.
                    # On a nonlocal deployment that will simply fail and the skeleton will be generated as normal.
                    skeleton = SkeletonService.retrieve_skeleton_from_local(params, 'h5')
                    if not skeleton:
                        skeleton = SkeletonService.generate_skeleton(*params)
                except Exception as e:
                    print(e)
                    return f"Failed to generate skeleton for {rid}: {str(e)}"
                save_h5 = True

            # Cache the skeleton in the requested format and return the content (JSON or PRECOMPUTED) or location (H5 or SWC).
            # Also cache the H5 skeleton if it was generated.

            if output_format == 'h5' or save_h5:
                if os.path.exists(DEBUG_SKELETON_CACHE_LOC):
                    # Save the skeleton to a local file to faciliate rapid debugging (no need to regenerate the skeleton again).
                    file_name = SkeletonService.get_skeleton_filename(*params, 'h5', False)
                    SkeletonIO.write_skeleton_h5(skeleton, DEBUG_SKELETON_CACHE_LOC + file_name)
                
                file_content = BytesIO()
                SkeletonIO.write_skeleton_h5(skeleton, file_content)
                file_content = file_content.read()
                SkeletonService.cache_skeleton(params, file_content, 'h5')
                if output_format == 'h5':
                    file_location = SkeletonService.get_skeleton_location(params, output_format)
                    return file_location
            
            if output_format == 'swc':
                file_content = BytesIO()
                SkeletonIO.export_to_swc(skeleton, file_content)
                # file_name = SkeletonService.get_skeleton_filename(*params, output_format, False)
                # SkeletonIO.export_to_swc(skeleton, file_name)
                # Read the file back as a bytes object to facilitate CloudFiles.put()
                # file_content = open(file_name, 'rb').read()
                SkeletonService.cache_skeleton(params, file_content, output_format)
                file_location = SkeletonService.get_skeleton_location(params, output_format)
                return file_location
            
            if output_format == 'json':
                skeleton_json = SkeletonService.skeleton_to_json(skeleton)
                SkeletonService.cache_skeleton(params, skeleton_json, output_format)
                if debug_minimize_json_skeleton:  # DEBUG
                    skeleton_json = SkeletonService.minimize_json_skeleton_for_easier_debugging(skeleton_json)
                return skeleton_json
            
            if output_format == 'precomputed':
                cv_skeleton = cloudvolume.Skeleton(
                    vertices=skeleton.vertices,
                    edges=skeleton.edges, 
                    radii=skeleton.radius,
                    space='voxel',
                    extra_attributes=[{
                        'id': 'radius',
                        'data_type': 'float32',
                        'num_components': 1
                    }],
                )
                skeleton_precomputed = cv_skeleton.to_precomputed()
                SkeletonService.cache_skeleton(params, skeleton_precomputed, output_format)
                response = Response(skeleton_precomputed, mimetype='application/octet-stream')
                response.headers.update(SkeletonService.response_headers())
                return SkeletonService.after_request(response)
