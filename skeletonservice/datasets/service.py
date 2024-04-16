from typing import List, Dict
import os
import numpy as np
import pandas as pd
import json
import gzip
from meshparty import skeleton, skeleton_io
import skeleton_plot.skel_io as skel_io
from skeleton_plot import utils
import caveclient
import pcg_skel
from cloudfiles import CloudFiles
import gzip
import os

from skeletonservice.datasets.models import (
    Skeleton,
)
from skeletonservice.datasets.schemas import (
    SkeletonSchema,
)

# SKELETON_CACHE_LOC = "file:///Users/keith.wiley/Work/Code/SkeletonService/skeletons/"
SKELETON_CACHE_LOC = "gs://keith-dev/"
COMPRESSION = 'gzip'  # Valid values mirror cloudfiles.CloudFiles.put() and put_json(): None, 'gzip', 'br' (brotli), 'zstd'

class SkeletonService:
    @staticmethod
    def get_all() -> List[Skeleton]:
        return [{"name": "Skeleton #1"}]  # Skeleton.query.all()
    
    @staticmethod
    def retrieve_sid_for_rid(rid, datastack,materialize_version ):
        '''
        Given a root id, find the nucleus id (aka soma id)
        '''
        client = caveclient.CAVEclient(datastack)
        client.materialize.version = materialize_version
        proof = client.materialize.query_table('proofreading_status_public_release')
        rid2 = proof[proof['valid_id']==rid].iloc[0]['pt_root_id']
        neurons = client.materialize.query_table('nucleus_ref_neuron_svm', desired_resolution=[1000, 1000, 1000])
        nid = neurons[neurons['pt_root_id']==rid2].iloc[0]['id_ref']  # target_id seems to be an equivalent column option here
        
        return nid
    @staticmethod
    def get_skeleton_filename(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, format, include_compression=True):
        file_name = f"skeleton__rid-{rid}__ds-{datastack}__mv-{materialize_version}__res-{root_resolution[0]}x{root_resolution[1]}x{root_resolution[2]}__cs-{collapse_soma}__cr-{collapse_radius}"
        
        assert format == 'json' or format == 'h5' or format == 'swc'
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
    def get_skeleton_location(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, format):
        return f"{SKELETON_CACHE_LOC}{SkeletonService.get_skeleton_filename(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, format)}"

    @staticmethod
    def retrieve_skeleton_from_cache(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, format):
        '''
        If the requested format is JSON, then read the skeleton and return the json content.
        But if the requested format is H5 or SWC, then return the location of the skeleton file.
        '''
        file_name = SkeletonService.get_skeleton_filename(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, format)
        cf = CloudFiles(SKELETON_CACHE_LOC)
        if cf.exists(file_name):
            if format == 'json':
                return cf.get_json(file_name, COMPRESSION)
            else:  # format == 'h5' or 'swc'
                return SkeletonService.get_skeleton_location(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, format)
        return None

    @staticmethod
    def retrieve_h5_skeleton_from_cache(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius):
        '''
        Fully read H5 skeleton as opposed to just returning the location of the file.
        '''
        file_name = SkeletonService.get_skeleton_filename(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, 'h5')
        cf = CloudFiles(SKELETON_CACHE_LOC)
        if cf.exists(file_name):
            skeleton_bytes = cf.get(file_name)
            with open(file_name, 'wb') as f:
                f.write(skeleton_bytes)
            return skeleton_io.read_skeleton_h5(file_name)
        return None
    
    @staticmethod
    def cache_skeleton(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, skeleton, format):
        file_name = SkeletonService.get_skeleton_filename(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, format)
        cf = CloudFiles(SKELETON_CACHE_LOC)
        if format == 'json':
            cf.put_json(file_name, skeleton, COMPRESSION)
        else:  # format == 'h5' or 'swc'
            cf.put(file_name, skeleton, compress=COMPRESSION)

    @staticmethod
    def generate_skeleton(rid, datastack, materialize_version, root_resolution=[1,1,1], collapse_soma=True, collapse_radius=7500):
        '''
        From https://caveconnectome.github.io/pcg_skel/tutorial/
        '''
        client = caveclient.CAVEclient(datastack)
        client.materialize.version = materialize_version # Ensure we will always use this data release
        
        # Get the location of the soma from nucleus detection:
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
    def get_skeleton_by_rid_sid(rid: int, output_format: str, sid: int, datastack: str, materialize_version: int,
                                root_resolution: List, collapse_soma: bool, collapse_radius: int) -> str:
        # DEBUG
        # rid = 864691135926952148 if rid == 0 else rid # v661: 864691135926952148, current: 864691135701676411
        # sid = 294657 if sid == 0 else sid # nucleus_id  # Nucleus id

        # DEBUG
        rid = 864691135397503777 if rid == 0 else rid       # From https://caveconnectome.github.io/pcg_skel/tutorial/
        datastack = 'minnie65_public'  # From https://caveconnectome.github.io/pcg_skel/tutorial/
        materialize_version = 795      # From https://caveconnectome.github.io/pcg_skel/tutorial/

        use_bucket_swc = False  # Use Emily's SWC skeletons
        if use_bucket_swc:
            # The SID is optional (and only used for SWC retrieval). We can just look it up based on the RID.
            sid = SkeletonService.retrieve_sid_for_rid(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius,) if sid == 0 else sid

            skel_path = "https://storage.googleapis.com/allen-minnie-phase3/minniephase3-emily-pcg-skeletons/minnie_all/v661/skeletons/"
            # dir_name, file_name = skel_path + f"{rid}_{sid}", f"{rid}_{sid}.swc"
            dir_name, file_name = skel_path, f"{rid}_{sid}.swc"
            
            #____________________________________________________________________________________________________
            # Return skeleton JSON object
            if '://' not in dir_name:
                dir_name = utils.cloud_path_join(dir_name, use_file_scheme = True)
            file_path = utils.cloud_path_join(dir_name, file_name)
            df = skel_io.read_swc(file_path)
            return df.to_json()

            #____________________________________________________________________________________________________
            # Return some manually constructed/summarized/described JSON of the skeleton
            # return {
            #     "n_branch_points": sk.n_branch_points,
            #     "n_end_points": sk.n_end_points,
            #     "n_vertices": sk.n_vertices
            # }
        else:
            skeleton_return = SkeletonService.retrieve_skeleton_from_cache(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, output_format)

            if skeleton_return:
                # JSON content or H5 SWC file location
                if output_format == 'json':
                    # The web UI won't show large JSON content, so to assist debugging I'm just returning the smaller data (not lists, etc.)
                    skeleton_return['branch_points'] = 0
                    skeleton_return['branch_points_undirected'] = 0
                    skeleton_return['distance_to_root'] = 0
                    skeleton_return['edges'] = 0
                    skeleton_return['end_points'] = 0
                    skeleton_return['end_points_undirected'] = 0
                    skeleton_return['hops_to_root'] = 0
                    skeleton_return['indices_unmasked'] = 0
                    skeleton_return['mesh_index'] = 0
                    skeleton_return['mesh_to_skel_map'] = 0
                    skeleton_return['mesh_to_skel_map_base'] = 0
                    skeleton_return['meta'] = 0
                    skeleton_return['node_mask'] = 0
                    skeleton_return['segment_map'] = 0
                    skeleton_return['topo_points'] = 0
                    skeleton_return['vertices'] = 0
                return skeleton_return 
            else:
                # If the requested format was JSON or SWC (and getting to this point implies no file already exists),
                # check for an H5 version before generating a new skeleton and use it to build a skeleton object if found.
                skeleton = None
                if output_format == 'json' or output_format == 'swc':
                    # skeleton_return = SkeletonService.retrieve_skeleton_from_cache(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, 'h5')
                    skeleton = SkeletonService.retrieve_h5_skeleton_from_cache(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius)
                
                save_h5 = False
                if not skeleton:
                    # No H5 skeleton was found, so generate a new skeleton
                    skeleton = SkeletonService.generate_skeleton(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius)
                    save_h5 = True

                # Cache the skeleton in the requested format and return the content (JSON) or location (H5 or SWC)
                # Also cache the H5 skeleton if it was generated

                if output_format == 'h5' or save_h5:
                    file_name = SkeletonService.get_skeleton_filename(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, 'h5', False)
                    skeleton_io.write_skeleton_h5(skeleton, file_name)
                    # Read the file back as a bytes object to facilitate CloudFiles.put()
                    file_content = open(file_name, 'rb').read()
                    SkeletonService.cache_skeleton(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, file_content, 'h5')
                if output_format == 'h5':
                    file_location = SkeletonService.get_skeleton_location(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, output_format)
                    return file_location
                
                if output_format == 'swc':
                    file_name = SkeletonService.get_skeleton_filename(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, output_format, False)
                    skeleton_io.export_to_swc(skeleton, file_name)
                    # Read the file back as a bytes object to facilitate CloudFiles.put()
                    file_content = open(file_name, 'rb').read()
                    SkeletonService.cache_skeleton(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, file_content, output_format)
                    file_location = SkeletonService.get_skeleton_location(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, output_format)
                    return file_location
                
                if output_format == 'json':
                    skeleton_json = SkeletonService.skeleton_to_json(skeleton)
                    SkeletonService.cache_skeleton(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, skeleton_json, output_format)
                    # The web UI won't show large JSON content, so to assist debugging I'm just returning the smaller data (not lists, etc.)
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
