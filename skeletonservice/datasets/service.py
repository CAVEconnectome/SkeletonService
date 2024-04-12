from typing import List, Dict
import os
import pandas as pd
import json
from meshparty import skeleton
import skeleton_plot.skel_io as skel_io
from skeleton_plot import utils
import caveclient
import pcg_skel
from cloudfiles import CloudFiles

from skeletonservice.datasets.models import (
    Skeleton,
)
from skeletonservice.datasets.schemas import (
    SkeletonSchema,
)

# SKELETON_CACHE_LOC = "file:///Users/keith.wiley/Work/Code/SkeletonService/skeletons/"
SKELETON_CACHE_LOC = "gs://keith-dev/"
COMPRESSION = 'gzip'  # Valid values mirror cloudfiles.CloudFiles.put_json(): None, 'gzip', 'br' (brotli), 'zstd'

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
    def get_skeleton_filename(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius):
        file_name = f"skeleton__rid-{rid}__ds-{datastack}__mv-{materialize_version}__res-{root_resolution[0]}x{root_resolution[1]}x{root_resolution[2]}__cs-{collapse_soma}__cr-{collapse_radius}.json"
        if COMPRESSION == 'gzip':
            file_name += ".gz"
        elif COMPRESSION == 'br':
            file_name += ".br"
        elif COMPRESSION == 'zstd':
            file_name += ".zst"
        return file_name

    @staticmethod
    def retrieve_skeleton_json_from_cache(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius):
        file_name = SkeletonService.get_skeleton_filename(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius)
        cf = CloudFiles(SKELETON_CACHE_LOC)
        if cf.exists(file_name):
            return cf.get_json(file_name, COMPRESSION)
        return None
    
    @staticmethod
    def cache_skeleton(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, skeleton_json):
        file_name = SkeletonService.get_skeleton_filename(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius)
        cf = CloudFiles(SKELETON_CACHE_LOC)
        cf.put_json(file_name, skeleton_json, COMPRESSION)

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
            'branch_points': skel.branch_points.tolist(),
            'branch_points_undirected': skel.branch_points_undirected.tolist(),
            # 'cover_paths': skel.cover_paths.tolist(),
            # 'csgraph':
            # 'csgraph_binary':
            # 'csgraph_binary_undirected':
            # 'csgraph_undirected':
            'distance_to_root': skel.distance_to_root.tolist(),
            'edges': skel.edges.tolist(),
            'end_points': skel.end_points.tolist(),
            'end_points_undirected': skel.end_points_undirected.tolist(),
            'hops_to_root': skel.hops_to_root.tolist(),
            'indices_unmasked': skel.indices_unmasked.tolist(),
            # 'kdtree':
            'mesh_index': skel.mesh_index.tolist(),
            'mesh_to_skel_map': skel.mesh_to_skel_map.tolist(),
            'mesh_to_skel_map_base': skel.mesh_to_skel_map_base.tolist(),
            'meta': SkeletonService.skeleton_metadata_to_json(skel.meta),
            'n_branch_points': skel.n_branch_points,
            'n_end_points': skel.n_end_points,
            'n_vertices': skel.n_vertices,
            'node_mask': skel.node_mask.tolist(),
            # 'pykdtree':
            'radius': skel.radius,
            'root': skel.root.tolist(),
            'root_position': skel.root_position.tolist(),
            'segment_map': skel.segment_map.tolist(),
            # 'segments':
            # 'segments_plus':
            'topo_points': skel.topo_points.tolist(),
            'unmasked_size': skel.unmasked_size,
            'vertex_properties': skel.vertex_properties,
            'vertices': skel.vertices.tolist(),
            'voxel_scaling': skel.voxel_scaling,
        }
        return sk_json

    @staticmethod
    def get_skeleton_by_rid_sid(rid: int, sid: int, datastack: str, materialize_version: int,
                                root_resolution: List, collapse_soma: bool, collapse_radius: int) -> pd.DataFrame:
        # DEBUG
        # rid = 864691135926952148 if rid == 0 else rid # v661: 864691135926952148, current: 864691135701676411
        # sid = 294657 if sid == 0 else sid # nucleus_id  # Nucleus id

        # DEBUG
        rid = 864691135397503777 if rid == 0 else rid       # From https://caveconnectome.github.io/pcg_skel/tutorial/
        datastack = 'minnie65_public'  # From https://caveconnectome.github.io/pcg_skel/tutorial/
        materialize_version = 795      # From https://caveconnectome.github.io/pcg_skel/tutorial/

        use_bucket_swc = False
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
            skel_json = SkeletonService.retrieve_skeleton_json_from_cache(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius)

            if skel_json:
                return skel_json
            else:
                skeleton_json = SkeletonService.skeleton_to_json(SkeletonService.generate_skeleton(
                    rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius))
                SkeletonService.cache_skeleton(rid, datastack, materialize_version, root_resolution, collapse_soma, collapse_radius, skeleton_json)
                return skeleton_json
