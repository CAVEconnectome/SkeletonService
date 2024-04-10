from typing import List, Dict
import os
import pandas as pd
import json
from meshparty import skeleton
import skeleton_plot.skel_io as skel_io
from skeleton_plot import utils
import caveclient
import pcg_skel

from skeletonservice.datasets.models import (
    Skeleton,
)
from skeletonservice.datasets.schemas import (
    SkeletonSchema,
)

SKELETON_CACHE_LOC = './skeletons'

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
    def get_skeleton_filename(rid, datastack, materialize_version):
        return f"skeleton__rid-{rid}__ds-{datastack}__mv-{materialize_version}.json"

    @staticmethod
    def retrieve_skeleton_json_from_cache(rid, datastack, materialize_version):
        os.makedirs(SKELETON_CACHE_LOC, exist_ok=True)

        file_name = SkeletonService.get_skeleton_filename(rid, datastack, materialize_version)
        file_path = os.path.join(SKELETON_CACHE_LOC, file_name)

        if os.path.exists(file_path):
            with open(file_path) as f:
                skeleton_json = json.load(f)
                return skeleton_json
        return None
    
    @staticmethod
    def cache_skeleton(rid, datastack, materialize_version, skeleton_json):
        os.makedirs(SKELETON_CACHE_LOC, exist_ok=True)

        file_name = SkeletonService.get_skeleton_filename(rid, datastack, materialize_version)
        file_path = os.path.join(SKELETON_CACHE_LOC, file_name)
        
        if not os.path.exists(file_path):
            with open(file_path, 'w') as f:
                json.dump(skeleton_json, f)

    @staticmethod
    def generate_skeleton(rid, datastack, materialize_version):
        '''
        From https://caveconnectome.github.io/pcg_skel/tutorial/
        TODO: I'm not confident that the parameter defaults are the optimal defaults, or even that there should be defaults at all; perhaps they should be required API inputs
        '''
        client = caveclient.CAVEclient(datastack)
        client.materialize.version = materialize_version # Ensure we will always use this data release
        
        # Get the location of the soma from nucleus detection:
        root_resolution = [1,1,1] # Could be another resolution as well, but this will mean the location is in nm.
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
            collapse_soma=True,
            collapse_radius=7500,
        )

        return skel
    
    @staticmethod
    def skeleton_to_json(skel):
        sk_json = {
            'jsonification_version': '1.0',
            'n_branch_points': skel.n_branch_points,
            'n_end_points': skel.n_end_points,
            'n_vertices': skel.n_vertices,
            'vertices': skel.vertices.tolist(),
            'edges': skel.edges.tolist(),
        }
        return sk_json

    @staticmethod
    def get_skeleton_by_rid_sid(rid: int, sid: int, datastack: str, materialize_version: int) -> pd.DataFrame:
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
            sid = SkeletonService.retrieve_sid_for_rid(rid, datastack, materialize_version) if sid == 0 else sid

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
            skel_json = SkeletonService.retrieve_skeleton_json_from_cache(rid, datastack, materialize_version)

            if skel_json:
                return skel_json
            else:
                skeleton_json = SkeletonService.skeleton_to_json(SkeletonService.generate_skeleton(rid, datastack, materialize_version))
                SkeletonService.cache_skeleton(rid, datastack, materialize_version, skeleton_json)
                return skeleton_json
