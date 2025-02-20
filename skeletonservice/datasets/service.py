from io import BytesIO
import binascii
import logging
import math
import time
from timeit import default_timer
from typing import List, Union
import os
import traceback
import datetime
from messagingclient import MessagingClient
import numpy as np
import pandas as pd
import json
import gzip
from flask import current_app, send_file, Response, request, has_request_context, jsonify
from .skeleton_io_from_meshparty import SkeletonIO
from meshparty import skeleton
import caveclient
import pcg_skel
from cloudfiles import CloudFiles, compression
import cloudvolume

# from skeletonservice.datasets.models import (
#     Skeleton,
# )
# from skeletonservice.datasets.schemas import (
#     SkeletonSchema,
# )

__version__ = "0.16.3"

CAVE_CLIENT_SERVER = os.environ.get("GLOBAL_SERVER_URL", "https://global.daf-apis.com")
CACHE_NON_H5_SKELETONS = True  # Timing experiments have confirmed minimal benefit from caching non-H5 skeletons
CACHE_MESHWORK = False
# DEBUG_SKELETON_CACHE_LOC = "/Users/keith.wiley/Work/Code/SkeletonService/skeletons/"
DEBUG_SKELETON_CACHE_BUCKET = "gs://keith-dev/"
DEBUG_MINIMIZE_JSON_SKELETON = False  # DEBUG: See _minimize_json_skeleton_for_easier_debugging() for explanation.
COMPRESSION = "gzip"  # Valid values mirror cloudfiles.CloudFiles.put() and put_json(): None, 'gzip', 'br' (brotli), 'zstd'
MAX_BULK_SYNCHRONOUS_SKELETONS = 10
DATASTACK_NAME_REMAPPING = {
    'minnie65_public': 'minnie65_phase3_v1',
    'flywire_fafb_public': 'flywire_fafb_production',
}
MESHWORK_VERSION = 1
NEUROGLANCER_SKELETON_VERSION = 2
SKELETON_DEFAULT_VERSION_PARAMS = [-1, 0]  # -1 for latest version, 0 for Neuroglancer version
SKELETON_VERSION_PARAMS = {
    # V1: Basic skeletons
    1: {'@type': 'neuroglancer_skeletons',
            'transform': [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0],
            'vertex_attributes': []},
    # V2: V1 extended to include radii and compartments, compatible with Neuroglancer
    2: {'@type': 'neuroglancer_skeletons',
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
                }]},
    # V3: V2 storing compartments more efficiently as uint8 instead of float32, which is not compatible with Neuroglancer
    3: {'@type': 'neuroglancer_skeletons',  # TODO: This is explicitly *not* a NeuroGlancer representation. So what should this '@type' be?
            'transform': [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0],
            'vertex_attributes': [{
                    'id': 'radius',
                    'data_type': 'float32',
                    'num_components': 1,
                },{
                    'id': 'compartment',
                    'data_type': 'uint8',
                    'num_components': 1,
                }]},
    # V4: V3 extended to include level-2 ids (not included when requesting an SWC skeleton and therefore identical to V3 in the SWC case)
    4: {'@type': 'neuroglancer_skeletons',  # TODO: This is explicitly *not* a NeuroGlancer representation. So what should this '@type' be?
            'transform': [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0],
            'vertex_attributes': [{
                    'id': 'radius',
                    'data_type': 'float32',
                    'num_components': 1,
                },{
                    'id': 'compartment',
                    'data_type': 'uint8',
                    'num_components': 1,
                }]},
}

verbose_level = 0
logging.basicConfig(level=logging.WARNING)


class SkeletonService:
    @staticmethod
    def get_version_specific_handler(skvn: int):
        if skvn == -1:
            skvn = sorted(SKELETON_VERSION_PARAMS.keys())[-1]
        elif skvn == 0:
            skvn = NEUROGLANCER_SKELETON_VERSION
        elif skvn not in SKELETON_VERSION_PARAMS.keys():
            raise ValueError(f"Invalid skeleton version: v{skvn}. Valid versions: {SKELETON_DEFAULT_VERSION_PARAMS + list(SKELETON_VERSION_PARAMS.keys())}")
        return current_app.config['SKELETON_VERSION_ENGINES'][skvn]
    
    @staticmethod
    def _minimize_json_skeleton_for_easier_debugging(skeleton_json):
        """
        The web UI won't show large JSON content, so to assist debugging I'm just returning the smaller data (not lists, etc.)
        """
        skeleton_json["branch_points"] = 0
        skeleton_json["branch_points_undirected"] = 0
        skeleton_json["distance_to_root"] = 0
        skeleton_json["edges"] = 0
        skeleton_json["end_points"] = 0
        skeleton_json["end_points_undirected"] = 0
        skeleton_json["hops_to_root"] = 0
        skeleton_json["indices_unmasked"] = 0
        skeleton_json["mesh_index"] = 0
        skeleton_json["mesh_to_skel_map"] = 0
        skeleton_json["mesh_to_skel_map_base"] = 0
        skeleton_json["meta"] = 0
        skeleton_json["node_mask"] = 0
        skeleton_json["segment_map"] = 0
        skeleton_json["topo_points"] = 0
        skeleton_json["vertices"] = 0
        return skeleton_json

    @staticmethod
    def _get_meshwork_filename(
        rid,
        bucket,
        skeleton_version_UNUSED,
        datastack_name,
        root_resolution,
        collapse_soma,
        collapse_radius,
        include_compression=True,
    ):
        """
        Build a filename for a meshwork file based on the parameters.
        The format and optional compression will be appended as extensions as necessary.
        """
        datastack_name_remapped = DATASTACK_NAME_REMAPPING[datastack_name] if datastack_name in DATASTACK_NAME_REMAPPING else datastack_name

        file_name = f"meshwork__v{MESHWORK_VERSION}__rid-{rid}__ds-{datastack_name_remapped}__res-{root_resolution[0]}x{root_resolution[1]}x{root_resolution[2]}__cs-{collapse_soma}__cr-{collapse_radius}"

        file_name += ".h5"

        if include_compression:
            if COMPRESSION == "gzip":
                file_name += ".gz"
            elif COMPRESSION == "br":
                file_name += ".br"
            elif COMPRESSION == "zstd":
                file_name += ".zst"

        return file_name

    @staticmethod
    def _get_skeleton_filename(
        rid,
        bucket,
        skeleton_version,
        datastack_name,
        root_resolution,
        collapse_soma,
        collapse_radius,
        format,
        include_compression=True,
    ):
        """
        Build a filename for a skeleton file based on the parameters.
        The format and optional compression will be appended as extensions as necessary.
        """
        datastack_name_remapped = DATASTACK_NAME_REMAPPING[datastack_name] if datastack_name in DATASTACK_NAME_REMAPPING else datastack_name

        # materialize_version has been removed, but I've left the stub here for the time being, just in case there is value is seeing and remember its prior usage.
        # file_name = f"skeleton__rid-{rid}__ds-{datastack_name}__mv-{materialize_version}__res-{root_resolution[0]}x{root_resolution[1]}x{root_resolution[2]}__cs-{collapse_soma}__cr-{collapse_radius}"
        file_name = f"skeleton__v{skeleton_version}__rid-{rid}__ds-{datastack_name_remapped}__res-{root_resolution[0]}x{root_resolution[1]}x{root_resolution[2]}__cs-{collapse_soma}__cr-{collapse_radius}"

        assert (
            format == "none"
            or format == "flatdict"
            or format == "json"
            or format == "jsoncompressed"
            or format == "arrays"
            or format == "arrayscompressed"
            or format == "precomputed"
            or format == "h5"
            or format == "swc"
            or format == "swccompressed"
        )
        if format != "none":
            file_name += f".{format}"
        else:
            # If no output is specified, then request or generate an h5 skeleton.
            file_name += ".h5"

        if include_compression:
            if COMPRESSION == "gzip":
                file_name += ".gz"
            elif COMPRESSION == "br":
                file_name += ".br"
            elif COMPRESSION == "zstd":
                file_name += ".zst"

        return file_name

    # @staticmethod
    # def _get_skeleton_location(params, format):
    #     """
    #     Build a location for a skeleton file based on the parameters and the cache location (likely a Google bucket).
    #     """
    #     bucket, skeleton_version = params[1], params[2]
    #     return f"{bucket}{skeleton_version}/{SkeletonService._get_skeleton_filename(*params, format)}"

    @staticmethod
    def _retrieve_skeleton_from_local(params, format):
        """
        This is a debugging function that reads a skeleton from a local file instead of
        computing a skeleton from scratch or retrieving one from a Google bucket.
        """
        try:
            if verbose_level >= 1:
                print("_retrieve_skeleton_from_local()")

            debug_skeleton_cache_loc = os.environ.get("DEBUG_SKELETON_CACHE_LOC", None)
            if debug_skeleton_cache_loc is None:
                if verbose_level >= 1:
                    print("DEBUG_SKELETON_CACHE_LOC is not set.")
                return None
            
            file_name = SkeletonService._get_skeleton_filename(
                *params, "h5", include_compression=False
            )
            if verbose_level >= 1:
                print(f"_retrieve_skeleton_from_local() Looking at {debug_skeleton_cache_loc + file_name}")
            if not os.path.exists(debug_skeleton_cache_loc + file_name):
                if verbose_level >= 1:
                    print(f"_retrieve_skeleton_from_local() No local skeleton file found at {debug_skeleton_cache_loc + file_name}")
                return None

            if verbose_level >= 1:
                print(
                    "_retrieve_skeleton_from_local() Local debug skeleton file found. Reading it..."
                )

            skeleton, lvl2_ids = SkeletonIO.read_skeleton_h5(debug_skeleton_cache_loc + file_name)

            skeleton_version = params[2]

            if format == "flatdict":
                skeleton = SkeletonService._skeleton_to_flatdict(skeleton, lvl2_ids, skeleton_version)
            elif format == "json" or format == "jsoncompressed" or format == "arrays" or format == "arrayscompressed":
                skeleton = SkeletonService._skeleton_to_json(skeleton)
            elif format == "precomputed":
                cv_skeleton = cloudvolume.Skeleton(
                    vertices=skeleton.vertices,
                    edges=skeleton.edges,
                    radii=skeleton.radius,
                    space="voxel",
                    extra_attributes=[
                        {"id": "radius", "data_type": "float32", "num_components": 1}
                    ],
                )
                skeleton = cv_skeleton.to_precomputed()
            elif format == "swc" or format == "swccompressed":
                file_name = SkeletonService._get_skeleton_filename(*params, format, False)
                SkeletonIO.export_to_swc(skeleton, file_name)
                file_content = open(file_name, "rb").read()
                skeleton = file_content
            else:  # format == 'h5'
                # It's already in H5 format, so just return the bytes as-is.
                pass

            return skeleton
        except Exception as e:
            print(f"Exception in _retrieve_skeleton_from_local(): {str(e)}. Traceback:")
            traceback.print_exc()
            return None

    @staticmethod
    def _confirm_skeleton_in_cache(params, format):
        """
        Confirm that the specified format of the skeleton is in the cache.
        """
        if not CACHE_NON_H5_SKELETONS and format != "h5":
            return False
        
        file_name = SkeletonService._get_skeleton_filename(*params, format)
        if verbose_level >= 1:
            print("File name being sought in cache:", file_name)
        bucket, skeleton_version = params[1], params[2]
        if verbose_level >= 1:
            print(f"_confirm_skeleton_in_cache() Querying skeleton at {bucket}{skeleton_version}/{file_name}")
        cf = CloudFiles(f"{bucket}{skeleton_version}/")
        return cf.exists(file_name)

    @staticmethod
    def _retrieve_meshwork_from_cache(params, include_compression):
        """
        """
        file_name = SkeletonService._get_meshwork_filename(
            *params, include_compression=include_compression
        )
        if verbose_level >= 1:
            print("_retrieve_meshwork_from_cache() File name being sought in cache:", file_name)
        bucket = params[1]
        if verbose_level >= 1:
            print(f"_retrieve_meshwork_from_cache() Querying meshwork at {bucket}meshworks/{MESHWORK_VERSION}/{file_name}")
        cf = CloudFiles(f"{bucket}meshworks/{MESHWORK_VERSION}/")
        if cf.exists(file_name):
            return cf.get(file_name)
        
        return None

    @staticmethod
    def _retrieve_skeleton_from_cache(params, format):
        """
        If the requested format is JSON or PRECOMPUTED, then read the skeleton and return it as native content.
        But if the requested format is H5 or SWC, then return the location of the skeleton file.
        """
        if not CACHE_NON_H5_SKELETONS and format != "h5" and format != "h5_mpsk":
            return None
        
        cached_format = format if format != "h5_mpsk" else "h5"
        file_name = SkeletonService._get_skeleton_filename(*params, cached_format)
        if verbose_level >= 1:
            print("_retrieve_skeleton_from_cache() File name being sought in cache:", file_name)
        bucket, skeleton_version = params[1], params[2]
        if verbose_level >= 1:
            print(f"_retrieve_skeleton_from_cache() Querying skeleton at {bucket}{skeleton_version}/{file_name}")
        cf = CloudFiles(f"{bucket}{skeleton_version}/")
        if cf.exists(file_name):
            if format == "flatdict":
                return cf.get(file_name)
            elif format == "json" or format == "arrays":
                return cf.get_json(file_name)
            elif format == "jsoncompressed" or format == "arrayscompressed":
                return cf.get(file_name)
            elif format == "precomputed":
                return cf.get(file_name)
            elif format == "h5":
                skeleton_bytes = cf.get(file_name)
                skeleton_bytes = BytesIO(skeleton_bytes)
                return skeleton_bytes
            elif format == "h5_mpsk":
                skeleton_bytes = cf.get(file_name)
                skeleton_bytes = BytesIO(skeleton_bytes)
                skeleton, lvl2_ids = SkeletonIO.read_skeleton_h5(skeleton_bytes)
                return skeleton, lvl2_ids
            elif format == "swc" or format == "swccompressed":
                skeleton_bytes = cf.get(file_name)
                skeleton_bytes = BytesIO(skeleton_bytes)
                return skeleton_bytes  # Don't even bother building a skeleton object
                
        return None if format != "h5_mpsk" else (None, None)

    @staticmethod
    def _cache_meshwork(params, nrn_file_content, include_compression=True):
        """
        Cache the meshwork in the requested format to the indicated location (likely a Google bucket).
        """
        file_name = SkeletonService._get_meshwork_filename(
            *params, include_compression=include_compression
        )
        bucket = params[1]
        if verbose_level >= 1:
            print(f"Caching meshwork to {bucket}meshworks/{MESHWORK_VERSION}/{file_name}")
        cf = CloudFiles(f"{bucket}meshworks/{MESHWORK_VERSION}/")
        cf.put(
            file_name,
            nrn_file_content,
            compress=COMPRESSION if include_compression else None,
        )

    @staticmethod
    def _cache_skeleton(params, skeleton_file_content, format, include_compression=True):
        """
        Cache the skeleton in the requested format to the indicated location (likely a Google bucket).
        """
        if not CACHE_NON_H5_SKELETONS and format != 'h5':
            return
        
        file_name = SkeletonService._get_skeleton_filename(
            *params, format, include_compression=include_compression
        )
        bucket, skeleton_version = params[1], params[2]
        if verbose_level >= 1:
            print(f"Caching skeleton to {bucket}{skeleton_version}/{file_name}")
        cf = CloudFiles(f"{bucket}{skeleton_version}/")
        if format == "json" or format == "arrays":
            cf.put_json(
                file_name, skeleton_file_content, COMPRESSION if include_compression else None
            )
        else:  # format == 'precomputed' or 'h5' or 'swc' or 'flatdict' or 'jsoncompressed' or 'arrayscompressed'
            cf.put(
                file_name,
                skeleton_file_content,
                compress=COMPRESSION if include_compression else None,
            )
    
    @staticmethod
    def _archive_skeletonization_time(bucket, rid, skeleton_version, skeletonization_elapsed_time):
        """
        Archive the skeletonization time for a given root id.
        TODO: This function does not lock or mutex the file while it alters it (which GCP buckets do not support).
            Multiple skeleton workers could collide here (get1, get2, put1, put2) such that only the last overlapping worker's data is saved.
        """
        if verbose_level >= 1:
            print(f"Archiving skeletonization time for rid {rid} and skeleton version {skeleton_version}: {skeletonization_elapsed_time} seconds")
        
        file_name = "skeletonization_times.csv"
        cf = CloudFiles(f"{bucket}")  # Don't bother entering a skeleton version subdirectory
        if cf.exists(file_name):
            skeleton_times = cf.get(file_name).decode("utf-8")
        else:
            # Initialize a new empty CSV file with a header line
            skeleton_times = "Timestamp,SkeletonService_version,CAVEclient_version,Skeleton_Version,Root_ID,Skeletonization_Time_Secs\n"
        
        skeleton_times += f"{datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')},{__version__},{caveclient.__version__},{skeleton_version},{rid},{skeletonization_elapsed_time}\n"
        skeleton_times_bytes = BytesIO(skeleton_times.encode("utf-8")).getvalue()
        cf.put(file_name, skeleton_times_bytes, compress=True)

    @staticmethod
    def _get_root_soma(rid, client, soma_tables=None):
        """Get the soma position of a root id.

        Args:
            rid (int): root id
            client (caveclient.CAVEclient): client to use
            soma_tables (list): list of soma tables to search in. If None, will use the soma_table from the datastack info

        Returns:
            tuple: (soma position, resolution) or None,None if no soma found

            soma position (np.array): x, y, z position of the soma
            resolution (np.array): x,y z resolution of the soma position in nm
        """

        if soma_tables is None:
            soma_tables = client.info.get_datastack_info()["soma_table"]

            if soma_tables is None:
                return None, None, None
            else:
                soma_tables = [soma_tables]
        now = datetime.datetime.now(datetime.timezone.utc)
        root_ts = client.chunkedgraph.get_root_timestamps(
            rid, latest=True, timestamp=now
        )[0]

        for soma_table in soma_tables:
            soma_df = client.materialize.tables[soma_table](pt_root_id=rid).live_query(
                timestamp=root_ts
            )
            if len(soma_df) == 1:
                break

        if len(soma_df) != 1:
            return None, None, None

        return root_ts, soma_df.iloc[0]["pt_position"], soma_df.attrs["dataframe_resolution"]

    @staticmethod
    def _generate_v1_skeleton(
        rid,
        bucket,
        skeleton_version,
        datastack_name,
        root_resolution,
        collapse_soma,
        collapse_radius,
        cave_client,
    ):
        """
        From https://caveconnectome.github.io/pcg_skel/tutorial/
        """
        if (datastack_name == "minnie65_public") or (
            datastack_name == "minnie65_phase3_v1"
        ):
            soma_tables = ["nucleus_alternative_points", "nucleus_detection_v0"]
        else:
            soma_tables = None

        root_ts, soma_location, soma_resolution = SkeletonService._get_root_soma(
            rid, cave_client, soma_tables
        )
        if verbose_level >= 1:
            print(f"soma_resolution: {soma_resolution}")

        # Get the location of the soma from nucleus detection:
        if verbose_level >= 1:
            print(
                f"_generate_v1_skeleton {rid} {datastack_name} {soma_resolution} {collapse_soma} {collapse_radius}"
            )
            print(f"CAVEClient version: {caveclient.__version__}")

        # Use the above parameters in the skeletonization:
        skel = pcg_skel.pcg_skeleton(
            rid,
            cave_client,
            root_point=soma_location,
            root_point_resolution=soma_resolution,
            collapse_soma=collapse_soma,
            collapse_radius=collapse_radius,
        )

        return skel

    @staticmethod
    def _generate_v2_skeleton(
        rid,
        bucket,
        skeleton_version,
        datastack_name,
        root_resolution,
        collapse_soma,
        collapse_radius,
        cave_client,
        prep_for_neuroglancer=True,
    ):
        """
        Templated and modified from _generate_v1_skeleton().
        """
        if verbose_level >= 1:
            print("_generate_v2_skeleton()", rid)
        if verbose_level >= 1:
            print(f"CAVEClient version: {caveclient.__version__}")
        if (datastack_name == "minnie65_public") or (
            datastack_name == "minnie65_phase3_v1"
        ):
            soma_tables = ["nucleus_alternative_points", "nucleus_detection_v0"]
        else:
            soma_tables = None

        root_ts, soma_location, soma_resolution = SkeletonService._get_root_soma(
            rid, cave_client, soma_tables
        )
        if verbose_level >= 1:
            print(f"soma_resolution: {soma_resolution}")

        # Get the location of the soma from nucleus detection:
        if verbose_level >= 1:
            print(
                f"_generate_v2_skeleton {rid} {datastack_name} {soma_resolution} {collapse_soma} {collapse_radius}"
            )
            print(f"CAVEClient version: {caveclient.__version__}")
        
        use_default_radii = False
        use_default_compartments = False

        # Use the above parameters in the meshwork generation and skeletonization:
        try:
            nrn = pcg_skel.pcg_meshwork(
                rid,
                datastack_name,
                cave_client,
                root_point=soma_location,
                root_point_resolution=soma_resolution,
                collapse_soma=collapse_soma,
                collapse_radius=collapse_radius,
                timestamp=root_ts,
                require_complete=True,
                synapses='all',
                synapse_table=cave_client.info.get_datastack_info().get('synapse_table'),
            )
        
            # Add synapse annotations.
            # At the time of this writing, this fails. Casey is looking into it.
            pcg_skel.features.add_is_axon_annotation(
                nrn,
                pre_anno='pre_syn',
                post_anno='post_syn',
                annotation_name='is_axon',
                threshold_quality=0.5,
                extend_to_segment=True,
                n_times=1
            )

            # Add volumetric properties
            pcg_skel.features.add_volumetric_properties(
                nrn,
                cave_client,
                # attributes: list[str] = VOL_PROPERTIES,
                # l2id_anno_name: str = "lvl2_ids",
                # l2id_col_name: str = "lvl2_id",
                # property_name: str = "vol_prop",
            )

            # Add segment properties
            pcg_skel.features.add_segment_properties(
                nrn,
                # segment_property_name: str = "segment_properties",
                # effective_radius: bool = True,
                # area_factor: bool = True,
                # strahler: bool = True,
                # strahler_by_compartment: bool = False,
                # volume_property_name: str = "vol_prop",
                # volume_col_name: str = "size_nm3",
                # area_col_name: str = "area_nm2",
                # root_as_sphere: bool = True,
                # comp_mask: str = "is_axon",
            )

            # del nrn.anno['pre_syn']
            # del nrn.anno['post_syn']
            skel = nrn.skeleton
        except np.exceptions.AxisError as e:
            use_default_radii = True
            use_default_compartments = True

            nrn = None
            skel = SkeletonService._generate_v1_skeleton(
                rid,
                bucket,
                skeleton_version,
                datastack_name,
                root_resolution,
                collapse_soma,
                collapse_radius,
                cave_client,
            )
        except Exception as e:
            print(f"Exception in _generate_v2_skeleton(): {str(e)}. Traceback:")
            traceback.print_exc()
            raise e

        # Assign the radii information to the skeleton
        if not use_default_radii:
            radius = nrn.anno.segment_properties.df.sort_values(by='mesh_ind')['r_eff'].values
            radius_sk = nrn.mesh_property_to_skeleton(radius, aggfunc="mean")
        else:
            radius_sk = np.ones(len(skel.vertices))
        # nrn.skeleton.radius = radius_sk  # Requires a property setter
        skel._rooted.radius = radius_sk
        skel.vertex_properties['radius'] = skel.radius
        
        # Assign the axon/dendrite information to the skeleton
        DEFAULT_COMPARTMENT_CODE, AXON_COMPARTMENT_CODE, SOMA_COMPARTMENT_CODE = 3, 2, 1
        if not use_default_compartments:
            # The compartment codes are found in skeleton_plot.plot_tools.py:
            # Default, Soma, Axon, Basal
            is_axon = nrn.mesh_property_to_skeleton(nrn.anno.is_axon.mesh_mask, aggfunc="median")
            axon_compartment_encoding = np.array([AXON_COMPARTMENT_CODE if v == 1 else DEFAULT_COMPARTMENT_CODE for v in is_axon])
            if (len(axon_compartment_encoding) != len(skel.vertices)):
                use_default_compartments = True

        if skel.root:
            axon_compartment_encoding[skel.root] = SOMA_COMPARTMENT_CODE

        if use_default_compartments:  # Don't change this to an "else"
            axon_compartment_encoding = np.ones(len(skel.vertices)) * DEFAULT_COMPARTMENT_CODE
        # TODO: See two "skeleton/info" routes in api.py, where the compartment encoding is restricted to float32,
        # due to a Neuroglancer limitation. Therefore, I cast the comparement to a float here for consistency.
        skel.vertex_properties['compartment'] = axon_compartment_encoding.astype(
            np.float32 if prep_for_neuroglancer else np.uint8)

        return nrn, skel

    @staticmethod
    def _generate_v3_skeleton(
        rid,
        bucket,
        skeleton_version,
        datastack_name,
        root_resolution,
        collapse_soma,
        collapse_radius,
        cave_client,
    ):
        if verbose_level >= 1:
            print("_generate_v3_skeleton() (which will pass through to v2)", rid)
        return SkeletonService._generate_v2_skeleton(
            rid,
            bucket,
            skeleton_version,
            datastack_name,
            root_resolution,
            collapse_soma,
            collapse_radius,
            cave_client,
            False,
        )

    @staticmethod
    def _generate_v4_skeleton(
        rid,
        bucket,
        skeleton_version,
        datastack_name,
        root_resolution,
        collapse_soma,
        collapse_radius,
        cave_client,
    ):
        if verbose_level >= 1:
            print("_generate_v4_skeleton() (which will pass through to v3)", rid)
        nrn, sk = SkeletonService._generate_v3_skeleton(
            rid,
            bucket,
            skeleton_version,
            datastack_name,
            root_resolution,
            collapse_soma,
            collapse_radius,
            cave_client,
        )

        lvl2_df = nrn.anno.lvl2_ids.df
        lvl2_df.sort_values(by='mesh_ind', inplace=True)
        lvl2_ids = list(lvl2_df['lvl2_id'])
        if verbose_level >= 1:
            print("_generate_v4_skeleton() rid, len(lvl2_ids):", rid, len(lvl2_ids))
        
        return nrn, sk, lvl2_ids
    
    @staticmethod
    def compressBytes(inputBytes: BytesIO):
        """
        Modeled on compressStringToBytes()
        """
        inputBytes.seek(0)
        stream = BytesIO()
        compressor = gzip.GzipFile(fileobj=stream, mode='wb')
        while True:  # until EOF
            chunk = inputBytes.read(8192)
            if not chunk:  # EOF?
                compressor.close()
                return stream.getvalue()
            compressor.write(chunk)
    
    @staticmethod
    def compressStringToBytes(inputString):
        """
        REF: https://stackoverflow.com/questions/15525837/which-is-the-best-way-to-compress-json-to-store-in-a-memory-based-store-like-red
        read the given string, encode it in utf-8, compress the data and return it as a byte array.
        """
        bio = BytesIO()
        bio.write(inputString.encode("utf-8"))
        bio.seek(0)
        stream = BytesIO()
        compressor = gzip.GzipFile(fileobj=stream, mode='w')
        while True:  # until EOF
            chunk = bio.read(8192)
            if not chunk:  # EOF?
                compressor.close()
                return stream.getvalue()
            compressor.write(chunk)

    @staticmethod
    def compressDictToBytes(inputDict, remove_spaces=True):
        inputDictStr = json.dumps(inputDict)
        if remove_spaces:
            inputDictStr = inputDictStr.replace(' ', '')
        inputDictStrBytes = SkeletonService.compressStringToBytes(inputDictStr)
        return inputDictStrBytes
    
    @staticmethod
    def decompressBytes(inputBytes):
        """
        Modeled on compressStringToBytes()
        """
        bio = BytesIO()
        stream = BytesIO(inputBytes)
        decompressor = gzip.GzipFile(fileobj=stream, mode='r')
        while True:  # until EOF
            chunk = decompressor.read(8192)
            if not chunk:
                decompressor.close()
                bio.seek(0)
                return bio.read()
            bio.write(chunk)
        return None

    @staticmethod
    def decompressBytesToString(inputBytes):
        """
        REF: https://stackoverflow.com/questions/15525837/which-is-the-best-way-to-compress-json-to-store-in-a-memory-based-store-like-red
        decompress the given byte array (which must be valid compressed gzip data) and return the decoded text (utf-8).
        """
        bio = BytesIO()
        stream = BytesIO(inputBytes)
        decompressor = gzip.GzipFile(fileobj=stream, mode='r')
        while True:  # until EOF
            chunk = decompressor.read(8192)
            if not chunk:
                decompressor.close()
                bio.seek(0)
                return bio.read().decode("utf-8")
            bio.write(chunk)
        return None

    @staticmethod
    def decompressBytesToDict(inputBytes):
        inputBytesStr = SkeletonService.decompressBytesToString(inputBytes)
        inputBytesStrDict = json.loads(inputBytesStr)
        return inputBytesStrDict

    @staticmethod
    def _skeleton_metadata_to_json(skeleton_metadata):
        """
        Used by _skeleton_to_json() and _skeleton_to_flatdict().
        """
        return {
            "root_id": skeleton_metadata.root_id,
            "soma_pt_x": skeleton_metadata.soma_pt_x,
            "soma_pt_y": skeleton_metadata.soma_pt_y,
            "soma_pt_z": skeleton_metadata.soma_pt_z,
            "soma_radius": skeleton_metadata.soma_radius,
            "collapse_soma": skeleton_metadata.collapse_soma,
            "collapse_function": skeleton_metadata.collapse_function,
            "invalidation_d": skeleton_metadata.invalidation_d,
            "smooth_vertices": skeleton_metadata.smooth_vertices,
            "compute_radius": skeleton_metadata.compute_radius,
            "shape_function": skeleton_metadata.shape_function,
            "smooth_iterations": skeleton_metadata.smooth_iterations,
            "smooth_neighborhood": skeleton_metadata.smooth_neighborhood,
            "smooth_r": skeleton_metadata.smooth_r,
            "cc_vertex_thresh": skeleton_metadata.cc_vertex_thresh,
            "remove_zero_length_edges": skeleton_metadata.remove_zero_length_edges,
            "collapse_params": skeleton_metadata.collapse_params,
            "timestamp": skeleton_metadata.timestamp,
            "skeleton_type": skeleton_metadata.skeleton_type,
            "meta": {
                "datastack": skeleton_metadata.meta.datastack,
                "space": skeleton_metadata.meta.space,
            },
        }

    @staticmethod
    def _skeleton_to_json(skel):
        """
        Convert a skeleton object to a JSON object.
        """
        sk_json = {
            "jsonification_version": "1.0",
        }
        if skel.branch_points is not None:
            sk_json["branch_points"] = skel.branch_points.tolist()
        if skel.branch_points_undirected is not None:
            sk_json["branch_points_undirected"] = skel.branch_points_undirected.tolist()
        if skel.distance_to_root is not None:
            sk_json["distance_to_root"] = skel.distance_to_root.tolist()
        if skel.edges is not None:
            sk_json["edges"] = skel.edges.tolist()
        if skel.end_points is not None:
            sk_json["end_points"] = skel.end_points.tolist()
        if skel.end_points_undirected is not None:
            sk_json["end_points_undirected"] = skel.end_points_undirected.tolist()
        if skel.hops_to_root is not None:
            sk_json["hops_to_root"] = skel.hops_to_root.tolist()
        if skel.indices_unmasked is not None:
            sk_json["indices_unmasked"] = skel.indices_unmasked.tolist()
        if skel.mesh_index is not None:
            sk_json["mesh_index"] = skel.mesh_index.tolist()
        if skel.mesh_to_skel_map is not None:
            sk_json["mesh_to_skel_map"] = skel.mesh_to_skel_map.tolist()
        if skel.mesh_to_skel_map_base is not None:
            sk_json["mesh_to_skel_map_base"] = skel.mesh_to_skel_map_base.tolist()
        if skel.meta is not None:
            sk_json["meta"] = SkeletonService._skeleton_metadata_to_json(skel.meta)
        if skel.node_mask is not None:
            sk_json["node_mask"] = skel.node_mask.tolist()
        if skel.radius is not None:
            sk_json["radius"] = skel.radius.tolist()
        if skel.root is not None:
            sk_json["root"] = skel.root.tolist()
        if skel.root_position is not None:
            sk_json["root_position"] = skel.root_position.tolist()
        if skel.segment_map is not None:
            sk_json["segment_map"] = skel.segment_map.tolist()
        if skel.topo_points is not None:
            sk_json["topo_points"] = skel.topo_points.tolist()
        if skel.unmasked_size is not None:
            sk_json["unmasked_size"] = skel.unmasked_size
        if skel.vertex_properties is not None:
            sk_json["vertex_properties"] = skel.vertex_properties
            for key in skel.vertex_properties.keys():
                if isinstance(skel.vertex_properties[key], np.ndarray):
                    sk_json["vertex_properties"][key] = skel.vertex_properties[key].tolist()
                else:
                    sk_json["vertex_properties"][key] = skel.vertex_properties[key]
        if skel.vertices is not None:
            sk_json["vertices"] = skel.vertices.tolist()
        if skel.voxel_scaling is not None:
            sk_json["voxel_scaling"] = skel.voxel_scaling
        return sk_json

    @staticmethod
    def _skeleton_to_flatdict(skel, lvl2_ids, skeleton_version):
        """
        Convert a skeleton object to a FLAT DICT object.
        """
        sk_flatdict_vsn = 4  # "1" if skeleton_version <= 3 else "2"

        sk_flatdict = {}
        
        if skel.meta is not None:
            sk_flatdict["meta"] = SkeletonService._skeleton_metadata_to_json(skel.meta)
        else:
            sk_flatdict["meta"] = {}
        sk_flatdict["meta"]["sk_dict_structure_version"] = sk_flatdict_vsn
        sk_flatdict["meta"]["skeleton_version"] = skeleton_version
        
        # if skel.branch_points is not None:
        #     sk_flatdict["branch_points"] = skel.branch_points.tolist()
        # if skel.branch_points_undirected is not None:
        #     sk_flatdict["branch_points_undirected"] = skel.branch_points_undirected.tolist()
        # if skel.distance_to_root is not None:
        #     sk_flatdict["distance_to_root"] = skel.distance_to_root.tolist()
        if skel.edges is not None:
            sk_flatdict["edges"] = skel.edges.tolist()
        # if skel.end_points is not None:
        #     sk_flatdict["end_points"] = skel.end_points.tolist()
        # if skel.end_points_undirected is not None:
        #     sk_flatdict["end_points_undirected"] = skel.end_points_undirected.tolist()
        # if skel.hops_to_root is not None:
        #     sk_flatdict["hops_to_root"] = skel.hops_to_root.tolist()
        # if skel.indices_unmasked is not None:
        #     sk_flatdict["indices_unmasked"] = skel.indices_unmasked.tolist()
        # if skel.mesh_index is not None:
        #     sk_flatdict["mesh_index"] = skel.mesh_index.tolist()
        if skel.mesh_to_skel_map is not None:
            sk_flatdict["mesh_to_skel_map"] = skel.mesh_to_skel_map.tolist()
        # if skel.mesh_to_skel_map_base is not None:
        #     sk_flatdict["mesh_to_skel_map_base"] = skel.mesh_to_skel_map_base.tolist()
        # if skel.node_mask is not None:
        #     sk_flatdict["node_mask"] = skel.node_mask.tolist()
        if skel.root is not None:
            sk_flatdict["root"] = skel.root.tolist()
        # if skel.root_position is not None:
        #     sk_flatdict["root_position"] = skel.root_position.tolist()
        # if skel.segment_map is not None:
        #     sk_flatdict["segment_map"] = skel.segment_map.tolist()
        # if skel.topo_points is not None:
        #     sk_flatdict["topo_points"] = skel.topo_points.tolist()
        # if skel.unmasked_size is not None:
        #     sk_flatdict["unmasked_size"] = skel.unmasked_size
        if skel.vertices is not None:
            sk_flatdict["vertices"] = skel.vertices.tolist()
        # if skel.voxel_scaling is not None:
        #     sk_flatdict["voxel_scaling"] = skel.voxel_scaling
        # vertex_properties should provide radius and compartment
        if skel.vertex_properties is not None:
            for key in skel.vertex_properties.keys():
                assert(key not in sk_flatdict)
                if isinstance(skel.vertex_properties[key], np.ndarray):
                    sk_flatdict[key] = skel.vertex_properties[key].tolist()
                else:
                    sk_flatdict[key] = skel.vertex_properties[key]
        if lvl2_ids is not None:
            if isinstance(lvl2_ids, np.ndarray):
                sk_flatdict["lvl2_ids"] = lvl2_ids.tolist()
            else:
                sk_flatdict["lvl2_ids"] = lvl2_ids

        return sk_flatdict

    @staticmethod
    def _json_to_skeleton(sk_json):
        """
        Convert a JSON description of a skeleton to a skeleton object.
        Most of the skeleton fields can't be populated because they are mere properties, not true member variables, lacking associated setter functions.
        """
        sk = skeleton.Skeleton(
            vertices=np.array(sk_json["vertices"]),
            edges=np.array(sk_json["edges"]),
            root=sk_json["root"],
            mesh_to_skel_map=np.array(sk_json["mesh_to_skel_map"]),
            mesh_index=np.array(sk_json["mesh_index"]) if "mesh_index" in sk_json else None,
            vertex_properties=sk_json["vertex_properties"],
            node_mask=np.array(sk_json["node_mask"]),
            voxel_scaling=sk_json["voxel_scaling"] if "voxel_scaling" in sk_json else None,
            #    meta=skeleton.SkeletonMetadata(sk_json),
        )
        # sk.branch_points = sk_json['branch_points']
        # sk.branch_points_undirected = sk_json['branch_points_undirected']
        # sk.distance_to_root = sk_json['distance_to_root']
        # sk.end_points = sk_json['end_points']
        # sk.end_points_undirected = sk_json['end_points_undirected']
        # sk.hops_to_root = sk_json['hops_to_root']
        # sk.indices_unmasked = sk_json['indices_unmasked']
        # sk.mesh_to_skel_map_base = sk_json['mesh_to_skel_map_base']
        # sk.n_branch_points = sk_json['n_branch_points']
        # sk.n_end_points = sk_json['n_end_points']
        # sk.n_vertices = sk_json['n_vertices']
        # sk.radius = sk_json['radius']
        # sk.root = sk_json['root']
        # sk.root_position = sk_json['root_position']
        # sk.segment_map = sk_json['segment_map']
        # sk.topo_points = sk_json['topo_points']
        # sk.unmasked_size = sk_json['unmasked_size']
        return sk

    @staticmethod
    def _skeleton_to_arrays(skel):
        """
        Convert a skeleton object to a minimal set of arrays.
        """
        sk_json = SkeletonService._skeleton_to_json(skel)
        sk_arrays = {
            'vertices': sk_json['vertices'],
            'vertex_properties': sk_json['vertex_properties'],
            'edges': sk_json['edges'],
        }
        
        return sk_arrays

    @staticmethod
    def _response_headers():
        """
        Build Flask Response header for a requested skeleton object.
        """
        return {
            "access-control-allow-credentials": "true",
            "access-control-expose-headers": "Cache-Control, Content-Disposition, Content-Encoding, Content-Length, Content-Type, Date, ETag, Server, Vary, X-Content-Type-Options, X-Frame-Options, X-Powered-By, X-XSS-Protection",
            "content-disposition": "attachment",
        }

    @staticmethod
    def _after_request(response):
        """
        Optionally gzip a response. Alternatively, return the response unaltered.
        Copied verbatim from materializationengine.blueprints.client.utils.py.
        Then modified with an exception trap because this code might be called through a client instead of a web interface.
        """
        try:
            accept_encoding = request.headers.get("Accept-Encoding", "")
            if verbose_level >= 1:
                print(f"_after_request() accept_encoding: {accept_encoding}")
            
            if "gzip" not in accept_encoding.lower():
                return response

            response.direct_passthrough = False

            if (
                response.status_code < 200
                or response.status_code >= 300
                or "Content-Encoding" in response.headers
            ):
                return response

            pre_compressed_size = len(response.data)
            response.data = compression.gzip_compress(response.data)
            if verbose_level >= 1:
                print(f"_after_request() Compressed data size from {pre_compressed_size} to {len(response.data)}")

            response.headers["Content-Encoding"] = "gzip"
            response.headers["Vary"] = "Accept-Encoding"
            response.headers["Content-Length"] = len(response.data)

            return response
        except Exception as e:
            print(f"Exception in _after_request(): {str(e)}. Traceback:")
            traceback.print_exc()
            return None
    
    @staticmethod
    def get_cache_contents(
        bucket: str,
        skeleton_version: int,
        rid_prefixes: List,
        limit: int = None,
        verbose_level_: int = 0
    ):
        """
        Get the contents of the cache for a specific bucket and skeleton version.
        """

        global verbose_level
        verbose_level = verbose_level_

        if bucket[-1] != "/":
            bucket += "/"

        if verbose_level >= 1:
            print(f"get_cache_contents() bucket: {bucket}, skeleton_version: {skeleton_version}, rid_prefixes: {rid_prefixes}, limit: {limit}")

        cf = CloudFiles(f"{bucket}{skeleton_version}/")
        all_h5_files = []
        for rid_prefix in rid_prefixes:
            prefix = f"skeleton__v{skeleton_version}__rid-{rid_prefix}"
            if verbose_level >= 1:
                print(f"get_cache_contents() prefix: {prefix}")
            one_prefix_files = list(cf.list(prefix=prefix))
            one_prefix_h5_files = [f for f in one_prefix_files if f.endswith(".h5.gz")]
            
            if verbose_level >= 1:
                print(f"get_cache_contents() num_found: {len(one_prefix_h5_files)}")
                if len(one_prefix_h5_files) > 0:
                    print(f"get_cache_contents() first result: {one_prefix_h5_files[0]}")
            
            all_h5_files.extend(one_prefix_h5_files)
        
        if not limit:
            return {
                "num_found": len(all_h5_files),
                "files": all_h5_files,
            }
        return {
                "num_found": len(all_h5_files),
                "files": all_h5_files[:limit],
            }
    
    @staticmethod
    def meshworks_exist(
        bucket: str,
        rids: Union[List, int],
        verbose_level_: int = 0
    ):
        """
        Confirm or deny that a set of root ids have meshworks in the cache.
        """

        global verbose_level
        verbose_level = verbose_level_

        if bucket[-1] != "/":
            bucket += "/"

        if verbose_level >= 1:
            print(f"meshworks_exist() bucket: {bucket}, rids: {rids}")
        
        return_single_value = False
        if not isinstance(rids, list):
            return_single_value = True
            rids = [rids]

        cf = CloudFiles(f"{bucket}meshworks/{MESHWORK_VERSION}/")
        if True:  # include_compression:
            if COMPRESSION == "gzip":
                compression_suffix = ".gz"
            elif COMPRESSION == "br":
                compression_suffix = ".br"
            elif COMPRESSION == "zstd":
                compression_suffix = ".zst"
        filenames = [f"meshwork__v{MESHWORK_VERSION}__rid-{rid}__ds-minnie65_phase3_v1__res-1x1x1__cs-True__cr-7500.h5" + compression_suffix for rid in rids]
        exist_results = cf.exists(filenames)
        exist_results_clean = {
            # See _get_meshwork_filename() for the format of the filename.
            int(filename[(filename.find("rid-")+len("rid-")):filename.find("__ds")]): result for filename, result in exist_results.items()
        }

        if return_single_value:
            exist_results_clean = exist_results_clean[rids[0]]

        return exist_results_clean
    
    @staticmethod
    def skeletons_exist(
        bucket: str,
        skeleton_version: int,
        rids: Union[List, int],
        verbose_level_: int = 0
    ):
        """
        Confirm or deny that a set of root ids have H5 skeletons in the cache.
        """

        global verbose_level
        verbose_level = verbose_level_

        if bucket[-1] != "/":
            bucket += "/"

        if verbose_level >= 1:
            print(f"skeletons_exist() bucket: {bucket}, skeleton_version: {skeleton_version}, rids: {rids}")
        
        return_single_value = False
        if not isinstance(rids, list):
            return_single_value = True
            rids = [rids]

        cf = CloudFiles(f"{bucket}{skeleton_version}/")
        if True:  # include_compression:
            if COMPRESSION == "gzip":
                compression_suffix = ".gz"
            elif COMPRESSION == "br":
                compression_suffix = ".br"
            elif COMPRESSION == "zstd":
                compression_suffix = ".zst"
        filenames = [f"skeleton__v{skeleton_version}__rid-{rid}__ds-minnie65_phase3_v1__res-1x1x1__cs-True__cr-7500.h5" + compression_suffix for rid in rids]
        exist_results = cf.exists(filenames)
        exist_results_clean = {
            # See _get_skeleton_filename() for the format of the filename.
            int(filename[(filename.find("rid-")+len("rid-")):filename.find("__ds")]): result for filename, result in exist_results.items()
        }

        if return_single_value:
            exist_results_clean = exist_results_clean[rids[0]]

        return exist_results_clean

    @staticmethod
    def publish_skeleton_request(
        datastack_name: str,
        rid: int,
        output_format: str,
        bucket: str,
        root_resolution: List,
        collapse_soma: bool,
        collapse_radius: int,
        skeleton_version: int,
        high_priority: bool,
        verbose_level_: int = 0,
    ):
        payload = b""
        attributes = {
            "skeleton_params_rid": f"{rid}",
            "skeleton_params_output_format": output_format,
            "skeleton_params_bucket": bucket,
            "skeleton_params_datastack_name": datastack_name,
            "skeleton_params_root_resolution": f"{' '.join(map(str, root_resolution))}",
            "skeleton_params_collapse_soma": f"{collapse_soma}",
            "skeleton_params_collapse_radius": f"{collapse_radius}",
            "skeleton_version": f"{skeleton_version}",
            "verbose_level": f"{verbose_level_}",
        }

        c = MessagingClient()
        exchange = os.getenv(
            "SKELETON_CACHE_HIGH_PRIORITY_EXCHANGE" if high_priority else "SKELETON_CACHE_LOW_PRIORITY_EXCHANGE",
            None)
        if verbose_level >= 1:
            print(f"publish_skeleton_request() Sending payload for rid {rid} to exchange {exchange}")
        c.publish(exchange, payload, attributes)

        if verbose_level_ >= 1:
            print(f"Message has been dispatched to {exchange}: {datastack_name} {rid} output_format: {output_format} skvn:{skeleton_version} {bucket}")

    @staticmethod
    def get_skeleton_by_datastack_and_rid(
        datastack_name: str,
        rid: int,
        # materialize_version: int,  # Removed
        output_format: str,
        # sid: int,  # Removed
        bucket: str,
        root_resolution: List,
        collapse_soma: bool,
        collapse_radius: int,
        skeleton_version: int = 0,  # The default skeleton version is 0, the Neuroglancer compatible version, not -1, the latest version, for backward compatibility
        via_requests: bool = True,
        verbose_level_: int = 0,
    ):
        """
        Get a skeleton by root id (with optional associated soma id).
        If the requested format already exists in the cache, then return it.
        If not, then generate the skeleton from its cached H5 format and return it.
        If the H5 format also doesn't exist yet, then generate and cache the H5 version before generating and returning the requested format.
        """
        global verbose_level
        verbose_level = verbose_level_

        cache_meshwork = CACHE_MESHWORK or output_format == "meshwork"

        if bucket[-1] != "/":
            bucket += "/"

        if verbose_level >= 1:
            print(
                f"get_skeleton_by_datastack_and_rid() datastack_name: {datastack_name}, rid: {rid}, bucket: {bucket}, skeleton_version: {skeleton_version},",
                f" root_resolution: {root_resolution}, collapse_soma: {collapse_soma}, collapse_radius: {collapse_radius}, output_format: {output_format}",
            )
        
        # Confirm the rid validity in a few ways
        cave_client = caveclient.CAVEclient(
            datastack_name,
            server_address=CAVE_CLIENT_SERVER,
        )

        # Confirm that the rid exists
        if not cave_client.chunkedgraph.is_valid_nodes(rid):
            return
        
        # Confirm that the rid is actually a root id and not some other sort of arbitrary number, e.g., a supervoxel id arriving via request from Neuroglancer
        cv = cave_client.info.segmentation_cloudvolume()
        if cv.meta.decode_layer_id(rid) != cv.meta.n_layers:
            return
        
        if not output_format:
            output_format = "none"

        assert (
            output_format in ["none", "meshwork_none", "flatdict", "json", "jsoncompressed", "arrays", "arrayscompressed", "precomputed", "h5", "swc", "swccompressed", "meshwork"]
        )

        # Resolve various default skeleton version options
        if skeleton_version == -1:
            skeleton_version = sorted(SKELETON_VERSION_PARAMS.keys())[-1]
        elif skeleton_version == 0:
            skeleton_version = NEUROGLANCER_SKELETON_VERSION
        elif skeleton_version not in SKELETON_VERSION_PARAMS.keys():
            raise ValueError(f"Invalid skeleton version: v{skeleton_version}. Valid versions: {SKELETON_DEFAULT_VERSION_PARAMS + list(SKELETON_VERSION_PARAMS.keys())}")

        # materialize_version has been removed but I've left stubs of it throughout this file should the need arise in the future.
        # params = [rid, bucket, datastack_name, materialize_version, root_resolution, collapse_soma, collapse_radius]
        params = [
            rid,
            bucket,
            skeleton_version,
            datastack_name,
            root_resolution,
            collapse_soma,
            collapse_radius,
        ]

        cached_skeleton = None
        cached_meshwork = None
        lvl2_ids = None
        if output_format == "none":
            skel_confirmation = SkeletonService._confirm_skeleton_in_cache(
                params, "h5"
            )
            if skel_confirmation:
                # Nothing else to do, so return
                if verbose_level >= 1:
                    print(f"Skeleton is already in cache: {rid}")
                return
            # At this point, fall through with cached_skeleton set to None to trigger generating a new skeleton.
        elif output_format == "meshwork_none":
            meshwork_confirmation = SkeletonService._retrieve_meshwork_from_cache(
                params, True
            )
            if meshwork_confirmation:
                # Nothing else to do, so return
                if verbose_level >= 1:
                    print(f"Meshwork is already in cache: {rid}")
                return
            # At this point, fall through with cached_meshwork set to None to trigger generating a new skeleton.
        elif output_format in ["flatdict", "json", "jsoncompressed", "arrays", "arrayscompressed", "precomputed", "h5", "swc", "swccompressed"]:
            cached_skeleton = SkeletonService._retrieve_skeleton_from_cache(
                params, output_format
            )
            if verbose_level >= 1:
                print(f"Cached skeleton query result: {cached_skeleton is not None}")
            if verbose_level >= 2:
                print(f"Cache skeleton query result: {cached_skeleton}")
        else:  # output_format == "meshwork":
            cached_meshwork = SkeletonService._retrieve_meshwork_from_cache(
                params, True
            )
            if verbose_level >= 1:
                print(f"Cached meshwork query result: {cached_meshwork is not None}")
            if verbose_level >= 2:
                print(f"Cached meshwork query result: {cached_meshwork}")

        # if os.path.exists(DEBUG_SKELETON_CACHE_LOC):
        #     cached_skeleton = SkeletonService._retrieve_skeleton_from_local(params, output_format)

        skeleton_bytes = None
        if cached_skeleton:
            # cached_skeleton will be JSON or PRECOMPUTED content, or H5 or SWC file bytes.
            # if output_format == "none":
            #     # If no output format is specified (e.g. the messaging interface), then returning a skeleton is not requested,
            #     # merely generating one if it doesn't exist, so, since a skeleton already exists in the cache, we're done.
            #     # There's nothing to generate and nothing to return.
            #     return
            if output_format == "precomputed":
                if via_requests and has_request_context():
                    response = Response(
                        cached_skeleton, mimetype="application/octet-stream"
                    )
                    response.headers.update(SkeletonService._response_headers())
                    response = SkeletonService._after_request(response)
                    return response
                return cached_skeleton
            elif output_format == "flatdict":
                # We can't return the compressed FLAT DICT file directly. We need to convert it to a bytes stream object.
                # skeleton_bytes = cached_skeleton

                if via_requests and has_request_context():
                    response = Response(
                        cached_skeleton, mimetype="application/octet-stream"
                    )
                    response.headers.update(SkeletonService._response_headers())
                    # Don't call after_request to compress the data since it is already compressed.
                    # response = SkeletonService._after_request(response)
                    return response
                return cached_skeleton
            elif output_format == "json":
                if DEBUG_MINIMIZE_JSON_SKELETON:  # DEBUG
                    cached_skeleton = (
                        SkeletonService._minimize_json_skeleton_for_easier_debugging(
                            cached_skeleton
                        )
                    )
                if verbose_level >= 1:
                    print(f"Length of cached skeleton: {len(cached_skeleton)} and corresponding json: {len(json.dumps(cached_skeleton))}")
                
                if via_requests and has_request_context():
                    t0 = default_timer()
                    response = jsonify(cached_skeleton)
                    if verbose_level >= 1:
                        t1 = default_timer()
                        et = t1 - t0
                        print(f"Time to jsonify json dict: {et}s")
                    response.headers.update(SkeletonService._response_headers())
                    response = SkeletonService._after_request(response)
                    return response
                return cached_skeleton
            elif output_format == "jsoncompressed":
                # We can't return the compressed JSON file directly. We need to convert it to a bytes stream object.
                # skeleton_bytes = cached_skeleton

                if via_requests and has_request_context():
                    response = Response(
                        cached_skeleton, mimetype="application/octet-stream"
                    )
                    response.headers.update(SkeletonService._response_headers())
                    # Don't call after_request to compress the data since it is already compressed.
                    # response = SkeletonService._after_request(response)
                    return response
                return cached_skeleton
            elif output_format == "arrays":
                if via_requests and has_request_context():
                    response = jsonify(cached_skeleton)
                    response.headers.update(SkeletonService._response_headers())
                    response = SkeletonService._after_request(response)
                    return response
                return cached_skeleton
            elif output_format == "arrayscompressed":
                # We can't return the compresses ARRAYS file directly. We need to convert it to a bytes stream object.
                # skeleton_bytes = cached_skeleton

                if via_requests and has_request_context():
                    response = Response(
                        cached_skeleton, mimetype="application/octet-stream"
                    )
                    response.headers.update(SkeletonService._response_headers())
                    # Don't call after_request to compress the data since it is already compressed.
                    # response = SkeletonService._after_request(response)
                    return response
                return cached_skeleton
            elif output_format == "h5":
                # We can't return the H5 file directly. We need to convert it to a bytes stream object.
                return cached_skeleton
            elif output_format == "swc":
                skeleton_bytes = cached_skeleton  # In this case, skeleton will just be a BytesIO object.
            elif output_format == "swccompressed":
                skeleton_bytes = cached_skeleton
        elif cached_meshwork:
            # Currently, the only possible output_format is "meshwork"
            if output_format == "meshwork":
                if via_requests and has_request_context():
                    file_name = SkeletonService._get_meshwork_filename(
                        *params, include_compression=True
                    )
                    file_content = SkeletonService.compressBytes(BytesIO(cached_meshwork))
                    
                    response = Response(
                        file_content, mimetype="application/octet-stream"
                    )
                    response.headers.update(SkeletonService._response_headers())
                    # Don't call after_request to compress the data since it is already compressed.
                    # response = SkeletonService._after_request(response)

                    return response
                return cached_meshwork
        
        # At this point:
        # either a cached skeleton was found that could be returned immediately,
        # or a cached skeleton was found, but needs further conversion,
        # or no cached skeleton was found.
        
        # If the requested format was JSON or SWC or PRECOMPUTED (and getting to this point implies no file already exists),
        # check for an H5 version before generating a new skeleton, and if found, then use it to build a skeleton object.
        # There is no need to check for an H5 skeleton if the requested format is H5 or None, since both seek an H5 above.
        nrn = None
        skeleton = None
        if not skeleton_bytes:
            if output_format in ["flatdict", "json", "jsoncompressed", "arrays", "arrayscompressed", "swc", "swccompressed", "precomputed"]:
                skeleton, lvl2_ids = SkeletonService._retrieve_skeleton_from_cache(params, "h5_mpsk")
            if verbose_level >= 1:
                print(f"H5 cache query result: {skeleton}")

        # If no H5 skeleton was found, generate a new skeleton.
        # Note that the skeleton for any given set of parameters will only ever be generated once, regardless of the multiple formats offered.
        # H5 will be used to generate all the other formats as needed.
        generate_new_skeleton = not skeleton and not skeleton_bytes
        if generate_new_skeleton:  # No H5 skeleton was found
            # First attempt a debugging retrieval to bypass computing a skeleton from scratch.
            # On a nonlocal deployment this will simply fail and the skeleton will be generated as normal.
            try:
                skeleton = SkeletonService._retrieve_skeleton_from_local(
                    params, "h5"
                )
            except Exception as e:
                print(f"Exception while retrieving local debugging skeleton for {rid}: {str(e)}. Traceback:")
                traceback.print_exc()
            
            try:
                if not skeleton:
                    if verbose_level >= 1:
                        print("No local (debugging) skeleton found. Proceeding to generate a new skeleton.")
                    skeletonization_start_time = default_timer()
                    if skeleton_version == 1:
                        skeleton = SkeletonService._generate_v1_skeleton(*params, cave_client)
                    elif skeleton_version == 2:
                        nrn, skeleton = SkeletonService._generate_v2_skeleton(*params, cave_client)
                    elif skeleton_version == 3:
                        nrn, skeleton = SkeletonService._generate_v3_skeleton(*params, cave_client)
                    elif skeleton_version == 4:
                        nrn, skeleton, lvl2_ids = SkeletonService._generate_v4_skeleton(*params, cave_client)
                    skeletonization_end_time = default_timer()
                    skeletonization_elapsed_time = skeletonization_end_time - skeletonization_start_time
                    if verbose_level >= 1:
                        print(f"Skeleton successfully generated in {skeletonization_elapsed_time} seconds: {skeleton}")
                    try:
                        SkeletonService._archive_skeletonization_time(bucket, rid, skeleton_version, skeletonization_elapsed_time)
                    except Exception as e:
                        # This is a non-critical operation, so don't let it stop the process.
                        print(f"Exception while archiving skeletonization time: {str(e)}. Traceback:")
                        traceback.print_exc()
                else:
                    if verbose_level >= 1:
                        print("Local (debugging) skeleton was found.")
            except Exception as e:
                print(f"Exception while generating skeleton for {rid}: {str(e)}. Traceback:")
                traceback.print_exc()
                return f"Exception while generating skeleton for {rid}: {str(e)}"

        # Cache the meshwork and the skeleton in the requested format and return the content in various formats.
        # Also cache the H5 skeleton if it was generated.

        # Wrap all attemps to cache the skeleton in a try/except block to catch any exceptions.
        # Attempt to return the successfully generated skeleton regardless of any caching failures.
        # Admittedly, this approach risks shielding developers/debuggers from detecting problems with the cache system, such as bucket failures,
        # but it maximizes the chance of returning a skeleton to the user.

        if output_format == "h5" or output_format == "meshwork" or generate_new_skeleton:
            try:
                debug_skeleton_cache_loc = os.environ.get("DEBUG_SKELETON_CACHE_LOC", None)
                if debug_skeleton_cache_loc:
                    if os.path.exists(debug_skeleton_cache_loc):
                        # Save the skeleton to a local file to faciliate rapid debugging (no need to regenerate the skeleton again).
                        file_name = SkeletonService._get_skeleton_filename(
                            *params, "h5", False
                        )
                        SkeletonIO.write_skeleton_h5(
                            skeleton, lvl2_ids, debug_skeleton_cache_loc + file_name
                        )
            except Exception as e:
                print(f"Exception while saving local debugging skeleton for {rid}: {str(e)}. Traceback:")
                traceback.print_exc()

            try:
                if cache_meshwork:
                    nrn_file_content = BytesIO()
                    nrn.save_meshwork(nrn_file_content, overwrite=False)
                    nrn_file_content_val = nrn_file_content.getvalue()
                    SkeletonService._cache_meshwork(params, nrn_file_content_val)
                    nrn_file_content.seek(0)  # The attached file won't have a proper header if this isn't done.

                sk_file_content = BytesIO()
                SkeletonIO.write_skeleton_h5(skeleton, lvl2_ids, sk_file_content)
                # file_content_sz = file_content.getbuffer().nbytes
                sk_file_content_val = sk_file_content.getvalue()
                SkeletonService._cache_skeleton(params, sk_file_content_val, "h5")
                sk_file_content.seek(0)  # The attached file won't have a proper header if this isn't done.

                if output_format == "h5":
                    if via_requests and has_request_context():
                        file_name = SkeletonService._get_skeleton_filename(
                            *params, output_format, include_compression=False
                        )
                        response = send_file(sk_file_content, "application/x-hdf5", download_name=file_name, as_attachment=True)
                        response = SkeletonService._after_request(response)
                        return response
                    return sk_file_content
                elif output_format == "meshwork":
                    if via_requests and has_request_context():
                        file_name = SkeletonService._get_meshwork_filename(
                            *params, include_compression=True
                        )
                        file_content = SkeletonService.compressBytes(nrn_file_content)
                        
                        response = Response(
                            file_content, mimetype="application/octet-stream"
                        )
                        response.headers.update(SkeletonService._response_headers())
                        # Don't call after_request to compress the data since it is already compressed.
                        # response = SkeletonService._after_request(response)

                        return response
                    return nrn_file_content
            except Exception as e:
                print(f"Exception while caching {output_format.upper()} skeleton for {rid}: {str(e)}. Traceback:")
                traceback.print_exc()

        if output_format == "swc" or output_format == "swccompressed":
            try:
                if not skeleton_bytes:  # There was no SWC in the cache, so we must generate one from the H5
                    assert skeleton is not None
                    file_content = BytesIO()
                    SkeletonIO.export_to_swc(skeleton, file_content,
                                             node_labels=np.array(skeleton.vertex_properties['compartment']).astype(int),
                                             radius=np.array(skeleton.vertex_properties['radius']))
                    # file_content_sz = file_content.getbuffer().nbytes
                    file_content_val = file_content.getvalue()
                    SkeletonService._cache_skeleton(params, file_content_val, output_format)
                    file_content.seek(0)  # The attached file won't have a proper header if this isn't done
                else:
                    # There was an SWC in the cache that we can use directly
                    file_content = skeleton_bytes

                if via_requests and has_request_context():
                    file_name = SkeletonService._get_skeleton_filename(
                        *params, output_format, include_compression=(output_format=="swccompressed")
                    )
                    if output_format == "swccompressed":
                        file_content = SkeletonService.compressBytes(file_content)
                    
                    if output_format == "swc":
                        response = send_file(file_content, "application/octet-stream", download_name=file_name, as_attachment=True)
                        response = SkeletonService._after_request(response)
                    else:
                        response = Response(
                            file_content, mimetype="application/octet-stream"
                        )
                        response.headers.update(SkeletonService._response_headers())
                        # Don't call after_request to compress the data since it is already compressed.
                        # response = SkeletonService._after_request(response)

                    return response
                return file_content
            except Exception as e:
                print(f"Exception while caching {output_format.upper()} skeleton for {rid}: {str(e)}. Traceback:")
                traceback.print_exc()
            return

        if output_format == "flatdict":
            try:
                if not skeleton_bytes:
                    assert skeleton is not None
                    print("Generating flat dict with lvl2_ids of length: ", len(lvl2_ids) if lvl2_ids is not None else 0)
                    skeleton_json = SkeletonService._skeleton_to_flatdict(skeleton, lvl2_ids, skeleton_version)
                    skeleton_bytes = SkeletonService.compressDictToBytes(skeleton_json)
                    SkeletonService._cache_skeleton(params, skeleton_bytes, output_format)
                if via_requests and has_request_context():
                    if verbose_level >= 1:
                        print(f"Compressed FLAT DICT size: {len(skeleton_bytes)}")
                    response = Response(
                        skeleton_bytes, mimetype="application/octet-stream"
                    )
                    response.headers.update(SkeletonService._response_headers())
                        # Don't call after_request to compress the data since it is already compressed.
                    # response = SkeletonService._after_request(response)
                    if response:
                        return response
                return skeleton_bytes
            except Exception as e:
                print(f"Exception while caching {output_format.upper()} skeleton for {rid}: {str(e)}. Traceback:")
                traceback.print_exc()

        if output_format == "json":
            try:
                skeleton_json = SkeletonService._skeleton_to_json(skeleton)
                SkeletonService._cache_skeleton(params, skeleton_json, output_format)
                if DEBUG_MINIMIZE_JSON_SKELETON:  # DEBUG
                    skeleton_json = (
                        SkeletonService._minimize_json_skeleton_for_easier_debugging(
                            skeleton_json
                        )
                    )
                if via_requests and has_request_context():
                    response = jsonify(skeleton_json)
                    response.headers.update(SkeletonService._response_headers())
                    response = SkeletonService._after_request(response)
                    return response
                return skeleton_json
            except Exception as e:
                print(f"Exception while caching {output_format.upper()} skeleton for {rid}: {str(e)}. Traceback:")
                traceback.print_exc()

        if output_format == "jsoncompressed":
            try:
                if not skeleton_bytes:
                    assert skeleton is not None
                    skeleton_json = SkeletonService._skeleton_to_json(skeleton)
                    skeleton_bytes = SkeletonService.compressDictToBytes(skeleton_json)
                    SkeletonService._cache_skeleton(params, skeleton_bytes, output_format)
                if via_requests and has_request_context():
                    if verbose_level >= 1:
                        print(f"Compressed JSON size: {len(skeleton_bytes)}")
                    response = Response(
                        skeleton_bytes, mimetype="application/octet-stream"
                    )
                    response.headers.update(SkeletonService._response_headers())
                        # Don't call after_request to compress the data since it is already compressed.
                    # response = SkeletonService._after_request(response)
                    if response:
                        return response
                return skeleton_bytes
            except Exception as e:
                print(f"Exception while caching {output_format.upper()} skeleton for {rid}: {str(e)}. Traceback:")
                traceback.print_exc()

        if output_format == "arrays":
            # The arrays format is practically identical to the JSON format.
            # Its purpose is to offer a vastly minimized and simplified representation:
            # Just vertices, edges, and vertex properties.
            try:
                skeleton_arrays = SkeletonService._skeleton_to_arrays(skeleton)
                SkeletonService._cache_skeleton(params, skeleton_arrays, output_format)
                if via_requests and has_request_context():
                    response = jsonify(skeleton_arrays)
                    response.headers.update(SkeletonService._response_headers())
                    response = SkeletonService._after_request(response)
                    return response
                return skeleton_arrays
            except Exception as e:
                print(f"Exception while caching {output_format.upper()} skeleton for {rid}: {str(e)}. Traceback:")
                traceback.print_exc()

        if output_format == "arrayscompressed":
            # The arrays format is practically identical to the JSON format.
            # Its purpose is to offer a vastly minimized and simplified representation:
            # Just vertices, edges, and vertex properties.
            try:
                if not skeleton_bytes:
                    assert skeleton is not None
                    skeleton_arrays = SkeletonService._skeleton_to_arrays(skeleton)
                    skeleton_bytes = SkeletonService.compressDictToBytes(skeleton_arrays)
                    SkeletonService._cache_skeleton(params, skeleton_bytes, output_format)
                if via_requests and has_request_context():
                    response = Response(
                        skeleton_bytes, mimetype="application/octet-stream"
                    )
                    response.headers.update(SkeletonService._response_headers())
                    # Don't call after_request to compress the data since it is already compressed.
                    # response = SkeletonService._after_request(response)
                    if response:
                        return response
                return skeleton_bytes
            except Exception as e:
                print(f"Exception while caching {output_format.upper()} skeleton for {rid}: {str(e)}. Traceback:")
                traceback.print_exc()

        if output_format == "precomputed":
            # TODO: These multiple levels of indirection involving converting through a series of various skeleton representations feels ugly. Is there a better way to do this?
            # Convert the MeshParty skeleton to a CloudVolume skeleton
            cv_skeleton = cloudvolume.Skeleton(
                vertices=skeleton.vertices,
                edges=skeleton.edges,
                space="voxel",
                # Passing extra_attributes into the ctor is partially redundant with the calls to add_vertex_attribute() below
                # extra_attributes=[ {"id": k, "data_type": "float32", "num_components": 1} for k in skeleton.vertex_properties.keys() ],
                extra_attributes=[],  # Prevent the defaults from being used
            )
            for item in SKELETON_VERSION_PARAMS[skeleton_version]['vertex_attributes']:
                cv_skeleton.add_vertex_attribute(item['id'], np.array(skeleton.vertex_properties[item['id']], dtype=item['data_type']))
            
            # Convert the CloudVolume skeleton to precomputed format
            skeleton_precomputed = cv_skeleton.to_precomputed()

            # Cache the precomputed skeleton
            try:
                SkeletonService._cache_skeleton(
                    params, skeleton_precomputed, output_format
                )
            except Exception as e:
                print(f"Exception while caching {output_format.upper()} skeleton for {rid}: {str(e)}. Traceback:")
                traceback.print_exc()

            if via_requests and has_request_context():
                response = Response(
                    skeleton_precomputed, mimetype="application/octet-stream"
                )
                response.headers.update(SkeletonService._response_headers())
                response = SkeletonService._after_request(response)
                if response:
                    return response
            return skeleton_precomputed

    @staticmethod
    def get_skeletons_bulk_by_datastack_and_rids(
        datastack_name: str,
        rids: List,
        bucket: str,
        root_resolution: List,
        collapse_soma: bool,
        collapse_radius: int,
        skeleton_version: int = 0,  # The default skeleton version is 0, the Neuroglancer compatible version, not -1, the latest version, for backward compatibility
        output_format: str = "flatdict",
        generate_missing_skeletons: bool = False,  # Deprecated, unused
        verbose_level_: int = 0,
    ):
        """
        Provide bulk retrieval (and optional generation) of skeletons by a list of root ids.
        """
        global verbose_level
        verbose_level = verbose_level_

        if bucket[-1] != "/":
            bucket += "/"

        if verbose_level >= 1:
            print(
                f"get_skeletons_bulk_by_datastack_and_rids() datastack_name: {datastack_name}, rids: {rids}, bucket: {bucket}, skeleton_version: {skeleton_version}",
                f" root_resolution: {root_resolution}, collapse_soma: {collapse_soma}, collapse_radius: {collapse_radius}, output_format: {output_format}, generate_missing_skeletons: {generate_missing_skeletons}",
            )
    
        # CaveClient has a bug (or a disagreement with SkeletonService) in terms of which output_format descriptors are valid.
        # While I should fix the bug in CaveClient, that will involve releasing a new version of CAVEclient, which is a bit of a heavy task for such a trivial problem.
        # I can fix it behind the scenes here more easily.
        assert (output_format == "flatdict" or output_format == "json" or output_format == "swc" or output_format == "jsoncompressed" or output_format == "swccompressed")
        if (output_format == "json" or output_format == "swc"):
            output_format += "compressed"

        if len(rids) > MAX_BULK_SYNCHRONOUS_SKELETONS:
            rids = rids[:MAX_BULK_SYNCHRONOUS_SKELETONS]
            if verbose_level >= 1:
                print(f"get_skeletons_bulk_by_datastack_and_rids() Truncating rids to {MAX_BULK_SYNCHRONOUS_SKELETONS}")

        cave_client = caveclient.CAVEclient(
            datastack_name,
            server_address=CAVE_CLIENT_SERVER,
        )
        cv = cave_client.info.segmentation_cloudvolume()
        
        skeletons = {}
        for rid in rids:
            params = [
                rid,
                bucket,
                skeleton_version,
                datastack_name,
                root_resolution,
                collapse_soma,
                collapse_radius,
            ]

            if not cave_client.chunkedgraph.is_valid_nodes(rid):
                skeletons[rid] = "invalid_rid"
                continue
            if cv.meta.decode_layer_id(rid) != cv.meta.n_layers:
                skeletons[rid] = "invalid_layer_rid"
                continue
            
            skeleton = SkeletonService._retrieve_skeleton_from_cache(params, output_format)
            if verbose_level >= 1:
                print(f"get_skeletons_bulk_by_datastack_and_rids() Cache query result for {output_format} rid {rid}: {skeleton is not None}")
            
            if skeleton is None:  # No JSON or SWC skeleton was found (but the H5 status is unknown at this point)
                h5_available = SkeletonService._confirm_skeleton_in_cache(params, "h5")
                if verbose_level >= 1:
                    print(f"H5 availability for rid {rid}: {h5_available}")
                if h5_available:
                    skeleton = SkeletonService.get_skeleton_by_datastack_and_rid(
                        datastack_name,
                        rid,
                        output_format,
                        bucket,
                        root_resolution,
                        collapse_soma,
                        collapse_radius,
                        skeleton_version,
                        False,
                        verbose_level_,
                    )
                if not h5_available:
                    # No H5 skeleton was found, so generate one asynchronously
                    SkeletonService.publish_skeleton_request(
                        datastack_name,
                        rid,
                        "none",
                        bucket,
                        root_resolution,
                        collapse_soma,
                        collapse_radius,
                        skeleton_version,
                        True,
                        verbose_level_,
                    )
                    skeleton = "async"
            
            if verbose_level >= 1:
                print(f"get_skeletons_bulk_by_datastack_and_rids() Final skeleton for rid {rid}: {skeleton is not None if skeleton != 'async' else skeleton}")
            
            # The BytesIO skeletons aren't JSON serializable and so won't fly back over the wire. Gotta convert 'em.
            # It's debatable whether an ascii encoding of this sort is necessarily smaller than the CSV representation, but presumably it is.
            # I haven't measured the respective sizes to compare and confirm.
            if skeleton == "async":
                skeletons[rid] = skeleton
            else:
                if output_format == "flatdict":
                    skeleton_hex_ascii = binascii.hexlify(skeleton).decode('ascii')
                    skeletons[rid] = skeleton_hex_ascii
                elif output_format == "jsoncompressed":
                    skeleton_hex_ascii = binascii.hexlify(skeleton).decode('ascii')
                    skeletons[rid] = skeleton_hex_ascii
                elif output_format == "swccompressed":
                    skeleton_hex_ascii = binascii.hexlify(skeleton.getvalue()).decode('ascii')
                    skeletons[rid] = skeleton_hex_ascii
    
        return skeletons
    
    @staticmethod
    def get_meshwork_by_datastack_and_rid_async(
        datastack_name: str,
        rid: int,
        bucket: str,
        root_resolution: List,
        collapse_soma: bool,
        collapse_radius: int,
        verbose_level_: int = 0,
    ):
        """
        Generate a meshwork aynschronously. Then poll for the result to be ready and return it.
        """
        global verbose_level
        verbose_level = verbose_level_

        cave_client = caveclient.CAVEclient(
            datastack_name,
            server_address=CAVE_CLIENT_SERVER,
        )
        cv = cave_client.info.segmentation_cloudvolume()
        if not cave_client.chunkedgraph.is_valid_nodes(rid):
            raise ValueError(f"Invalid root id: {rid} (perhaps it doesn't exist; the error is unclear)")
        if cv.meta.decode_layer_id(rid) != cv.meta.n_layers:
            raise ValueError(f"Invalid root id: {rid} (perhaps this is an id corresponding to a different level of the PCG, e.g., a supervoxel id)")
        
        t0 = default_timer()

        if not SkeletonService.meshworks_exist(
            bucket,
            rid,
            verbose_level_
        ):
            t1 = default_timer()

            SkeletonService.publish_skeleton_request(
                datastack_name,
                rid,
                "meshwork",
                bucket,
                root_resolution,
                collapse_soma,
                collapse_radius,
                -1,
                True,
                verbose_level_,
            )

            t2 = default_timer()

            if verbose_level >= 1:
                print(f"Polling for meshwork to be available for rid {rid}...")
            while not SkeletonService.meshworks_exist(
                bucket,
                rid,
                verbose_level_
            ):
                time.sleep(5)
            if verbose_level >= 1:
                print(f"Meshwork is now available for rid {rid}.")
        else:
            t2 = t1 = default_timer()
            if verbose_level >= 1:
                print(f"No need to initiate asynchronous skeleton generation for rid {rid}. It already exists.")
        
        t3 = default_timer()

        meshwork = SkeletonService.get_skeleton_by_datastack_and_rid(
            datastack_name,
            rid,
            "meshwork",
            bucket,
            root_resolution,
            collapse_soma,
            collapse_radius,
            -1,
            True,
            verbose_level_,
        )
        
        t4 = default_timer()

        if verbose_level >= 1:
            et1 = t1 - t0
            et2 = t2 - t1
            et3 = t3 - t2
            et4 = t4 - t3
            print(f"get_meshwork_by_datastack_and_rid_async() Elapsed times: {et1:.3f}s {et2:.3f}s {et3:.3f}s {et4:.3f}s")
            print(f"get_meshwork_by_datastack_and_rid_async() Final skeleton for rid {rid}: {meshwork is not None}")
        
        return meshwork
    
    @staticmethod
    def get_skeleton_by_datastack_and_rid_async(
        datastack_name: str,
        rid: int,
        output_format: str,
        bucket: str,
        root_resolution: List,
        collapse_soma: bool,
        collapse_radius: int,
        skeleton_version: int = 0,  # The default skeleton version is 0, the Neuroglancer compatible version, not -1, the latest version, for backward compatibility
        verbose_level_: int = 0,
    ):
        """
        Generate a skeleton aynschronously. Then poll for the result to be ready and return it.
        """
        global verbose_level
        verbose_level = verbose_level_

        cave_client = caveclient.CAVEclient(
            datastack_name,
            server_address=CAVE_CLIENT_SERVER,
        )
        cv = cave_client.info.segmentation_cloudvolume()
        if not cave_client.chunkedgraph.is_valid_nodes(rid):
            raise ValueError(f"Invalid root id: {rid} (perhaps it doesn't exist; the error is unclear)")
        if cv.meta.decode_layer_id(rid) != cv.meta.n_layers:
            raise ValueError(f"Invalid root id: {rid} (perhaps this is an id corresponding to a different level of the PCG, e.g., a supervoxel id)")
        
        t0 = default_timer()

        if not SkeletonService.skeletons_exist(
            bucket,
            skeleton_version,
            rid,
            verbose_level_
        ):
            t1 = default_timer()

            SkeletonService.publish_skeleton_request(
                datastack_name,
                rid,
                "none",
                bucket,
                root_resolution,
                collapse_soma,
                collapse_radius,
                skeleton_version,
                True,
                verbose_level_,
            )

            t2 = default_timer()

            if verbose_level >= 1:
                print(f"Polling for skeleton to be available for rid {rid}...")
            while not SkeletonService.skeletons_exist(
                bucket,
                skeleton_version,
                rid,
                verbose_level_
            ):
                time.sleep(5)
            if verbose_level >= 1:
                print(f"Skeleton is now available for rid {rid}.")
        else:
            t2 = t1 = default_timer()
            if verbose_level >= 1:
                print(f"No need to initiate asynchronous skeleton generation for rid {rid}. It already exists.")
        
        t3 = default_timer()

        skeleton = SkeletonService.get_skeleton_by_datastack_and_rid(
            datastack_name,
            rid,
            output_format,
            bucket,
            root_resolution,
            collapse_soma,
            collapse_radius,
            skeleton_version,
            True,
            verbose_level_,
        )
        
        t4 = default_timer()

        if verbose_level >= 1:
            et1 = t1 - t0
            et2 = t2 - t1
            et3 = t3 - t2
            et4 = t4 - t3
            print(f"get_skeleton_by_datastack_and_rid_async() Elapsed times: {et1:.3f}s {et2:.3f}s {et3:.3f}s {et4:.3f}s")
            print(f"get_skeleton_by_datastack_and_rid_async() Final skeleton for rid {rid}: {skeleton is not None}")
        
        return skeleton

    
    @staticmethod
    def generate_meshworks_bulk_by_datastack_and_rids_async(
        datastack_name: str,
        rids: List,
        bucket: str,
        root_resolution: List,
        collapse_soma: bool,
        collapse_radius: int,
        verbose_level_: int = 0,
    ):
        """
        Generate multiple skeletons aynschronously without returning anything.
        """
        global verbose_level
        verbose_level = verbose_level_
        
        if verbose_level_ >= 1:
            print(f"generate_meshworks_bulk_by_datastack_and_rids_async() datastack_name: {datastack_name}, rids: {rids}, bucket: {bucket}")

        cave_client = caveclient.CAVEclient(
            datastack_name,
            server_address=CAVE_CLIENT_SERVER,
        )
        cv = cave_client.info.segmentation_cloudvolume()

        num_valid_rids = 0
        for rid in rids:
            if cave_client.chunkedgraph.is_valid_nodes(rid) and cv.meta.decode_layer_id(rid) == cv.meta.n_layers: 
                SkeletonService.publish_skeleton_request(
                    datastack_name,
                    rid,
                    "meshwork_none",
                    bucket,
                    root_resolution,
                    collapse_soma,
                    collapse_radius,
                    -1,
                    False,
                    verbose_level_,
                )
                num_valid_rids += 1
        
        meshwork_generation_time_estimate_secs = 60  # seconds
        try:
            num_workers = current_app.config["SKELETONCACHE_MAX_REPLICAS"]  # Number of skeleton worker (Kubernetes pods) available (# This should be read from the server somehow)
        except KeyError:
            print("Flask config variable SKELETONCACHE_MAX_REPLICAS not found. Using default value of 15.")
            num_workers = 15
        estimated_async_time_secs_upper_bound =  math.ceil(num_valid_rids / num_workers) * meshwork_generation_time_estimate_secs
        if verbose_level >= 1:
            print(f"Estimated async time: ceiling({num_valid_rids} / {num_workers}) * {meshwork_generation_time_estimate_secs} = {estimated_async_time_secs_upper_bound}")
        return estimated_async_time_secs_upper_bound

    
    @staticmethod
    def generate_skeletons_bulk_by_datastack_and_rids_async(
        datastack_name: str,
        rids: List,
        bucket: str,
        root_resolution: List,
        collapse_soma: bool,
        collapse_radius: int,
        skeleton_version: int = 0,  # The default skeleton version is 0, the Neuroglancer compatible version, not -1, the latest version, for backward compatibility
        verbose_level_: int = 0,
    ):
        """
        Generate multiple skeletons aynschronously without returning anything.
        """
        global verbose_level
        verbose_level = verbose_level_
        
        cave_client = caveclient.CAVEclient(
            datastack_name,
            server_address=CAVE_CLIENT_SERVER,
        )
        cv = cave_client.info.segmentation_cloudvolume()
        
        num_valid_rids = 0
        for rid in rids:
            if cave_client.chunkedgraph.is_valid_nodes(rid) and cv.meta.decode_layer_id(rid) == cv.meta.n_layers: 
                SkeletonService.publish_skeleton_request(
                    datastack_name,
                    rid,
                    "none",
                    bucket,
                    root_resolution,
                    collapse_soma,
                    collapse_radius,
                    skeleton_version,
                    False,
                    verbose_level_,
                )
                num_valid_rids += 1
        
        skeleton_generation_time_estimate_secs = 60  # seconds
        try:
            num_workers = current_app.config["SKELETONCACHE_MAX_REPLICAS"]  # Number of skeleton worker (Kubernetes pods) available (# This should be read from the server somehow)
        except KeyError:
            print("Flask config variable SKELETONCACHE_MAX_REPLICAS not found. Using default value of 15.")
            num_workers = 15
        estimated_async_time_secs_upper_bound =  math.ceil(num_valid_rids / num_workers) * skeleton_generation_time_estimate_secs
        if verbose_level >= 1:
            print(f"Estimated async time: ceiling({num_valid_rids} / {num_workers}) * {skeleton_generation_time_estimate_secs} = {estimated_async_time_secs_upper_bound}")
        return estimated_async_time_secs_upper_bound
