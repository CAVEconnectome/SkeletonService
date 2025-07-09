from __future__ import annotations
from pcg_skel import chunk_tools, features
from pcg_skel import skel_utils as sk_utils

import cloudvolume
import warnings
import numpy as np
import datetime

from caveclient import CAVEclient
from meshparty import meshwork, skeletonize, trimesh_io
from typing import Union, Optional

Numeric = Union[int, float, np.number]

DEFAULT_VOXEL_RESOLUTION = [4, 4, 40]
DEFAULT_COLLAPSE_RADIUS = 7500.0
DEFAULT_INVALIDATION_D = 7500

skeleton_type = "pcg_skel"

def pcg_graph(
    root_id: int,
    client: CAVEclient.frameworkclient.CAVEclientFull,
    cv: cloudvolume.CloudVolume = None,
    return_l2dict: bool = False,
    nan_rounds: int = 10,
    require_complete: bool = False,
    level2_graph: Optional[np.ndarray] = None,
):
    """Compute the level 2 spatial graph (or mesh) of a given root id using the l2cache.

    Parameters
    ----------
    root_id : int
        Root id of a segment
    client : CAVEclient.caveclient
        Initialized CAVEclient for the dataset.
    cv : cloudvolume.CloudVolume
        Initialized CloudVolume object for the dataset. This does not replace the caveclient, but
        a pre-initizialized cloudvolume can save some time during batch processing.
    return_l2dict : bool
        If True, returns the mappings between l2 ids and vertices.
    nan_rounds : int
        If vertices are missing (or not computed), this sets the number of iterations for smoothing over them.
    require_complete : bool
        If True, raise an Exception if any vertices are missing from the cache.
    level2_graph : np.ndarray, optional
        Level 2 graph for the root id as returned by client.chunkedgraph.level2_chunk_graph.
        A list of lists of edges between level 2 chunks, as defined by their chunk ids.
        If None, will query the chunkedgraph for the level 2 graph. Optional, by default None.


    Returns
    -------
    mesh : meshparty.trimesh_io.Mesh
        Object with a vertex for every level 2 id and edges between all connected level 2 chunks.
    l2dict : dict, optional
        Dictionary with keys as level 2 ids and values as mesh vertex index. Optional, only returned if `return_l2dict` is True.
    l2dict_reverse : dict, optional
        Dictionary with keys as mesh vertex indices and values as level 2 id. Optional, only returned if `return_l2dict` is True.
    """
 
    if level2_graph is None:
        lvl2_eg = client.chunkedgraph.level2_chunk_graph(root_id)
    else:
        lvl2_eg = level2_graph
    if len(lvl2_eg) == 0:
        l2id = client.chunkedgraph.get_leaves(root_id, stop_layer=2)
        lvl2_eg = np.array([l2id, l2id]).reshape((1, 2))
        is_singlet = True
    else:
        is_singlet = False

    eg, l2dict_mesh, l2dict_r_mesh, x_ch = chunk_tools.build_spatial_graph(
        lvl2_eg,
        cv=cv,
        client=client,
        method="service",
        require_complete=require_complete,
    )
    if is_singlet:
        eg = None
    mesh_loc = trimesh_io.Mesh(
        vertices=x_ch,
        faces=[[0, 0, 0]],  # Some functions fail if no faces are set.
        link_edges=eg,
    )

    sk_utils.fix_nan_verts_mesh(mesh_loc, nan_rounds)

    if return_l2dict:
        return mesh_loc, l2dict_mesh, l2dict_r_mesh
    else:
        return mesh_loc


def pcg_skeleton(
    root_id: int,
    client: CAVEclient.frameworkclient.CAVEclientFull,
    datastack_name: str = None,
    cv: cloudvolume.CloudVolume = None,
    invalidation_d: Numeric = 7500,
    return_mesh: bool = False,
    return_l2dict: bool = False,
    return_l2dict_mesh: bool = False,
    root_point: list = None,
    root_point_resolution: list = None,
    collapse_soma: bool = False,
    collapse_radius: Numeric = 7500,
    nan_rounds: int = 10,
    require_complete: bool = False,
    level2_graph: Optional[np.ndarray] = None,
):
    """Produce a skeleton from the level 2 graph.
    Parameters
    ----------
    root_id : int
        Root id of a segment
    client : CAVEclient.caveclient.CAVEclientFull
        Initialized CAVEclient for the dataset.
    datastack_name : string, optional
        If client is None, initializes a CAVEclient at this datastack.
    cv : cloudvolume.CloudVolume, optional
        Initialized CloudVolume object for the dataset. This does not replace the caveclient, but
        a pre-initizialized cloudvolume can save some time during batch processing.
    invalidation_d : int, optional
        Distance (in nanometers) for TEASAR skeleton invalidation.
    return_mesh : bool, optional
        If True, returns the mesh graph as well as the skeleton.
    return_l2dict : bool, optional
        If True, returns the mappings between l2 ids and skeleton vertices.
    return_l2dict_mesh : bool, optional
        If True, returns mappings between l2 ids and mesh graph vertices.
    root_point : npt.ArrayLike, optional
        3-element list or array with the x,y,z location of the root point.
        If None, the most distant tip is set to root.
    root_point_resolution : npt.ArrayLike, optional
        3-element list or array with the x,y,z resolution of the root point, in nanometers per voxel dimension.
    collapse_soma : bool, optional
        If True, collapse nearby vertices into the root point.
    collapse_radius : int, optional
        Distance (in nanometers) for soma collapse.
    nan_rounds : int, optional
        If vertices are missing (or not computed), this sets the number of iterations for smoothing over them.
    require_complete : bool, optional
        If True, raise an Exception if any vertices are missing from the cache.
    level2_graph : np.ndarray, optional
        Level 2 graph for the root id as returned by client.chunkedgraph.level2_chunk_graph.
        A list of lists of edges between level 2 chunks, as defined by their chunk ids.
        If None, will query the chunkedgraph for the level 2 graph. Optional, by default None.

    Returns
    -------
    sk : meshparty.skeleton.Skeleton
        Skeleton for the root id
    mesh : meshparty.trimesh_io.Mesh, optional
        Mesh graph that the skeleton is based on, only returned if return_mesh is True.
    (l2dict_skel, l2dict_skel_reverse): (dict, dict), optional
        Dictionaries mapping l2 ids to skeleton vertices and skeleton vertices to l2 ids, respectively. Only returned if return_l2dict is True.
    (l2dict_mesh, l2dict_mesh_reverse): (dict, dict), optional
        Dictionaries mapping l2 ids to mesh graph vertices and mesh_graph vertices to l2 ids, respectively. Only returned if return_l2dict is True.
    """
    if client is None:
        client = CAVEclient(datastack_name)
 
    if root_point_resolution is None and root_point is not None:
        if cv is None:
            cv = client.info.segmentation_cloudvolume(progress=False)
        if cv is None:
            raise ValueError(
                "Must provide either a client or cv object to get the root_point_resolution."
            )
        root_point_resolution = cv.mip_resolution(0)
    if root_point is not None:
        root_point = np.array(root_point) * root_point_resolution

    mesh, l2dict_mesh, l2dict_r_mesh = pcg_graph(
        root_id,
        client=client,
        return_l2dict=True,
        nan_rounds=nan_rounds,
        require_complete=require_complete,
        level2_graph=level2_graph,
    )

    metameta = {"space": "l2cache", "datastack": client.datastack_name}
    if len(mesh.vertices) == 1:
        vertices = np.array([mesh.vertices[0], mesh.vertices[0]])
        sk = skeletonize.Skeleton(
            vertices=vertices,
            edges=np.array([0, 1]).reshape((1, 2)),
            root=0,
            mesh_to_skel_map=np.array([0]),
            mesh_index=np.array([0, 0]),
            remove_zero_length_edges=False,
            meta={
                "root_id": root_id,
                "skeleton_type": skeleton_type,
                "meta": metameta,
            },
        )
        l2dict, l2dict_r = l2dict_mesh.copy(), l2dict_r_mesh.copy()
        # Assign a fake l2id of zero to the duplicate vertex
        l2dict_r[1] = 0
        l2dict[0] = 1

    else:
        sk = skeletonize_mesh(
            mesh,
            invalidation_d=invalidation_d,
            soma_pt=root_point,
            collapse_soma=collapse_soma,
            soma_radius=collapse_radius,
            compute_radius=False,
            cc_vertex_thresh=0,
            remove_zero_length_edges=True,
            meta={
                "root_id": root_id,
                "skeleton_type": skeleton_type,
                "meta": metameta,
            },
        )
        l2dict, l2dict_r = sk_utils.filter_l2dict(sk, l2dict_r_mesh)

    print("pcg_skeleton() sk.vertex_properties: ", sk.vertex_properties)

    out_list = [sk]
    if return_mesh:
        out_list.append(mesh)
    if return_l2dict:
        out_list.append((l2dict, l2dict_r))
    if return_l2dict_mesh:
        out_list.append((l2dict_mesh, l2dict_r_mesh))
    if len(out_list) == 1:
        return out_list[0]
    else:
        return tuple(out_list)


def pcg_meshwork(
    root_id: int,
    datastack_name: Optional[str] = None,
    client: Optional[CAVEclient] = None,
    cv: Optional[cloudvolume.CloudVolume] = None,
    root_point: Optional[list] = None,
    root_point_resolution: Optional[list] = None,
    collapse_soma: bool = False,
    collapse_radius: Numeric = DEFAULT_COLLAPSE_RADIUS,
    synapses: Optional[Union[bool, str]] = None,
    synapse_table: Optional[str] = None,
    remove_self_synapse: bool = True,
    synapse_reference_tables: dict = {},
    live_query: bool = False,
    timestamp: Optional[datetime.datetime] = None,
    invalidation_d: Numeric = DEFAULT_INVALIDATION_D,
    require_complete: bool = False,
    metadata: bool = False,
    synapse_partners: bool = False,
    synapse_point_resolution: list = [1, 1, 1],
    synapse_representative_point_pre: str = "ctr_pt_position",
    synapse_representative_point_post: str = "ctr_pt_position",
    level2_graph: Optional[np.ndarray] = None,
) -> meshwork.Meshwork:
    """Generate a meshwork file based on the level 2 graph.

    Parameters
    ----------
    root_id : int
        Root id of an object in the pychunkedgraph.
    datastack_name : str or None, optional
        Datastack name to use to initialize a client, if none is provided. By default None.
    client : caveclient.CAVEclientFull or None, optional
        Initialized CAVE client. If None is given, will use the datastack_name to create one. By default None
    cv : cloudvolume.CloudVolume or None, optional
        Initialized cloudvolume. If none is given, the client info will be used to create one. By default None
    root_point : array-like or None, optional
        3 element xyz location for the location to set the root in units set by root_point_resolution,
        by default None. If None, a distal tip is selected.
    root_point_resolution : array-like, optional
        Resolution in euclidean space of the root_point, by default [4, 4, 40]
    collapse_soma : bool, optional,
        If True, collapses vertices within a given radius of the root point into the root vertex, typically to better
        represent primary neurite branches. Requires a specified root_point. Default if False.
    collapse_radius : float, optional
        Max distance in euclidean space for soma collapse. Default is 10,000 nm (10 microns).
    synapses : 'pre', 'post', 'all', True, or None, optional
        If not None, queries the synapse_table for presynaptic synapses (if 'pre'),  postsynaptic sites (if 'post'), or both (if 'all' or True). By default None
    synapse_table : str, optional
        Name of the synapse table to query if synapses are requested, by default None
    remove_self_synapse : bool, optional
        If True, filters out synapses whose pre- and postsynaptic root ids are the same neuron, by default True
    synapse_reference_tables : dict, optional
        A dictionary of synapse reference tables to attach to synapses.
        Keys are 'pre' and 'post', values are table names. Defaults to {}.
    live_query : bool, optional
        If True, expect a timestamp for querying at a give point in time. Otherwise, use the materializatio set by the client. Optional, by default False.
    timestamp = datetime.datetime, optional
        If set, acts as the time at which all root ids and annotations are found at.
    invalidation_d : int, optional
        Invalidation radius in hops for the mesh skeletonization along the chunk adjacency graph, by default 3
    require_complete : bool, optional
        If True, raise an Exception if any vertices are missing from the cache, by default False
    metadata : bool, optional
        If True, adds metadata to the meshwork annotations. By default False.
    synapse_partners : bool, optional
        If True, includes the partner root id to the synapse annotation. By default False, because partner roots can change across time.
    synapse_point_resolution : array-like, optional
        Resolution in euclidean space of the synapse points, by default None. If None, the resolution will be the default of the synapse table.
    synapse_representative_point_pre : str, optional
        If set, uses the specified column in the synapse table for the pre-synaptic points. By default 'ctr_pt_position'.
    synapse_representative_point_post : str, optional
        If set, uses the specified column in the synapse table for the post-synaptic points. By default 'ctr_pt_position'.

    Returns
    -------
    meshparty.meshwork.Meshwork
        Meshwork object with skeleton based on the level 2 graph. See documentation for details.
    """
    if client is None:
        client = CAVEclient(datastack_name)
    if root_point_resolution is None and root_point is not None:
        if cv is None:
            cv = client.info.segmentation_cloudvolume(progress=False, parallel=1)
        if cv is None:
            raise ValueError(
                "Must provide either a client or cv object to get the root_point_resolution."
            )
        root_point_resolution = cv.mip_resolution(0)
    if synapse_table is None and synapses is not None:
        synapse_table = client.materialize.synapse_table

    sk, mesh, (l2dict_mesh, l2dict_mesh_r) = pcg_skeleton(
        root_id,
        client=client,
        cv=cv,
        root_point=root_point,
        root_point_resolution=root_point_resolution,
        collapse_soma=collapse_soma,
        collapse_radius=collapse_radius,
        invalidation_d=invalidation_d,
        return_mesh=True,
        return_l2dict_mesh=True,
        require_complete=require_complete,
        level2_graph=level2_graph,
    )

    nrn = meshwork.Meshwork(mesh, seg_id=root_id, skeleton=sk)
    print("pcg_meshwork() nrn.skeleton.vertex_properties: ", nrn.skeleton.vertex_properties)

    pre, post = False, False
    if synapses is not None and synapse_table is not None:
        if synapses == "pre":
            pre, post = True, False
        elif synapses == "post":
            pre, post = False, True
        elif synapses == "all" or synapses is True:
            pre, post = True, True
        else:
            raise ValueError('Synapses must be one of "pre", "post", or "all" or True.')

        if not timestamp:
            timestamp = client.materialize.get_timestamp()

        features.add_synapses(
            nrn,
            synapse_table,
            l2dict_mesh,
            client,
            root_id=root_id,
            pre=pre,
            post=post,
            remove_self_synapse=remove_self_synapse,
            timestamp=timestamp,
            live_query=live_query,
            metadata=metadata,
            synapse_partners=synapse_partners,
            synapse_point_resolution=synapse_point_resolution,
            synapse_representative_point_pre=synapse_representative_point_pre,
            synapse_representative_point_post=synapse_representative_point_post,
            synapse_reference_tables=synapse_reference_tables,
        )

    features.add_lvl2_ids(nrn, l2dict_mesh)
    return nrn








from scipy import sparse, spatial, optimize, signal
import numpy as np
import time
from meshparty import utils

try:
    from pykdtree.kdtree import KDTree
except:
    KDTree = spatial.cKDTree
from tqdm import tqdm
from skeletonservice.datasets.skeleton_from_meshparty import Skeleton

import fastremap
import logging


def skeletonize_mesh(
    mesh,
    soma_pt=None,
    soma_radius=7500,
    collapse_soma=True,
    collapse_function="sphere",
    invalidation_d=12000,
    smooth_vertices=False,
    compute_radius=True,
    shape_function="single",
    compute_original_index=True,
    verbose=True,
    smooth_iterations=12,
    smooth_neighborhood=2,
    smooth_r=0.1,
    cc_vertex_thresh=100,
    root_index=None,
    remove_zero_length_edges=True,
    collapse_params={},
    meta={},
):
    """
    Build skeleton object from mesh skeletonization

    Parameters
    ----------
    mesh: meshparty.trimesh_io.Mesh
        the mesh to skeletonize, defaults assume vertices in nm
    soma_pt: np.array
        a length 3 array specifying to soma location to make the root
        default=None, in which case a heuristic root will be chosen
        in units of mesh vertices.
    soma_radius: float
        distance in mesh vertex units over which to consider mesh
        vertices close to soma_pt to belong to soma
        these vertices will automatically be invalidated and no
        skeleton branches will attempt to reach them.
        This distance will also be used to collapse all skeleton
        points within this distance to the soma_pt root if collpase_soma
        is true. (default=7500 (nm))
    collapse_soma: bool
        whether to collapse the skeleton around the soma point (default True)
    collapse_function: 'sphere' or 'branch'
        Determines which soma collapse function to use. Sphere uses the soma_radius
        and collapses all vertices within that radius to the soma. Branch is an experimental
        approach that tries to compute the right boundary for each branch into soma.
    invalidation_d: float
        the distance along the mesh to invalidate when applying TEASAR
        like algorithm.  Controls how detailed a structure the skeleton
        algorithm reaches. default (12000 (nm))
    smooth_vertices: bool
        whether to smooth the vertices of the skeleton
    compute_radius: bool
        whether to calculate the radius of the skeleton at each point on the skeleton
        (default True)
    shape_function: 'single' or 'cone'
        Selects how to compute the radius, either with a single ray or a cone of rays. Default is 'single'.
    compute_original_index: bool
        whether to calculate how each of the mesh nodes maps onto the skeleton
        (default True)
    smooth_iterations: int, optional
        Number of iterations to smooth (default is 12)
    smooth_neighborhood: int, optional
        Size of neighborhood to look at for smoothing
    smooth_r: float, optional
        Weight of update step in smoothing algorithm, default is 0.2
    root_index: int or None, optional
        A precise mesh vertex to use as the skeleton root. If provided, the vertex location overrides soma_pt. By default, None.
    remove_zero_length_edges: bool
        If True, removes vertices involved in zero length edges, which can disrupt graph computations. Default True.
    collapse_params: dict
        Extra keyword arguments for the collapse function. See soma_via_sphere and soma_via_branch_starts for specifics.
    cc_vertex_thresh : int, optional
        Smallest number of vertices in a connected component to skeletonize.
    verbose: bool
        whether to print verbose logging
    meta: dict
        Skeletonization metadata to add to the skeleton. See skeleton.SkeletonMetadata for keys.

    Returns
    -------
    :obj:`meshparty.skeleton.Skeleton`
           a Skeleton object for this mesh
    """
    (
        skel_verts,
        skel_edges,
        orig_skel_index,
        skel_map,
    ) = skeletonize.calculate_skeleton_paths_on_mesh(
        mesh,
        invalidation_d=invalidation_d,
        cc_vertex_thresh=cc_vertex_thresh,
        root_index=root_index,
        return_map=True,
    )

    if smooth_vertices is True:
        smooth_verts = skeletonize.smooth_graph(
            skel_verts,
            skel_edges,
            neighborhood=smooth_neighborhood,
            iterations=smooth_iterations,
            r=smooth_r,
        )
        skel_verts = smooth_verts

    if root_index is not None and soma_pt is None:
        soma_pt = mesh.vertices[root_index]

    if soma_pt is not None:
        soma_pt = np.array(soma_pt).reshape(1, 3)

    rs = None

    if collapse_soma is True and soma_pt is not None:
        temp_sk = Skeleton(
            skel_verts,
            skel_edges,
            mesh_index=mesh.map_indices_to_unmasked(orig_skel_index),
            mesh_to_skel_map=skel_map,
        )
        _, close_ind = temp_sk.kdtree.query(soma_pt)
        temp_sk.reroot(close_ind[0])

        if collapse_function == "sphere":
            soma_verts, soma_r = skeletonize.soma_via_sphere(
                soma_pt, temp_sk.vertices, temp_sk.edges, soma_radius
            )
        elif collapse_function == "branch":
            try:
                from .ray_tracing import ray_trace_distance, shape_diameter_function
            except:
                raise ImportError('Could not import pyembree for ray tracing')

            if shape_function == "single":
                rs = ray_trace_distance(
                    mesh.filter_unmasked_indices_padded(temp_sk.mesh_index), mesh
                )
            elif shape_function == "cone":
                rs = shape_diameter_function(
                    mesh.filter_unmasked_indices_padded(temp_sk.mesh_index),
                    mesh,
                    num_points=30,
                    cone_angle=np.pi / 3,
                )

            soma_verts, soma_r = skeletonize.soma_via_branch_starts(
                temp_sk,
                mesh,
                soma_pt,
                rs,
                search_radius=collapse_params.get("search_radius", 25000),
                fallback_radius=collapse_params.get("fallback_radius", soma_radius),
                cutoff_threshold=collapse_params.get("cutoff_threshold", 0.4),
                min_cutoff=collapse_params.get("min_cutoff", 0.1),
                dynamic_range=collapse_params.get("dynamic_range", 1),
                dynamic_threshold=collapse_params.get("dynamic_threshold", False),
            )
        if root_index is not None:
            collapse_index = np.flatnonzero(orig_skel_index == root_index)[0]
        else:
            collapse_index = None
        new_v, new_e, new_skel_map, vert_filter, root_ind = skeletonize.collapse_soma_skeleton(
            soma_verts,
            soma_pt,
            temp_sk.vertices,
            temp_sk.edges,
            mesh_to_skeleton_map=temp_sk.mesh_to_skel_map,
            collapse_index=collapse_index,
            return_filter=True,
            return_soma_ind=True,
        )
    else:
        new_v, new_e, new_skel_map = skel_verts, skel_edges, skel_map
        vert_filter = np.arange(len(orig_skel_index))
        if root_index is not None:
            root_ind = np.flatnonzero(orig_skel_index == root_index)[0]
        elif soma_pt is None:
            sk_graph = utils.create_csgraph(new_v, new_e)
            root_ind = utils.find_far_points_graph(sk_graph)[0]
        else:
            # Still try to root close to the soma
            _, qry_inds = spatial.cKDTree(new_v, balanced_tree=False).query(
                soma_pt[np.newaxis, :]
            )
            root_ind = qry_inds[0]

    skel_map_full_mesh = np.full(mesh.node_mask.shape, -1, dtype=np.int64)
    skel_map_full_mesh[mesh.node_mask] = new_skel_map
    ind_to_fix = mesh.map_boolean_to_unmasked(np.isnan(new_skel_map))
    skel_map_full_mesh[ind_to_fix] = -1

    props = {}

    if compute_original_index is True:
        if collapse_soma is True and soma_pt is not None:
            mesh_index = temp_sk.mesh_index[vert_filter]
            if root_index is None:
                mesh_index = np.append(mesh_index, -1)
        else:
            mesh_index = orig_skel_index[vert_filter]
        props["mesh_index"] = mesh_index

    if compute_radius is True:
        if rs is None:
            try:
                from .ray_tracing import ray_trace_distance, shape_diameter_function
            except:
                raise ImportError('Could not import pyembree for ray tracing')

            if shape_function == "single":
                rs = ray_trace_distance(orig_skel_index[vert_filter], mesh)
            elif shape_function == "cone":
                rs = shape_diameter_function(orig_skel_index[vert_filter], mesh)
        else:
            rs = rs[vert_filter]
        if collapse_soma is True and soma_pt is not None:
            if root_index is None:
                rs = np.append(rs, soma_r)
            else:
                rs[root_ind] = soma_r
        props["rs"] = rs

    sk_params = {
        "soma_pt_x": soma_pt[0, 0] if soma_pt is not None else None,
        "soma_pt_y": soma_pt[0, 1] if soma_pt is not None else None,
        "soma_pt_z": soma_pt[0, 2] if soma_pt is not None else None,
        "soma_radius": soma_radius,
        "collapse_soma": collapse_soma,
        "collapse_function": collapse_function,
        "invalidation_d": invalidation_d,
        "smooth_vertices": smooth_vertices,
        "compute_radius": compute_radius,
        "shape_function": shape_function,
        "smooth_iterations": smooth_iterations,
        "smooth_neighborhood": smooth_neighborhood,
        "smooth_r": smooth_r,
        "cc_vertex_thresh": cc_vertex_thresh,
        "remove_zero_length_edges": remove_zero_length_edges,
        "collapse_params": collapse_params,
        "timestamp": time.time(),
    }
    sk_params.update(meta)

    sk = Skeleton(
        new_v,
        new_e,
        mesh_to_skel_map=skel_map_full_mesh,
        mesh_index=props.get("mesh_index", None),
        radius=props.get("rs", None),
        root=root_ind,
        remove_zero_length_edges=remove_zero_length_edges,
        meta=sk_params,
    )
    print("skeletonize_mesh() sk.vertex_properties: ", sk.vertex_properties)

    if compute_radius is True:
        skeletonize._remove_nan_radius(sk)

    return sk
