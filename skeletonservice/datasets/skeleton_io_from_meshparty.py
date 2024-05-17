'''
This entire file consists of pieces grabed from meshparty.skeleton_io.py in order to remove the dependency on meshparty
and subsequently enable me to make some minor modifications to the code.
'''

import os
import numpy as np
import json
import h5py
import orjson
from dataclasses import asdict
from meshparty import skeleton

FILE_VERSION = 2

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(
            obj,
            (
                np.int_,
                np.intc,
                np.intp,
                np.int8,
                np.int16,
                np.int32,
                np.int64,
                np.uint8,
                np.uint16,
                np.uint32,
                np.uint64,
            ),
        ):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)

class SkeletonIO:

#==================================================================================================
#==================================================================================================
#==================================================================================================
# H5 Import

    @staticmethod
    def _convert_keys_to_int(x):
        '''
        Adapted from meshparty.skeleton_io._convert_keys_to_int()
        '''
        if isinstance(x, dict):
            return {int(k): v for k, v in x.items()}
        return x
    
    @staticmethod
    def _read_skeleton_h5_by_part(filename):
        '''
        Adapted from meshparty.skeleton_io.read_skeleton_h5_by_part()
        '''
        if isinstance(filename, str):
            assert os.path.isfile(filename)

        with h5py.File(filename, "r") as f:
            vertices = f["vertices"][()]
            edges = f["edges"][()]

            if "mesh_to_skel_map" in f.keys():
                mesh_to_skel_map = f["mesh_to_skel_map"][()]
            else:
                mesh_to_skel_map = None

            vertex_properties = {}
            if "vertex_properties" in f.keys():
                for vp_key in f["vertex_properties"].keys():
                    vertex_properties[vp_key] = json.loads(
                        f["vertex_properties"][vp_key][()], object_hook=SkeletonIO._convert_keys_to_int
                    )

            if "meta" in f.keys():
                dat = f["meta"][()].tobytes()
                meta = orjson.loads(dat)
            else:
                meta = {}

            if "root" in f.keys():
                root = f["root"][()]
            else:
                root = None

        return vertices, edges, meta, mesh_to_skel_map, vertex_properties, root
    
    @staticmethod
    def read_skeleton_h5(filename, remove_zero_length_edges=False):
        '''
        Adapted from meshparty.skeleton_io.read_skeleton_h5()
        '''
        # return skeleton_io.read_skeleton_h5(filename)
        (
            vertices,
            edges,
            meta,
            mesh_to_skel_map,
            vertex_properties,
            root,
        ) = SkeletonIO._read_skeleton_h5_by_part(filename)
        return skeleton.Skeleton(
            vertices=vertices,
            edges=edges,
            mesh_to_skel_map=mesh_to_skel_map,
            vertex_properties=vertex_properties,
            root=root,
            remove_zero_length_edges=remove_zero_length_edges,
            meta=meta,
        )

#==================================================================================================
#==================================================================================================
#==================================================================================================
# H5 Export

    @staticmethod
    def _write_dict_to_group(f, group_name, data_dict):
        '''
        Adapted from meshparty.skeleton_io._write_dict_to_group()
        '''
        d_grp = f.create_group(group_name)
        for d_name, d_data in data_dict.items():
            d_grp.create_dataset(d_name, data=json.dumps(d_data, cls=NumpyEncoder))

    @staticmethod
    def _write_skeleton_h5_by_part(
        filename,
        vertices,
        edges,
        meta,
        mesh_to_skel_map=None,
        vertex_properties={},
        root=None,
        overwrite=False,
    ):
        '''
        Adapted from meshparty.skeleton_io._write_skeleton_h5_by_part()
        '''
        if os.path.isfile(filename):
            if overwrite:
                os.remove(filename)
            else:
                return
            with h5py.File(filename, "w") as f:
                f.attrs["file_version"] = FILE_VERSION

                f.create_dataset("vertices", data=vertices, compression="gzip")
                f.create_dataset("edges", data=edges, compression="gzip")
                f.create_dataset(
                    "meta",
                    data=np.string_(
                        orjson.dumps(asdict(meta), option=orjson.OPT_SERIALIZE_NUMPY)
                    ),
                )
                if mesh_to_skel_map is not None:
                    f.create_dataset(
                        "mesh_to_skel_map", data=mesh_to_skel_map, compression="gzip"
                    )
                if len(vertex_properties) > 0:
                    SkeletonIO._write_dict_to_group(f, "vertex_properties", vertex_properties)
                if root is not None:
                    f.create_dataset("root", data=root)
    
    @staticmethod
    def write_skeleton_h5(sk, filename, overwrite=False):
        '''
        Adapted from meshparty.skeleton_io.write_skeleton_h5()
        '''
        # return skeleton_io.write_skeleton_h5(skeleton, file_name)
        if not hasattr(sk, "meta"):
            sk.meta = {}

        SkeletonIO._write_skeleton_h5_by_part(
            filename,
            vertices=sk.vertices,
            edges=sk.edges,
            meta=sk.meta,
            mesh_to_skel_map=sk.mesh_to_skel_map,
            vertex_properties=sk.vertex_properties,
            root=sk.root,
            overwrite=overwrite,
        )


#==================================================================================================
#==================================================================================================
#==================================================================================================
# SWC Export
    
    @staticmethod
    def _build_swc_array(skel, node_labels, radius, xyz_scaling):
        '''
        Adapted from meshparty.skeleton_io._build_swc_array()
        '''
        order_old = np.concatenate([p[::-1] for p in skel.cover_paths])
        new_ids = np.arange(skel.n_vertices)
        order_map = dict(zip(order_old, new_ids))

        node_labels = np.array(node_labels)[order_old]
        xyz = skel.vertices[order_old]
        radius = radius[order_old]
        par_ids = np.array([order_map.get(nid, -1) for nid in skel.parent_nodes(order_old)])

        swc_dat = np.hstack(
            (
                new_ids[:, np.newaxis],
                node_labels[:, np.newaxis],
                xyz / xyz_scaling,
                radius[:, np.newaxis] / xyz_scaling,
                par_ids[:, np.newaxis],
            )
        )
        return swc_dat

    @staticmethod
    def export_to_swc(
        skel,
        filename,
        node_labels=None,
        radius=None,
        header=None,
        xyz_scaling=1000,
        resample_spacing=None,
        interp_kind="linear",
        tip_length_ratio=0.5,
        avoid_root=True,
    ):
        '''
        Adapted from meshparty.skeleton_io.export_to_swc()
        '''
        # return skeleton_io.export_to_swc(skeleton, file_name)    if header is None:
        if header is None:
            header_string = ""
        else:
            if isinstance(header, str):
                header = [header]
            header[0] = " ".join(["#", header[0]])
            header_string = "\n# ".join(header)

        if radius is None:
            radius = np.full(len(skel.vertices), xyz_scaling)
        elif np.issubdtype(type(radius), int):
            radius = np.full(len(skel.vertices), radius)

        if node_labels is None:
            node_labels = np.full(len(skel.vertices), 0)

        if resample_spacing is not None:
            skel, output_map = skeleton.resample(
                skel,
                spacing=resample_spacing,
                tip_length_ratio=tip_length_ratio,
                kind=interp_kind,
                avoid_root=avoid_root,
            )
            node_labels = node_labels[output_map]
            radius = radius[output_map]

        swc_dat = SkeletonIO._build_swc_array(skel, node_labels, radius, xyz_scaling)

        np.savetxt(
            filename,
            swc_dat,
            delimiter=" ",
            header=header_string,
            comments="#",
            fmt=["%i", "%i", "%.3f", "%.3f", "%.3f", "%.3f", "%i"],
        )
