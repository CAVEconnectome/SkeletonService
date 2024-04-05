from typing import List, Dict
import pandas as pd
from meshparty import skeleton
import skeleton_plot.skel_io as skel_io

from skeletonservice.datasets.models import (
    Skeleton,
)
from skeletonservice.datasets.schemas import (
    SkeletonSchema,
)

class SkeletonService:
    @staticmethod
    def get_all() -> List[Skeleton]:
        return [{"name": "Skeleton #1"}]  # Skeleton.query.all()

    @staticmethod
    def get_skeleton_by_rid_sid(rid: int, sid: int) -> Dict:
        # return Skeleton.query.filter_by(name=skeleton_name).first_or_404()

        rid = 864691135926952148 if rid == 0 else rid # v661: 864691135926952148, current: 864691135701676411
        sid = 294657 if sid == 0 else sid # nucleus_id  # Nucleus id

        skel_path = "https://storage.googleapis.com/allen-minnie-phase3/minniephase3-emily-pcg-skeletons/minnie_all/v661/skeletons/"
        # dir_name, file_name = skel_path + f"{rid}_{sid}", f"{rid}_{sid}.swc"
        dir_name, file_name = skel_path, f"{rid}_{sid}.swc"
        
        sk = skel_io.read_skeleton(dir_name, file_name)
        
        return {
            "n_branch_points": sk.n_branch_points,
            "n_end_points": sk.n_end_points,
            "n_vertices": sk.n_vertices
        }
