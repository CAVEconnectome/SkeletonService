from typing import List
from skeletonservice.datasets.service import SkeletonService


class SkeletonService_skvn2(SkeletonService):
    def __init__(self):
        super().__init__()
        print("SkeletonService_skvn2 initialized")

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
        skeleton_version: int = -1,
        verbose_level_: int = 0,
    ):
        print("SkeletonService_skvn2.get_skeleton_by_datastack_and_rid")
        return SkeletonService.get_skeleton_by_datastack_and_rid(
            datastack_name,
            rid,
            output_format,
            bucket,
            root_resolution,
            collapse_soma,
            collapse_radius,
            skeleton_version,
            verbose_level_,
        )
    