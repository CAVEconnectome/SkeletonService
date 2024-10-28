from typing import List
from skeletonservice.datasets.service import SkeletonService


class SkeletonService_skvn3(SkeletonService):
    def __init__(self):
        super().__init__()
        print("SkeletonService_skvn3 initialized")
    
    @staticmethod
    def get_cache_contents(
        bucket: str,
        skeleton_version: int = -1,
        rid_prefix: int = None,
        limit: int=None,
        verbose_level_: int = 0,
    ):
        print(f"SkeletonService_skvn3.get_cache_contents: {bucket} {skeleton_version} {rid_prefix} {limit}")
        return SkeletonService.get_cache_contents(
            bucket,
            skeleton_version,
            rid_prefix,
            limit,
            verbose_level_,
        )

    @staticmethod
    def get_skeleton_by_datastack_and_rid(
        datastack_name: str,
        rid: int,
        output_format: str,
        bucket: str,
        root_resolution: List,
        collapse_soma: bool,
        collapse_radius: int,
        skeleton_version: int = -1,
        via_requests: bool = True,
        verbose_level_: int = 0,
    ):
        print(f"SkeletonService_skvn3.get_skeleton_by_datastack_and_rid: {datastack_name} {rid} {output_format} {bucket}")
        return SkeletonService.get_skeleton_by_datastack_and_rid(
            datastack_name,
            rid,
            output_format,
            bucket,
            root_resolution,
            collapse_soma,
            collapse_radius,
            skeleton_version,
            via_requests,
            verbose_level_,
        )

    @staticmethod
    def get_bulk_skeletons_by_datastack_and_rids(
        datastack_name: str,
        rids: List,
        bucket: str,
        root_resolution: List,
        collapse_soma: bool,
        collapse_radius: int,
        skeleton_version: int = -1,
        output_format: str = "json",
        generate_missing_skeletons: bool = False,
        verbose_level_: int = 0,
    ):
        print(f"SkeletonService_skvn3.get_skeleton_by_datastack_and_rid: {datastack_name} {rids} {bucket}")
        return SkeletonService.get_bulk_skeletons_by_datastack_and_rids(
            datastack_name,
            rids,
            bucket,
            root_resolution,
            collapse_soma,
            collapse_radius,
            skeleton_version,
            output_format,
            generate_missing_skeletons,
            verbose_level_,
        )

    @staticmethod
    def generate_bulk_skeletons_by_datastack_and_rids_without_retrieval(
        datastack_name: str,
        rids: List,
        bucket: str,
        root_resolution: List,
        collapse_soma: bool,
        collapse_radius: int,
        skeleton_version: int = -1,
        verbose_level_: int = 0,
    ):
        print(f"SkeletonService_skvn3.generate_bulk_skeletons_by_datastack_and_rids_without_retrieval: {datastack_name} {rids} {bucket}")
        return SkeletonService.generate_bulk_skeletons_by_datastack_and_rids_without_retrieval(
            datastack_name,
            rids,
            bucket,
            root_resolution,
            collapse_soma,
            collapse_radius,
            skeleton_version,
            verbose_level_,
        )
