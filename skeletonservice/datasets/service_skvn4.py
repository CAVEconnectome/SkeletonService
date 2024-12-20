from typing import List, Union
from skeletonservice.datasets.service import SkeletonService


class SkeletonService_skvn4(SkeletonService):
    def __init__(self):
        super().__init__()
        print("SkeletonService_skvn4 initialized")
    
    @staticmethod
    def get_cache_contents(
        bucket: str,
        skeleton_version: int = -1,
        rid_prefixes: List = None,
        limit: int = None,
        verbose_level_: int = 0,
    ):
        print(f"SkeletonService_skvn4.get_cache_contents: {bucket} {skeleton_version} {rid_prefixes} {limit}")
        return SkeletonService.get_cache_contents(
            bucket,
            skeleton_version,
            rid_prefixes,
            limit,
            verbose_level_,
        )
    
    @staticmethod
    def skeletons_exist(
        bucket: str,
        skeleton_version: int = -1,
        rids: Union[List, int] = None,
        verbose_level_: int = 0,
    ):
        print(f"SkeletonService_skvn4.skeletons_exist: {bucket} {skeleton_version} {rids}")
        return SkeletonService.skeletons_exist(
            bucket,
            skeleton_version,
            rids,
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
        print(f"SkeletonService_skvn4.get_skeleton_by_datastack_and_rid: {datastack_name} {rid} {output_format} {bucket}")
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
        print(f"SkeletonService_skvn4.get_skeleton_by_datastack_and_rid: {datastack_name} {rids} {bucket}")
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
        print(f"SkeletonService_skvn4.generate_bulk_skeletons_by_datastack_and_rids_without_retrieval: {datastack_name} {rids} {bucket}")
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