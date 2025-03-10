import io
from unittest.mock import patch
import responses

from skeletonservice.datasets.service import SkeletonService
from cloudfiles import CloudFiles
from messagingclient import MessagingClient
from caveclient import CAVEclient, endpoints

from .conftest import (
    datastack_dict,
    test_info,
)

info_mapping = {
    "i_server_address": datastack_dict["global_server"],
    "datastack_name": datastack_dict["datastack_name"],
}
url_template = endpoints.infoservice_endpoints_v2["datastack_info"]
info_url = url_template.format_map(info_mapping)

class TestSkeletonsService:
    def test_create_versioned_skeleton_service(self, test_app):
        SkelClassVsn = SkeletonService.get_version_specific_handler(1)
        assert SkelClassVsn.__name__ == "SkeletonService_skvn1"

        SkelClassVsn = SkeletonService.get_version_specific_handler(2)
        assert SkelClassVsn.__name__ == "SkeletonService_skvn2"

        SkelClassVsn = SkeletonService.get_version_specific_handler(3)
        assert SkelClassVsn.__name__ == "SkeletonService_skvn3"

        SkelClassVsn = SkeletonService.get_version_specific_handler(4)
        assert SkelClassVsn.__name__ == "SkeletonService_skvn4"

    def test_bytes_compression(self, test_app):
        SkelClassVsn = SkeletonService.get_version_specific_handler(4)

        data = b"test"
        data_compressed = SkelClassVsn.compressBytes(io.BytesIO(data))
        data_decompressed = SkelClassVsn.decompressBytes(data_compressed)
        assert data_decompressed == data

    def test_string_compression(self, test_app):
        SkelClassVsn = SkeletonService.get_version_specific_handler(4)

        data = "test"
        data_compressed = SkelClassVsn.compressStringToBytes(data)
        data_decompressed = SkelClassVsn.decompressBytesToString(data_compressed)
        assert data_decompressed == data

    def test_dict_compression(self, test_app):
        SkelClassVsn = SkeletonService.get_version_specific_handler(4)

        data = {"a": 1, "b": 2}
        data_compressed = SkelClassVsn.compressDictToBytes(data)
        data_decompressed = SkelClassVsn.decompressBytesToDict(data_compressed)
        assert data_decompressed == data

    def test_get_cache_contents(self, test_app, cloudfiles_mock):
        rid_prefix = "a"
        skeleton_version = 4
        prefix = f"skeleton__v{skeleton_version}__rid-{rid_prefix}"
        filename = f"{prefix}.h5.gz"

        patch.object(CloudFiles, "list", return_value=[filename]).start()

        SkelClassVsn = SkeletonService.get_version_specific_handler(4)

        cache_contents = SkelClassVsn.get_cache_contents(
            bucket="gs://test_bucket",
            skeleton_version=4,
            rid_prefixes=[rid_prefix],
        )
        assert cache_contents == {
            "num_found": 1,
            "files": [filename]
        }

    def test_meshworks_exist(self, test_app, cloudfiles_mock):
        rids = [1]
        results_mock = {"rid-1__ds": True}

        patch.object(CloudFiles, "exists", return_value=results_mock).start()

        SkelClassVsn = SkeletonService.get_version_specific_handler(4)

        results = SkelClassVsn.meshworks_exist(
            bucket="gs://test_bucket",
            rids=rids
        )
        assert results == {
            1: True,
        }

    def test_skeletons_exist(self, test_app, cloudfiles_mock):
        rids = [1]
        results_mock = {"rid-1__ds": True}

        patch.object(CloudFiles, "exists", return_value=results_mock).start()

        SkelClassVsn = SkeletonService.get_version_specific_handler(4)

        results = SkelClassVsn.skeletons_exist(
            bucket="gs://test_bucket",
            rids=rids
        )
        assert results == {
            1: True,
        }
        pass

    def test_publish_skeleton_request(self, test_app, messagingclient_mock):
        patch.object(MessagingClient, "publish", return_value=None).start()

        SkelClassVsn = SkeletonService.get_version_specific_handler(4)

        SkelClassVsn.publish_skeleton_request(
            datastack_name=datastack_dict["datastack_name"],
            rid=1,
            output_format="none",
            bucket="gs://test_bucket",
            root_resolution=[1, 1, 1],
            collapse_soma=True,
            collapse_radius=7500,
            skeleton_version=4,
            high_priority=False,
        )

        # There is no return value, so just assert True, i.e., that the test reached this point without crashing or anything like that.
        # A more thorough test would confirm that a message is inserted into a PubSub queue, but two points:
        # (1) That would be a much more complicated test to write so I will put it off for now.
        # (2) There would be no point in mocking such behavior since the test would be the successful insertion into a true PubSub queue.
        #     As such, it would be more of an integration test than a unit test.
        assert True

    @responses.activate
    def test_get_skeleton_by_datastack_and_rid(self, test_app,
            caveclient_mock, cloudvolume_mock, cloudfiles_mock):
        responses.add(responses.GET, url=info_url, json=test_info, status=200)

        patch.object(CAVEclient, "__new__", return_value=caveclient_mock).start()

        patch.object(caveclient_mock.l2cache, "has_cache", return_value=True).start()
        patch.object(caveclient_mock.chunkedgraph, 'get_roots', return_value=[1, 2, 3]).start()
        patch.object(caveclient_mock.chunkedgraph, 'is_valid_nodes', return_value=True).start()
        patch.object(caveclient_mock.info, 'segmentation_cloudvolume', return_value=cloudvolume_mock).start()

        def exists_side_effect(*args, **kwargs):
            # Customize the return value based on the function's parameters
            if args[0] == "skeleton__v4__rid-1__ds-test_stack__res-1x1x1__cs-True__cr-7500.h5.gz":
                return {"rid-1__ds": True}
            elif args[0] == "skeletonization_refusal_root_ids.txt":
                return False
        patch.object(CloudFiles, "exists", side_effect=exists_side_effect).start()

        skvn = 4
        SkelClassVsn = SkeletonService.get_version_specific_handler(skvn)

        sk = SkelClassVsn.get_skeleton_by_datastack_and_rid(
            datastack_name=datastack_dict["datastack_name"],
            rid=1,
            output_format="none",
            bucket="gs://test_bucket",
            root_resolution=[1, 1, 1],
            collapse_soma=True,
            collapse_radius=7500,
            skeleton_version=skvn,  # The default skeleton version is 0, the Neuroglancer compatible version, not -1, the latest version, for backward compatibility
            via_requests=False,
        )

        # This test will return a None skeleton both because the requested output format is "none"
        # and because the cache lookup will return False.
        assert sk is None

    @responses.activate
    def test_get_meshwork_by_datastack_and_rid_async(self, test_app, caveclient_mock, cloudvolume_mock):
        responses.add(responses.GET, url=info_url, json=test_info, status=200)
        
        patch.object(CAVEclient, "__new__", return_value=caveclient_mock).start()

        patch.object(caveclient_mock.l2cache, "has_cache", return_value=True).start()
        patch.object(caveclient_mock.chunkedgraph, 'get_roots', return_value=[1, 2, 3]).start()
        patch.object(caveclient_mock.chunkedgraph, 'is_valid_nodes', return_value=True).start()
        patch.object(caveclient_mock.info, 'segmentation_cloudvolume', return_value=cloudvolume_mock).start()

        def exists_side_effect(*args, **kwargs):
            # Customize the return value based on the function's parameters
            if args[0] == ['meshwork__v1__rid-1__ds-minnie65_phase3_v1__res-1x1x1__cs-True__cr-7500.h5.gz']:
                return {"rid-1__ds": True}
            elif args[0] == "skeletonization_refusal_root_ids.txt":
                return False
        patch.object(CloudFiles, "exists", side_effect=exists_side_effect).start()

        patch.object(SkeletonService, "get_skeleton_by_datastack_and_rid", return_value=None).start()

        SkelClassVsn = SkeletonService.get_version_specific_handler(4)

        mw = SkelClassVsn.get_meshwork_by_datastack_and_rid_async(
            datastack_name=datastack_dict["datastack_name"],
            rid=1,
            bucket="gs://test_bucket",
            root_resolution=[1, 1, 1],
            collapse_soma=True,
            collapse_radius=7500,
        )

        # This test patches away the actual call to get_skeleton_by_datastack_and_rid(), so it will return None.
        assert mw is None

    @responses.activate
    def test_get_skeleton_by_datastack_and_rid_async(self, test_app, caveclient_mock, cloudvolume_mock):
        responses.add(responses.GET, url=info_url, json=test_info, status=200)
        
        patch.object(CAVEclient, "__new__", return_value=caveclient_mock).start()

        patch.object(caveclient_mock.l2cache, "has_cache", return_value=True).start()
        patch.object(caveclient_mock.chunkedgraph, 'get_roots', return_value=[1, 2, 3]).start()
        patch.object(caveclient_mock.chunkedgraph, 'is_valid_nodes', return_value=True).start()
        patch.object(caveclient_mock.info, 'segmentation_cloudvolume', return_value=cloudvolume_mock).start()

        def exists_side_effect(*args, **kwargs):
            # Customize the return value based on the function's parameters
            if args[0] == ['skeleton__v4__rid-1__ds-minnie65_phase3_v1__res-1x1x1__cs-True__cr-7500.h5.gz']:
                return {"rid-1__ds": True}
            elif args[0] == "skeletonization_refusal_root_ids.txt":
                return False
        patch.object(CloudFiles, "exists", side_effect=exists_side_effect).start()

        patch.object(SkeletonService, "get_skeleton_by_datastack_and_rid", return_value=None).start()

        skvn = 4
        SkelClassVsn = SkeletonService.get_version_specific_handler(skvn)

        sk = SkelClassVsn.get_skeleton_by_datastack_and_rid_async(
            datastack_name=datastack_dict["datastack_name"],
            rid=1,
            output_format="none",
            bucket="gs://test_bucket",
            root_resolution=[1, 1, 1],
            collapse_soma=True,
            collapse_radius=7500,
            skeleton_version=skvn,
        )

        # This test patches away the actual call to get_skeleton_by_datastack_and_rid(), so it will return None.
        assert sk is None

    def test_generate_meshworks_bulk_by_datastack_and_rids_async(self, test_app, caveclient_mock, cloudvolume_mock):
        responses.add(responses.GET, url=info_url, json=test_info, status=200)
        
        patch.object(CAVEclient, "__new__", return_value=caveclient_mock).start()

        patch.object(caveclient_mock.l2cache, "has_cache", return_value=True).start()
        patch.object(caveclient_mock.chunkedgraph, 'get_roots', return_value=[1, 2, 3]).start()
        patch.object(caveclient_mock.chunkedgraph, 'is_valid_nodes', return_value=True).start()
        patch.object(caveclient_mock.info, 'segmentation_cloudvolume', return_value=cloudvolume_mock).start()

        def exists_side_effect(*args, **kwargs):
            # Customize the return value based on the function's parameters
            if args[0] == ['meshwork__v4__rid-1__ds-minnie65_phase3_v1__res-1x1x1__cs-True__cr-7500.h5.gz']:
                return {"rid-1__ds": True}
            elif args[0] == "skeletonization_refusal_root_ids.txt":
                return False
        patch.object(CloudFiles, "exists", side_effect=exists_side_effect).start()

        patch.object(SkeletonService, "publish_skeleton_request", return_value=None).start()

        SkelClassVsn = SkeletonService.get_version_specific_handler(4)

        et = SkelClassVsn.generate_meshworks_bulk_by_datastack_and_rids_async(
            datastack_name=datastack_dict["datastack_name"],
            rids=[1],
            bucket="gs://test_bucket",
            root_resolution=[1, 1, 1],
            collapse_soma=True,
            collapse_radius=7500,
        )

        assert et == 60

    def test_generate_skeletons_bulk_by_datastack_and_rids_async(self, test_app, caveclient_mock, cloudvolume_mock):
        responses.add(responses.GET, url=info_url, json=test_info, status=200)
        
        patch.object(CAVEclient, "__new__", return_value=caveclient_mock).start()

        patch.object(caveclient_mock.l2cache, "has_cache", return_value=True).start()
        patch.object(caveclient_mock.chunkedgraph, 'get_roots', return_value=[1, 2, 3]).start()
        patch.object(caveclient_mock.chunkedgraph, 'is_valid_nodes', return_value=True).start()
        patch.object(caveclient_mock.info, 'segmentation_cloudvolume', return_value=cloudvolume_mock).start()

        def exists_side_effect(*args, **kwargs):
            # Customize the return value based on the function's parameters
            if args[0] == ['skeleton__v4__rid-1__ds-minnie65_phase3_v1__res-1x1x1__cs-True__cr-7500.h5.gz']:
                return {"rid-1__ds": True}
            elif args[0] == "skeletonization_refusal_root_ids.txt":
                return False
        patch.object(CloudFiles, "exists", side_effect=exists_side_effect).start()

        patch.object(SkeletonService, "publish_skeleton_request", return_value=None).start()

        skvn = 4
        SkelClassVsn = SkeletonService.get_version_specific_handler(skvn)

        et = SkelClassVsn.generate_skeletons_bulk_by_datastack_and_rids_async(
            datastack_name=datastack_dict["datastack_name"],
            rids=[1],
            bucket="gs://test_bucket",
            root_resolution=[1, 1, 1],
            collapse_soma=True,
            collapse_radius=7500,
            skeleton_version=skvn,
        )

        assert et == 60
