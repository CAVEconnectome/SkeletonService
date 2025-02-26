import io
from unittest.mock import patch
from skeletonservice.datasets.service import SkeletonService
from cloudfiles import CloudFiles

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

    # @patch.object(CloudFiles, "list")
    def test_get_cache_contents(self, test_app, cloudfiles, mocker):
        rid_prefix = "a"
        skeleton_version = 4
        prefix = f"skeleton__v{skeleton_version}__rid-{rid_prefix}"
        filename = f"{prefix}.h5.gz"

        patcher = patch.object(CloudFiles, "list", return_value=[filename])
        mock = patcher.start()

        SkelClassVsn = SkeletonService.get_version_specific_handler(4)

        cache_contents = SkelClassVsn.get_cache_contents(
            bucket="gs://test_bucket",
            skeleton_version=4,
            rid_prefixes=[rid_prefix])
        
        assert cache_contents == {
            "num_found": 1,
            "files": [filename]
        }

    def test_meshworks_exist(self, test_app):
        pass

    def test_skeletons_exist(self, test_app):
        pass

    def test_publish_skeleton_request(self, test_app):
        pass

    def test_get_skeleton_by_datastack_and_rid(self, test_app):
        pass

    def test_get_meshwork_by_datastack_and_rid_async(self, test_app):
        pass

    def test_get_skeleton_by_datastack_and_rid_async(self, test_app):
        pass

    def test_generate_skeletons_bulk_by_datastack_and_rids_async(self, test_app):
        pass
