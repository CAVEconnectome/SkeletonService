# These tests were initially developed in a Jupyter notebook which was then copied into this repo (see tests/test_integration.ipynb).
# # The notebook was then converted to a Python script (and altered somewhat) so that the testing framework can run the tests.

# SkeletonService integration tests

import urllib3
import logging
import importlib.metadata
import json
import requests
import pandas as pd
import caveclient as cc
from cloudfiles import CloudFiles

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger()
logger.setLevel(logging.WARNING)

class TestSkeletonsServiceIntegration:
    def run_test(self, server_address, fast_run=False):
        # Set things up
        self.fast_run = fast_run

        print(f"CAVEclient version: v{cc.__version__} , v{importlib.metadata.version('CAVEclient')}")

        self.datastack_name = "minnie65_phase3_v1"

        self.client = cc.CAVEclient(self.datastack_name)
        self.client.materialize.version = 1078

        self.skclient = cc.skeletonservice.SkeletonClient(server_address, self.datastack_name, over_client=self.client, verify=False)
        print(f"SkeletonService server and version: {server_address} , v{self.skclient._server_version}")

        self.bulk_rids = [864691135463611454, 864691135687456480]
        self.larger_bulk_rids = self.bulk_rids * 6  # Twelve rids will exceed the ten-rid limit of get_bulk_skeletons()
        self.single_rid = self.bulk_rids[0]
        self.sample_refusal_list_rid = 112233445566778899
        self.sample_invalid_node_rid = 864691135687000000
        self.sample_supervoxel_rid =    88891049011371731
        self.skvn = 4

        # Delete the test rid files from the bucket so we can test regenerating them from scratch

        bucket = None
        if "localhost" in server_address or "ltv" in server_address:
            bucket = f"gs://minnie65_skeletons/ltv/{self.datastack_name}/{self.skvn}"
        elif "minnie" in server_address:
            bucket = f"gs://minnie65_skeletons/{self.datastack_name}/{self.skvn}"
        print(f"Testing bucket: {bucket}")

        cf = CloudFiles(bucket)
        for rid in self.bulk_rids:
            for output_format in ["h5", "flatdict", "swccompressed"]:
                filename = f"skeleton__v{self.skvn}__rid-{rid}__ds-{self.datastack_name}__res-1x1x1__cs-True__cr-7500.{output_format}.gz"
                print(filename)
                print(cf.exists(filename))

        for rid in self.bulk_rids:
            for output_format in ["h5", "flatdict", "swccompressed"]:
                filename = f"skeleton__v{self.skvn}__rid-{rid}__ds-{self.datastack_name}__res-1x1x1__cs-True__cr-7500.{output_format}.gz"
                print(filename)
                print(cf.exists(filename))
                if not self.fast_run:
                    cf.delete(filename)
                print(cf.exists(filename))
        
        self.run_test_metadata_1()
        self.run_test_cache_status_1_1()
        self.run_test_cache_status_1_2()
        self.run_test_cache_status_1_3()
        self.run_test_cache_contents_1()
        self.run_test_cache_contents_2()
        self.run_test_cache_contents_3()
        self.run_test_invalid_request_1()
        self.run_test_invalid_request_2()
        self.run_test_invalid_request_3()
        self.run_test_retrieval_1()
        self.run_test_retrieval_2()
        self.run_test_retrieval_3()
        self.run_test_cache_status_2_1()
        self.run_test_cache_status_2_2()
        self.run_test_bulk_retrieval_1()
        self.run_test_bulk_retrieval_2()
        self.run_test_bulk_retrieval_3()
        self.run_test_bulk_async_request_1()
        self.run_test_bulk_async_request_2()
        self.run_test_bulk_async_request_3()

        return True

    # Metadata tests

    def run_test_metadata_1(self):
        precomputed_skeleton_info = self.skclient.get_precomputed_skeleton_info(skvn=self.skvn)
        # print(precomputed_skeleton_info)
        assert precomputed_skeleton_info == {
            '@type': 'neuroglancer_skeletons',
            'transform': [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0],
            'vertex_attributes': [
                {'id': 'radius', 'data_type': 'float32', 'num_components': 1},
                {'id': 'compartment', 'data_type': 'uint8', 'num_components': 1}
            ]
        }
        print("TEST PASSED")

    # Cache status tests

    def run_test_cache_status_1_1(self):
        rids_exist = self.skclient.skeletons_exist(skeleton_version=self.skvn, root_ids=self.bulk_rids)
        # print(json.dumps(rids_exist, indent=4))
        if not self.fast_run:
            assert rids_exist == {
                self.bulk_rids[0]: False,
                self.bulk_rids[1]: False
            }
        print("TEST PASSED")

    def run_test_cache_status_1_2(self):
        rids_exist = self.skclient.skeletons_exist(skeleton_version=self.skvn, root_ids=self.bulk_rids, log_warning=False)
        # print(json.dumps(rids_exist, indent=4))
        if not self.fast_run:
            assert rids_exist == {
                self.bulk_rids[0]: False,
                self.bulk_rids[1]: False
            }
        print("TEST PASSED")

    def run_test_cache_status_1_3(self):
        # Requires CAVEclient version >= v7.6.1
        rids_exist = self.skclient.skeletons_exist(skeleton_version=self.skvn, root_ids=self.bulk_rids, verbose_level=1)
        print(json.dumps(rids_exist, indent=4))
        if not self.fast_run:
            assert rids_exist == {
                self.bulk_rids[0]: False,
                self.bulk_rids[1]: False
            }
        print("TEST PASSED")

    # Cache contents tests

    def run_test_cache_contents_1(self):
        cache_contents = self.skclient.get_cache_contents(skeleton_version=self.skvn, root_id_prefixes=self.bulk_rids)
        print(json.dumps(cache_contents, indent=4))
        if not self.fast_run:
            assert cache_contents == {
                "num_found": 0,
                "files": []
            }
        print("TEST PASSED")

    def run_test_cache_contents_2(self):
        cache_contents = self.skclient.get_cache_contents(skeleton_version=self.skvn, root_id_prefixes=self.bulk_rids, log_warning=False)
        print(json.dumps(cache_contents, indent=4))
        if not self.fast_run:
            assert cache_contents == {
                "num_found": 0,
                "files": []
            }
        print("TEST PASSED")

    def run_test_cache_contents_3(self):
        # Requires CAVEclient version >= v7.6.1
        cache_contents = self.skclient.get_cache_contents(skeleton_version=self.skvn, root_id_prefixes=self.bulk_rids, verbose_level=1)
        print(json.dumps(cache_contents, indent=4))
        if not self.fast_run:
            assert cache_contents == {
                "num_found": 0,
                "files": []
            }
        print("TEST PASSED")

    # Invalid skeleton request tests

    def run_test_invalid_request_1(self):
        try:
            sk = self.skclient.get_skeleton(self.sample_refusal_list_rid, self.datastack_name, skeleton_version=self.skvn, output_format='dict', verbose_level=1)
        except requests.HTTPError as e:
            print(e)
            assert e.response.text == '{\n    "Error": "Problematic root id: ' + str(self.sample_refusal_list_rid) + ' is in the refusal list"\n}\n'
        print("TEST PASSED")

    def run_test_invalid_request_2(self):
        try:
            sk = self.skclient.get_skeleton(self.sample_invalid_node_rid, self.datastack_name, skeleton_version=self.skvn, output_format='dict', verbose_level=1)
        except requests.HTTPError as e:
            print(e)
            assert e.response.text == '{\n    "Error": "Invalid root id: ' + str(self.sample_invalid_node_rid) + ' (perhaps it doesn\'t exist; the error is unclear)"\n}\n'
        print("TEST PASSED")

    def run_test_invalid_request_3(self):
        try:
            sk = self.skclient.get_skeleton(self.sample_supervoxel_rid, self.datastack_name, skeleton_version=self.skvn, output_format='dict', verbose_level=1)
        except requests.HTTPError as e:
            print(e)
            assert e.response.text == '{\n    "Error": "Invalid root id: ' + str(self.sample_supervoxel_rid) + ' (perhaps this is an id corresponding to a different level of the PCG, e.g., a supervoxel id)"\n}\n'
        print("TEST PASSED")

    # Skeleton request tests

    def run_test_retrieval_1(self):
        sk = self.skclient.get_skeleton(self.single_rid, self.datastack_name, skeleton_version=self.skvn, output_format='dict', verbose_level=1)
        # display(sk)
        assert sk is not None and isinstance(sk, dict)
        print("TEST PASSED")

    def run_test_retrieval_2(self):
        sk = self.skclient.get_skeleton(self.single_rid, self.datastack_name, skeleton_version=self.skvn, output_format='dict', verbose_level=1)
        # display(sk)
        assert sk is not None and isinstance(sk, dict)
        print("TEST PASSED")

    def run_test_retrieval_3(self):
        sk = self.skclient.get_skeleton(self.single_rid, self.datastack_name, skeleton_version=self.skvn, output_format='swc', verbose_level=1)
        # display(sk)
        assert sk is not None and isinstance(sk, pd.DataFrame)
        print("TEST PASSED")

    ## Inspect the cache after generating new skeletons

    def run_test_cache_status_2_1(self):
        rids_exist = self.skclient.skeletons_exist(skeleton_version=self.skvn, root_ids=self.bulk_rids)
        print(json.dumps(rids_exist, indent=4))
        if not self.fast_run:
            assert rids_exist == {
                self.bulk_rids[0]: True,
                self.bulk_rids[1]: False
            }
        print("TEST PASSED")

    def run_test_cache_status_2_2(self):
        cache_contents = self.skclient.get_cache_contents(skeleton_version=self.skvn, root_id_prefixes=self.bulk_rids)
        print(json.dumps(cache_contents, indent=4))
        if not self.fast_run:
            assert cache_contents == {
                "num_found": 1,
                "files": [
                    f"skeleton__v4__rid-{self.bulk_rids[0]}__ds-{self.datastack_name}__res-1x1x1__cs-True__cr-7500.h5.gz"
                ]
            }
        print("TEST PASSED")

    # Small bulk skeleton request tests
    ## This routine truncates the request list to a small number (10 at the time of this writing), returns any skeletons that are available, and submits the rest to the asynchronous queue

    def run_test_bulk_retrieval_1(self):
        result = self.skclient.get_bulk_skeletons(self.bulk_rids, skeleton_version=self.skvn, output_format='dict')
        # We can't assert both root ids but only one was generated by the previous tests above.
        # The other root id will be asyncronously triggered by this test but won't be available for 20-60 seconds afterwards.
        assert(str(self.bulk_rids[0]) in result.keys())
        print("TEST PASSED")

    def run_test_bulk_retrieval_2(self):
        result = self.skclient.get_bulk_skeletons(self.bulk_rids, skeleton_version=self.skvn, output_format='dict', verbose_level=1)
        # We can't assert both root ids but only one was generated by the previous tests above.
        # The other root id will be asyncronously triggered by this test but won't be available for 20-60 seconds afterwards.
        assert(str(self.bulk_rids[0]) in result.keys())
        print("TEST PASSED")

    def run_test_bulk_retrieval_3(self):
        result = self.skclient.get_bulk_skeletons(self.larger_bulk_rids, skeleton_version=self.skvn, output_format='dict', verbose_level=1)
        # We can't assert both root ids but only one was generated by the previous tests above.
        # The other root id will be asyncronously triggered by this test but won't be available for 20-60 seconds afterwards.
        assert(str(self.bulk_rids[0]) in result.keys())
        print("TEST PASSED")

    # Asynchronous bulk skeleton request tests
    ## This routine submits a large number of requests and returns only the estimated time to complete the job; it doesn't return any skeletons.
    ### The estimated job time depends on the number of parallel workers available on the serverm with each skeleton allocated 60s for estimation purposes.
    ### For example, with 10 workers, 1–10 skeletons would take 60s, 11–20 skeletons would take 120s, etc.
    ### At the time of this writing, all servers are configured to use 30 workers.

    def run_test_bulk_async_request_1(self):
        result = self.skclient.generate_bulk_skeletons_async(self.bulk_rids, skeleton_version=self.skvn)
        print(type(result), result)
        assert result == 60.0
        print("TEST PASSED")

    def run_test_bulk_async_request_2(self):
        result = self.skclient.generate_bulk_skeletons_async(self.bulk_rids, skeleton_version=self.skvn, verbose_level=1)
        print(type(result), result)
        assert result == 60.0
        print("TEST PASSED")

    def run_test_bulk_async_request_3(self):
        result = self.skclient.generate_bulk_skeletons_async(self.larger_bulk_rids, skeleton_version=self.skvn, verbose_level=1)
        print(type(result), result)
        assert result == 60.0
        print("TEST PASSED")

    def test_integration(self, test_app):
        # Pick a test server:
        ## * localhost:5000 — Test SkeletonService on the local machine, say via the VS Code Debugger
        ## * ltv5 — The SkeletonService on the test cluster
        ## * minniev6 — Test SkeletonService "in the wild"

        server_addresses = [
            # This server won't work on Github, only on a local machine where VSCode is running skeleton service in the debugger.
            # It can be used when running the tests locally but needs to be disabled before pushing the code to Github.
            # "https://localhost:5000",

            # Run integration tests against the test cluster.
            # This should be a pretty safe test to run.
            "https://ltv5.microns-daf.com",
            
            # Run integration tests against a production cluster.
            # This should be pretty safe, but I like to run it manually, on my local machine.
            # Therefore, I would recommend commenting it out before pushing to Github.
            # "https://minniev6.microns-daf.com",
        ]
        
        for server_address in server_addresses:
            try:
                assert self.run_test(server_address)#, fast_run=True)
            except Exception as e:
                print(f"Error running test on {server_address}: {e}")
                assert False
        