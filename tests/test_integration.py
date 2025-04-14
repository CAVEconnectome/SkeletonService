'''
These tests were initially developed in a Jupyter notebook which was then copied into this repo (see tests/test_integration.ipynb).
The notebook was then converted to a Python script (and altered somewhat) so that the testing framework can run the tests.
However, these tests will fail when automatically run on Github because the CAVEclient constructor will fail at the authentication step.
I need to add a token or a secret or something, but I don't want to simply add such a file to the repo.
I haven't solved this problem yet. As such, these tests don't currectly work.
But, you can run the notebook version of these tests manually and they should work since you will have the necessary token and secret files on your local machine.
Ultimately, the script needs to be automatically called in a container on a pod whenever a new tag is deployed to the server.
'''

# SkeletonService integration tests

import inspect
import os
import sys
from timeit import default_timer
import numpy as np
import packaging
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

# These tests don't quite make sense.
# They can't be run from a technical perspective because in a Github context, the CAVEclient constructor will fail at the authentication step.
# Furthermore, they don't make conceptual sense at the time of a Git commit or push
# since the changes need to be deployed on the Kubernetes cluster before the tests can be run.
# I'm leaving this code here as a place holder (and it goes along with the associated notebook),
# but it can't be enabled because it will fail on Github.
PYTEST_INTEGRATION_TESTS_ENABLED = False

verbose_level = 0

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class TestSkeletonsServiceIntegration:
    def test_passed(self):
        if verbose_level >= 1:
            print(f"{bcolors.BOLD}{bcolors.OKGREEN}TEST PASSED{bcolors.ENDC}")

    def test_failed(self):
        if verbose_level >= 1:
            print(f"{bcolors.BOLD}{bcolors.FAIL}TEST FAILED{bcolors.ENDC}")

    def print_test_result(self, result):
        if result:
            self.test_passed()
        else:
            self.test_failed()
            
    def run_one_test(self, result):
        self.print_test_result(result)
        # Asserting the result prevents the notebook from automatically running all tests.
        # I'm unsure if I want to assert the result and stop or gather all test results at the end.
        # assert result
        return result

    def run_one_server_test(self, server_address, fast_run=False):
        # Set things up
        self.fast_run = fast_run

        if verbose_level >= 1:
            print(f"CAVEclient version: v{cc.__version__} , v{importlib.metadata.version('CAVEclient')}")

        self.datastack_name = "minnie65_phase3_v1"

        self.client = cc.CAVEclient(self.datastack_name)
        self.client.materialize.version = 1078

        self.skclient = cc.skeletonservice.SkeletonClient(server_address, self.datastack_name, over_client=self.client, verify=False)
        if verbose_level >= 1:
            print(f"SkeletonService server and version: {server_address} , v{self.skclient._server_version}")

        # Hard-code the expected service version instead of retrieving it from the skclient above so we can manually determine when an intended version has fully deployed on a new pod
        self.expected_skeleton_service_version = "0.18.5"
        self.expected_available_skeleton_versions = [-1, 0, 1, 2, 3, 4]
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
        if verbose_level >= 1:
            print(f"Testing bucket: {bucket}")

        cf = CloudFiles(bucket)
        for rid in self.bulk_rids:
            for output_format in ["h5", "flatdict", "swccompressed"]:
                filename = f"skeleton__v{self.skvn}__rid-{rid}__ds-{self.datastack_name}__res-1x1x1__cs-True__cr-7500.{output_format}.gz"
                if verbose_level >= 2:
                    print(filename)
                    print(cf.exists(filename))

        for rid in self.bulk_rids:
            for output_format in ["h5", "flatdict", "swccompressed"]:
                filename = f"skeleton__v{self.skvn}__rid-{rid}__ds-{self.datastack_name}__res-1x1x1__cs-True__cr-7500.{output_format}.gz"
                if verbose_level >= 2:
                    print(filename)
                    print(cf.exists(filename))
                if not self.fast_run:
                    cf.delete(filename)
                if verbose_level >= 2:
                    print(cf.exists(filename))
        
        results = np.array([0, 0])
        results += self.run_test_metadata_1()
        results += self.run_test_metadata_2()
        results += self.run_test_metadata_3()
        results += self.run_test_cache_status_1_1()
        results += self.run_test_cache_status_1_2()
        results += self.run_test_cache_status_1_3()
        results += self.run_test_cache_contents_1()
        results += self.run_test_cache_contents_2()
        results += self.run_test_cache_contents_3()
        results += self.run_test_invalid_request_1()
        results += self.run_test_invalid_request_2()
        results += self.run_test_invalid_request_3()
        results += self.run_test_retrieval_1()
        results += self.run_test_retrieval_2()
        results += self.run_test_retrieval_3()
        results += self.run_test_cache_status_2_1()
        results += self.run_test_cache_status_2_2()
        results += self.run_test_bulk_retrieval_1()
        results += self.run_test_bulk_retrieval_2()
        results += self.run_test_bulk_retrieval_3()
        results += self.run_test_bulk_async_request_1()
        results += self.run_test_bulk_async_request_2()
        results += self.run_test_bulk_async_request_3()

        return results

    # Metadata tests

    def run_test_metadata_1(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        skeleton_service_version = self.skclient.get_version()
        # if verbose_level >= 2:
            # print(skeleton_service_version)
        test_result = self.run_one_test(skeleton_service_version == packaging.version.Version(self.expected_skeleton_service_version))
        if not test_result:
            print("Make sure you have assigned the expected version near the top of this test suite. Search for 'expected_skeleton_service_version'.")
        return (1, 0) if test_result else (0, 1)

    def run_test_metadata_2(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        skeleton_versions = self.skclient.get_versions()
        # if verbose_level >= 2:
            # print(skeleton_versions)
        test_result = self.run_one_test(skeleton_versions == self.expected_available_skeleton_versions)
        return (1, 0) if test_result else (0, 1)

    def run_test_metadata_3(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        precomputed_skeleton_info = self.skclient.get_precomputed_skeleton_info(skvn=self.skvn)
        # if verbose_level >= 2:
            # print(precomputed_skeleton_info)
        test_result = self.run_one_test(precomputed_skeleton_info == {
            '@type': 'neuroglancer_skeletons',
            'transform': [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0],
            'vertex_attributes': [
                {'id': 'radius', 'data_type': 'float32', 'num_components': 1},
                {'id': 'compartment', 'data_type': 'uint8', 'num_components': 1}
            ]
        })
        return (1, 0) if test_result else (0, 1)

    # Cache status tests

    def run_test_cache_status_1_1(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        rids_exist = self.skclient.skeletons_exist(skeleton_version=self.skvn, root_ids=self.bulk_rids)
        # if verbose_level >= 2:
            # print(json.dumps(rids_exist, indent=4))
        if not self.fast_run:
            test_result = self.run_one_test(rids_exist == {
                self.bulk_rids[0]: False,
                self.bulk_rids[1]: False
            })
            return (1, 0) if test_result else (0, 1)
        return (1, 0)

    def run_test_cache_status_1_2(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        rids_exist = self.skclient.skeletons_exist(skeleton_version=self.skvn, root_ids=self.bulk_rids, log_warning=False)
        # if verbose_level >= 2:
            # print(json.dumps(rids_exist, indent=4))
        if not self.fast_run:
            test_result = self.run_one_test(rids_exist == {
                self.bulk_rids[0]: False,
                self.bulk_rids[1]: False
            })
            return (1, 0) if test_result else (0, 1)
        return (1, 0)

    def run_test_cache_status_1_3(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        # Requires CAVEclient version >= v7.6.1
        rids_exist = self.skclient.skeletons_exist(skeleton_version=self.skvn, root_ids=self.bulk_rids, verbose_level=1)
        if verbose_level >= 2:
            print(json.dumps(rids_exist, indent=4))
        if not self.fast_run:
            test_result = self.run_one_test(rids_exist == {
                self.bulk_rids[0]: False,
                self.bulk_rids[1]: False
            })
            return (1, 0) if test_result else (0, 1)
        return (1, 0)

    # Cache contents tests

    def run_test_cache_contents_1(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        cache_contents = self.skclient.get_cache_contents(skeleton_version=self.skvn, root_id_prefixes=self.bulk_rids)
        if verbose_level >= 2:
            print(json.dumps(cache_contents, indent=4))
        if not self.fast_run:
            test_result = self.run_one_test(cache_contents == {
                "num_found": 0,
                "files": []
            })
            return (1, 0) if test_result else (0, 1)
        return (1, 0)

    def run_test_cache_contents_2(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        cache_contents = self.skclient.get_cache_contents(skeleton_version=self.skvn, root_id_prefixes=self.bulk_rids, log_warning=False)
        if verbose_level >= 2:
            print(json.dumps(cache_contents, indent=4))
        if not self.fast_run:
            test_result = self.run_one_test(cache_contents == {
                "num_found": 0,
                "files": []
            })
            return (1, 0) if test_result else (0, 1)
        return (1, 0)

    def run_test_cache_contents_3(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        # Requires CAVEclient version >= v7.6.1
        cache_contents = self.skclient.get_cache_contents(skeleton_version=self.skvn, root_id_prefixes=self.bulk_rids, verbose_level=1)
        if verbose_level >= 2:
            print(json.dumps(cache_contents, indent=4))
        if not self.fast_run:
            test_result = self.run_one_test(cache_contents == {
                "num_found": 0,
                "files": []
            })
            return (1, 0) if test_result else (0, 1)
        return (1, 0)

    # Invalid skeleton request tests

    def run_test_invalid_request_1(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        try:
            sk = self.skclient.get_skeleton(self.sample_refusal_list_rid, self.datastack_name, skeleton_version=self.skvn, output_format='dict', verbose_level=1)
            self.test_failed()
        except ValueError as e:
            if verbose_level >= 2:
                print(e.args[0])
            test_result = self.run_one_test(e.args[0] == 'Invalid root id: ' + str(self.sample_refusal_list_rid) + ' (perhaps it doesn\'t exist; the error is unclear)')
            return (1, 0) if test_result else (0, 1)
        return (0, 1)

    def run_test_invalid_request_2(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        try:
            sk = self.skclient.get_skeleton(self.sample_invalid_node_rid, self.datastack_name, skeleton_version=self.skvn, output_format='dict', verbose_level=1)
            self.test_failed()
        except ValueError as e:
            if verbose_level >= 2:
                print(e.args[0])
            test_result = self.run_one_test(e.args[0] == 'Invalid root id: ' + str(self.sample_invalid_node_rid) + ' (perhaps it doesn\'t exist; the error is unclear)')
            return (1, 0) if test_result else (0, 1)
        return (0, 1)

    def run_test_invalid_request_3(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        try:
            sk = self.skclient.get_skeleton(self.sample_supervoxel_rid, self.datastack_name, skeleton_version=self.skvn, output_format='dict', verbose_level=1)
            self.test_failed()
        except ValueError as e:
            if verbose_level >= 2:
                print(e.args[0])
            test_result = self.run_one_test(e.args[0] == 'Invalid root id: ' + str(self.sample_supervoxel_rid) + ' (perhaps this is an id corresponding to a different level of the PCG, e.g., a supervoxel id)')
            return (1, 0) if test_result else (0, 1)
        return (0, 1)

    # Skeleton request tests

    def run_test_retrieval_1(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        start_time = default_timer()
        sk = self.skclient.get_skeleton(self.single_rid, self.datastack_name, skeleton_version=self.skvn, output_format='dict', verbose_level=1)
        elapsed_time = default_timer() - start_time
        # if verbose_level >= 2:
            # display(sk)
        test_result = np.array([0, 0])
        test_result1 = self.run_one_test(sk is not None and isinstance(sk, dict))
        test_result += (1, 0) if test_result1 else (0, 1)
        test_result2 = self.run_one_test(elapsed_time > 5 and elapsed_time < 90)
        test_result += (1, 0) if test_result2 else (0, 1)
        return test_result

    def run_test_retrieval_2(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        start_time = default_timer()
        sk = self.skclient.get_skeleton(self.single_rid, self.datastack_name, skeleton_version=self.skvn, output_format='dict', verbose_level=1)
        elapsed_time = default_timer() - start_time
        # if verbose_level >= 2:
            # display(sk)
        test_result = np.array([0, 0])
        test_result1 = self.run_one_test(sk is not None and isinstance(sk, dict))
        test_result += (1, 0) if test_result1 else (0, 1)
        test_result2 = self.run_one_test(elapsed_time < 5)
        test_result += (1, 0) if test_result2 else (0, 1)
        return test_result

    def run_test_retrieval_3(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        start_time = default_timer()
        sk = self.skclient.get_skeleton(self.single_rid, self.datastack_name, skeleton_version=self.skvn, output_format='swc', verbose_level=1)
        elapsed_time = default_timer() - start_time
        # if verbose_level >= 2:
            # display(sk)
        test_result = np.array([0, 0])
        test_result1 = self.run_one_test(sk is not None and isinstance(sk, pd.DataFrame))
        test_result += (1, 0) if test_result1 else (0, 1)
        test_result2 = self.run_one_test(elapsed_time < 5)
        test_result += (1, 0) if test_result2 else (0, 1)
        return test_result

    ## Inspect the cache after generating new skeletons

    def run_test_cache_status_2_1(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        rids_exist = self.skclient.skeletons_exist(skeleton_version=self.skvn, root_ids=self.bulk_rids)
        if verbose_level >= 2:
            print(json.dumps(rids_exist, indent=4))
        if not self.fast_run:
            test_result = self.run_one_test(rids_exist == {
                self.bulk_rids[0]: True,
                self.bulk_rids[1]: False
            })
            if not test_result:
                print( \
f"{bcolors.BOLD}{bcolors.WARNING}NOTE: This test might erroneously fail if the test suite is run multiple times in close succession since the asynchronous skeletonizations initiated by the earlier run \
might complete between the time when the cache is cleared at the beginning of this run and the time this test is run.{bcolors.ENDC}")
            return (1, 0) if test_result else (0, 1)
        return (1, 0)

    def run_test_cache_status_2_2(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        cache_contents = self.skclient.get_cache_contents(skeleton_version=self.skvn, root_id_prefixes=self.bulk_rids)
        if verbose_level >= 2:
            print(json.dumps(cache_contents, indent=4))
        if not self.fast_run:
            test_result = self.run_one_test(cache_contents == {
                "num_found": 1,
                "files": [
                    f"skeleton__v4__rid-{self.bulk_rids[0]}__ds-{self.datastack_name}__res-1x1x1__cs-True__cr-7500.h5.gz"
                ]
            })
            if not test_result:
                print( \
f"{bcolors.BOLD}{bcolors.WARNING}NOTE: This test might erroneously fail if the test suite is run multiple times in close succession since the asynchronous skeletonizations initiated by the earlier run \
might complete between the time when the cache is cleared at the beginning of this run and the time this test is run.{bcolors.ENDC}")
            return (1, 0) if test_result else (0, 1)
        return (1, 0)

    # Small bulk skeleton request tests
    ## This routine truncates the request list to a small number (10 at the time of this writing), returns any skeletons that are available, and submits the rest to the asynchronous queue

    def run_test_bulk_retrieval_1(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        result = self.skclient.get_bulk_skeletons(self.bulk_rids, skeleton_version=self.skvn, output_format='dict')
        # We can't test(both root ids but only one was generated by the previous tests above.
        # The other root id will be asyncronously triggered by this test but won't be available for 20-60 seconds afterwards.
        test_result = self.run_one_test(str(self.bulk_rids[0]) in result.keys())
        return (1, 0) if test_result else (0, 1)

    def run_test_bulk_retrieval_2(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        result = self.skclient.get_bulk_skeletons(self.bulk_rids, skeleton_version=self.skvn, output_format='dict', verbose_level=1)
        # We can't test(both root ids but only one was generated by the previous tests above.
        # The other root id will be asyncronously triggered by this test but won't be available for 20-60 seconds afterwards.
        test_result = self.run_one_test(str(self.bulk_rids[0]) in result.keys())
        return (1, 0) if test_result else (0, 1)

    def run_test_bulk_retrieval_3(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        result = self.skclient.get_bulk_skeletons(self.larger_bulk_rids, skeleton_version=self.skvn, output_format='dict', verbose_level=1)
        # We can't test(both root ids but only one was generated by the previous tests above.
        # The other root id will be asyncronously triggered by this test but won't be available for 20-60 seconds afterwards.
        test_result = self.run_one_test(str(self.bulk_rids[0]) in result.keys())
        return (1, 0) if test_result else (0, 1)

    # Asynchronous bulk skeleton request tests
    ## This routine submits a large number of requests and returns only the estimated time to complete the job; it doesn't return any skeletons.
    ### The estimated job time depends on the number of parallel workers available on the server with each skeleton allocated 60s for estimation purposes.
    ### For example, with 10 workers, 1–10 skeletons would take 60s, 11–20 skeletons would take 120s, etc.
    ### At the time of this writing, all servers are configured to use 30 workers.

    def run_test_bulk_async_request_1(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        result = self.skclient.generate_bulk_skeletons_async(self.bulk_rids, skeleton_version=self.skvn)
        if verbose_level >= 2:
            print(type(result), result)
        test_result = self.run_one_test(result == 60.0)
        return (1, 0) if test_result else (0, 1)

    def run_test_bulk_async_request_2(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        result = self.skclient.generate_bulk_skeletons_async(self.bulk_rids, skeleton_version=self.skvn, verbose_level=1)
        if verbose_level >= 2:
            print(type(result), result)
        test_result = self.run_one_test(result == 60.0)
        return (1, 0) if test_result else (0, 1)

    def run_test_bulk_async_request_3(self):
        if verbose_level >= 1:
            print(inspect.stack()[0][3])
        result = self.skclient.generate_bulk_skeletons_async(self.larger_bulk_rids, skeleton_version=self.skvn, verbose_level=1)
        if verbose_level >= 2:
            print(type(result), result)
        test_result = self.run_one_test(result == 60.0)
        return (1, 0) if test_result else (0, 1)

    def test_integration(self, test_app, force_run=False):
        # Pick a test server:
        ## * localhost:5000 — Test SkeletonService on the local machine, say via the VS Code Debugger
        ## * ltv5 — The SkeletonService on the test cluster
        ## * minniev6 — Test SkeletonService "in the wild"

        if not force_run and not PYTEST_INTEGRATION_TESTS_ENABLED:
            return {}

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
        
        server_results = {}
        for server_address in server_addresses:
            try:
                results = self.run_one_server_test(server_address)#, fast_run=True)
                server_results[server_address] = results
            except Exception as e:
                if verbose_level >= 2:
                    print(f"Error running test on {server_address}: {e}")
                result = self.self.run_one_test(False)
                server_results[server_address] = (0, 1)
        
        return server_results
    
    def run(self, verbose_level_=0):
        global verbose_level
        verbose_level = verbose_level_

        server_results = self.test_integration(None, True)
        for key, value in server_results.items():
            num_passed, num_failed = value
            if verbose_level >= 1:
                print(f"Test results on server {key}:    Num tests passed: {num_passed},    Num tests failed: {num_failed}")
            if num_failed > 0:
                return False
        return True

if __name__ == "__main__":
    test = TestSkeletonsServiceIntegration()
    result = test.run(1)
    if not result:
        err_msg = "ALERT! Some SkeletonService integration tests have failed."

        slack_webhook_id = os.getenv("SLACK_WEBHOOK_ID", "T0CL3AB5X/B08KJ36BJAF/DfcLRvJzizvCaozpMugAnu38")
        if slack_webhook_id:
            url = f"https://hooks.slack.com/services/{slack_webhook_id}"
            json_content = {
                "text": err_msg
            }
            result = requests.post(url, json=json_content)
            print(result.status_code, result.text)

        sys.exit(f"{bcolors.BOLD}{bcolors.FAIL}{err_msg}{bcolors.ENDC}")
    
    sys.exit(0)
