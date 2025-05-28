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

import ast
import datetime
import inspect
import os
import sys
import argparse
import time
from timeit import default_timer
import traceback
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
try:
    from skeletonservice.datasets.service import DATASTACK_NAME_REMAPPING
    from skeletonservice.datasets.service import __version__ as this_skeletonservice_version
    this_skeletonservice_version = packaging.version.Version(this_skeletonservice_version)
except ImportError:
    # If the import fails, we are probably running in a Jupyter notebook context.
    # In this case, we will simply redefine the remapping dictionary here.
    DATASTACK_NAME_REMAPPING = {
        'minnie65_public': 'minnie65_phase3_v1',
        'flywire_fafb_public': 'flywire_fafb_production',
    }
    this_skeletonservice_version = None

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger()
logger.setLevel(logging.WARNING)

printer = None

CAVE_CLIENT_SERVER = os.environ.get("GLOBAL_SERVER_URL", "https://global.daf-apis.com")

SKELETON_CACHE_BUCKET = os.environ.get("SKELETON_CACHE_BUCKET", "")

PROD_SERVERS = [
    "https://minniev6.microns-daf.com",
    "https://api.em.brain.allentech.org",
    "https://cave.fanc-fly.com",
    "https://prod.flywire-daf.com",
]

# DEFAULT_SLACK_WEBHOOK_ID will be overridden by an environment variable coming from Kubernetes but the global here will be used when running locally.
DEFAULT_SLACK_WEBHOOK_ID = "T0CL3AB5X/B08KJ36BJAF/DfcLRvJzizvCaozpMugAnu38"  # Keith Wiley's Slack direct messages in the Connectome org
# DEFAULT_SLACK_WEBHOOK_ID = "T0CL3AB5X/B08P52E7A05/iXHgqifbk8MtpDw4adoSh5pW"  # deployment-hour-alerts channel in the Connectome org

TOTAL_NUM_TESTS = 27

verbose_level = 0

# Ref: https://svn.blender.org/svnroot/bf-blender/trunk/blender/build_files/scons/tools/bcolors.py
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

bold_fmt_begin = None
ok_fmt_begin = None
warning_fmt_begin = None
fail_fmt_begin = None
skip_fmt_begin = None
fmt_end = None

class SessionedPrinter:
    """
    This class was adapted from the static version in servicy.py.
    """

    def __init__(self, kube):
        global bold_fmt_begin, ok_fmt_begin, warning_fmt_begin, fail_fmt_begin, skip_fmt_begin, fmt_end

        self.session_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S.%f')[:-3]

        bold_fmt_begin = f"{bcolors.BOLD if not kube else ''}"
        ok_fmt_begin = bold_fmt_begin + f"{bcolors.OKGREEN if not kube else ''}"
        warning_fmt_begin = bold_fmt_begin + f"{bcolors.WARNING if not kube else ''}"
        fail_fmt_begin = bold_fmt_begin + f"{bcolors.FAIL if not kube else ''}"
        skip_fmt_begin = bold_fmt_begin + f"{bcolors.OKBLUE if not kube else ''}"
        fmt_end = f"{bcolors.ENDC if not kube else ''}"
    
    def _print_with_session_timestamp(self, *args, sep=' ', end='\n', file=None, flush=False):
        try:
            s = sep.join([str(v) for v in args])
            s = s.replace('\n', f"\n[{self.session_timestamp}] ")
            s = f"[{self.session_timestamp}] " + s
            print(s, end=end, file=file, flush=flush)
        except Exception as e:
            print(f"Error printing message for session [{self.session_timestamp}]: {str(e)}")
            traceback.print_exc()
            print(*args, sep=sep, end=end, file=file, flush=flush)
    
    def print(self, *args, sep=' ', end='\n', file=None, flush=False):
        try:
            self._print_with_session_timestamp(*args, sep=sep, end=end, file=file, flush=flush)
        except Exception as e:
            print(f"Error printing message for session [{self.session_timestamp}]: {str(e)}")
            traceback.print_exc()
            print(*args, sep=sep, end=end, file=file, flush=flush)

class SkeletonsServiceIntegrationTest:
    def __init__(self, kube, datastack_config):
        self.kube = kube
        self.datastack_config = datastack_config
    
    def test_passed(self):
        if verbose_level >= 1:
            printer.print(ok_fmt_begin + "TEST PASSED" + fmt_end)

    def test_failed_with_warning(self):
        if verbose_level >= 1:
            printer.print(warning_fmt_begin + "TEST SUSPICIOUS" + fmt_end)

    def test_failed(self):
        if verbose_level >= 1:
            printer.print(fail_fmt_begin + "TEST FAILED" + fmt_end)

    def test_skipped(self):
        if verbose_level >= 1:
            printer.print(skip_fmt_begin + "TEST SKIPPED" + fmt_end)

    def print_test_result(self, result, warning_only, skipped):
        if result:
            self.test_passed()
        elif warning_only:
            self.test_failed_with_warning()
        elif skipped:
            self.test_skipped()
        else:
            self.test_failed()
            
    def eval_one_test_result(self, result, warning_only=False, skipped=False):
        self.print_test_result(result, warning_only, skipped)
        # Asserting the result prevents the notebook from automatically running all tests.
        # I'm unsure if I want to assert the result and stop or gather all test results at the end.
        # assert result
        return result

    def run_one_server_test(self, server_address, fast_run=False):
        # Set things up
        self.fast_run = fast_run

        if verbose_level >= 1:
            # These have to agree. They are pull from the same source.
            printer.print(f"CAVEclient version: v{cc.__version__} , v{importlib.metadata.version('CAVEclient')}")

        self.remapped_datastack_name = DATASTACK_NAME_REMAPPING[self.datastack_config["name"]] if self.datastack_config["name"] in DATASTACK_NAME_REMAPPING else self.datastack_config["name"]

        self.client = cc.CAVEclient(self.datastack_config["name"], server_address=CAVE_CLIENT_SERVER)
        self.client.materialize.version = self.datastack_config["materialization_version"]

        self.skclient = cc.skeletonservice.SkeletonClient(server_address, self.datastack_config["name"], over_client=self.client, auth_client=self.client.auth, verify=False)
        
        if verbose_level >= 1:
            printer.print(f"SkeletonService server and version C: {CAVE_CLIENT_SERVER} , {server_address} , v{self.skclient._server_version} , v{self.skclient.server_version} , v{self.skclient._get_version()} , v{self.skclient.get_version()} , v{this_skeletonservice_version}")
        if self.skclient._server_version != this_skeletonservice_version:
            printer.print(fail_fmt_begin + "ERROR: SkeletonService version mismatch! This should be impossible at this point! Rerunning the version update wait..." + fmt_end)
            wait_for_skeletonservice_updated_version_deployment(self.kube, self.datastack_config["name"], self.datastack_config["materialization_version"])

        # Hard-code the expected service version instead of retrieving it from the skclient above so we can manually determine when an intended version has fully deployed on a new pod
        self.expected_skeleton_service_version = "0.18.10"
        self.expected_available_skeleton_versions = [-1, 0, 1, 2, 3, 4]

        self.skvn = 4

        if not self.datastack_config["bulk_rids"]:
            return self.skclient._server_version, np.array([0, 0, 0, TOTAL_NUM_TESTS])

        self.single_rid = self.datastack_config["bulk_rids"][0]
        self.valid_rids = [v for v in self.datastack_config["bulk_rids"]]
        if self.datastack_config["single_vertex_rid"]:
            self.valid_rids.append(self.datastack_config["single_vertex_rid"])
        self.larger_bulk_rids = self.datastack_config["bulk_rids"] * 6  # Twelve rids will exceed the ten-rid limit of get_bulk_skeletons()
        self.sample_invalid_node_rid = 864691135687000000

        # Delete the test rid files from the bucket so we can test regenerating them from scratch

        bucket = None
        if "localhost" in server_address or "ltv" in server_address:
            bucket = f"gs://minnie65_skeletons/ltv/{self.datastack_config['name']}/{self.skvn}"
        elif "minnie" in server_address:
            bucket = f"gs://minnie65_skeletons/{self.datastack_config['name']}/{self.skvn}"
        elif "flywire" in server_address:
            bucket = f"gs://flywire_skeletons/{self.datastack_config['name']}/{self.skvn}"
        elif "api" in server_address:
            bucket = f"gs://v1dd_pcg/pcg_skeletons/{self.datastack_config['name']}/{self.skvn}"
        # bucket = f"gs://{SKELETON_CACHE_BUCKET}/{self.datastack_config['name']}/{self.skvn}"
        if verbose_level >= 1:
            printer.print(f"Testing bucket: {bucket}")

        cf = CloudFiles(bucket)
        # for rid in self.valid_rids:
        #     for output_format in ["h5", "flatdict", "swccompressed"]:
        #         filename = f"skeleton__v{self.skvn}__rid-{rid}__ds-{self.remapped_datastack_name}__res-1x1x1__cs-True__cr-7500.{output_format}.gz"
        #         if verbose_level >= 2:
        #             printer.print(filename)
        #             printer.print("Exists status:", cf.exists(filename))

        for rid in self.valid_rids:
            for output_format in ["h5", "flatdict", "swccompressed"]:
                filename = f"skeleton__v{self.skvn}__rid-{rid}__ds-{self.remapped_datastack_name}__res-1x1x1__cs-True__cr-7500.{output_format}.gz"
                if verbose_level >= 2:
                    printer.print(filename)
                    predeletion_exists = cf.exists(filename)
                if not self.fast_run:
                    cf.delete(filename)
                if verbose_level >= 2:
                    printer.print("Exists status before/after deletion:", predeletion_exists, cf.exists(filename))
        
        results = np.array([0, 0, 0, 0])  # Passed, suspicious, failed, skipped
        # results += self.run_test_metadata_1()  # This test isn't too important and likely to cause problems if I forget to update it.
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
        results += self.run_test_retrieval_1()  # Runs two tests, accumulates two tallied results
        results += self.run_test_retrieval_2()  # Runs two tests, accumulates two tallied results
        results += self.run_test_retrieval_3()  # Runs two tests, accumulates two tallied results
        results += self.run_test_retrieval_4()  # Runs two tests, accumulates two tallied results
        results += self.run_test_cache_status_2_1()
        results += self.run_test_cache_status_2_2()
        results += self.run_test_bulk_retrieval_1()
        results += self.run_test_bulk_retrieval_2()
        results += self.run_test_bulk_retrieval_3()
        results += self.run_test_bulk_async_request_1()
        results += self.run_test_bulk_async_request_2()
        results += self.run_test_bulk_async_request_3()

        return self.skclient._server_version, results

    #====================================================================================================
    # Metadata tests

    def run_test_metadata_1(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        skeleton_service_version = self.skclient.get_version()
        # if verbose_level >= 2:
            # printer.print(skeleton_service_version)
        test_result = self.eval_one_test_result(skeleton_service_version == packaging.version.Version(self.expected_skeleton_service_version))
        if not test_result:
            printer.print("Make sure you have assigned the expected version near the top of this test suite. Search for 'expected_skeleton_service_version'.")
        return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)

    def run_test_metadata_2(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        skeleton_versions = self.skclient.get_versions()
        # if verbose_level >= 2:
            # printer.print(skeleton_versions)
        test_result = self.eval_one_test_result(skeleton_versions == self.expected_available_skeleton_versions)
        return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)

    def run_test_metadata_3(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        precomputed_skeleton_info = self.skclient.get_precomputed_skeleton_info(skvn=self.skvn)
        # if verbose_level >= 2:
            # printer.print(precomputed_skeleton_info)
        test_result = self.eval_one_test_result(precomputed_skeleton_info == {
            '@type': 'neuroglancer_skeletons',
            'transform': [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0],
            'vertex_attributes': [
                {'id': 'radius', 'data_type': 'float32', 'num_components': 1},
                {'id': 'compartment', 'data_type': 'uint8', 'num_components': 1}
            ]
        })
        return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)

    #====================================================================================================
    # Cache status tests

    def run_test_cache_status_1_1(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        rids_exist = self.skclient.skeletons_exist(skeleton_version=self.skvn, root_ids=self.valid_rids)
        # if verbose_level >= 2:
            # printer.print(json.dumps(rids_exist, indent=4))
        if not self.fast_run:
            test_result = self.eval_one_test_result(rids_exist == {
                valid_rid: False for valid_rid in self.valid_rids
            })
            return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)
        return (1, 0, 0, 0)

    def run_test_cache_status_1_2(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        rids_exist = self.skclient.skeletons_exist(skeleton_version=self.skvn, root_ids=self.valid_rids, log_warning=False)
        # if verbose_level >= 2:
            # printer.print(json.dumps(rids_exist, indent=4))
        if not self.fast_run:
            test_result = self.eval_one_test_result(rids_exist == {
                valid_rid: False for valid_rid in self.valid_rids
            })
            return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)
        return (1, 0, 0, 0)

    def run_test_cache_status_1_3(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        # Requires CAVEclient version >= v7.6.1
        rids_exist = self.skclient.skeletons_exist(skeleton_version=self.skvn, root_ids=self.valid_rids, verbose_level=1)
        if verbose_level >= 2:
            printer.print(json.dumps(rids_exist, indent=4))
        if not self.fast_run:
            test_result = self.eval_one_test_result(rids_exist == {
                valid_rid: False for valid_rid in self.valid_rids
            })
            return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)
        return (1, 0, 0, 0)

    #====================================================================================================
    # Cache contents tests

    def run_test_cache_contents_1(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        cache_contents = self.skclient.get_cache_contents(skeleton_version=self.skvn, root_id_prefixes=self.valid_rids)
        if verbose_level >= 2:
            printer.print(json.dumps(cache_contents, indent=4))
        if not self.fast_run:
            test_result = self.eval_one_test_result(cache_contents == {
                "num_found": 0,
                "files": []
            })
            return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)
        return (1, 0, 0, 0)

    def run_test_cache_contents_2(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        cache_contents = self.skclient.get_cache_contents(skeleton_version=self.skvn, root_id_prefixes=self.valid_rids, log_warning=False)
        if verbose_level >= 2:
            printer.print(json.dumps(cache_contents, indent=4))
        if not self.fast_run:
            test_result = self.eval_one_test_result(cache_contents == {
                "num_found": 0,
                "files": []
            })
            return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)
        return (1, 0, 0, 0)

    def run_test_cache_contents_3(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        # Requires CAVEclient version >= v7.6.1
        cache_contents = self.skclient.get_cache_contents(skeleton_version=self.skvn, root_id_prefixes=self.valid_rids, verbose_level=1)
        if verbose_level >= 2:
            printer.print(json.dumps(cache_contents, indent=4))
        if not self.fast_run:
            test_result = self.eval_one_test_result(cache_contents == {
                "num_found": 0,
                "files": []
            })
            return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)
        return (1, 0, 0, 0)

    #====================================================================================================
    # Invalid skeleton request tests

    def run_test_invalid_request_1(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        if not self.datastack_config["refusal_list_rid"]:
            self.eval_one_test_result('"Skipped": "Test refusal list root id not provided', skipped=True)
            return (0, 0, 0, 1)
        try:
            sk = self.skclient.get_skeleton(self.datastack_config["refusal_list_rid"], self.datastack_config["name"], skeleton_version=self.skvn, output_format='dict', verbose_level=1)
            self.test_failed()
        except requests.HTTPError as e:
            if verbose_level >= 2:
                printer.print(e.args[0])
            test_result = self.eval_one_test_result('"Error": "Problematic root id: ' + str(self.datastack_config["refusal_list_rid"]) + ' is in the refusal list"' in e.args[0])
            return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)
        return (0, 0, 1, 0)

    def run_test_invalid_request_2(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        try:
            sk = self.skclient.get_skeleton(self.sample_invalid_node_rid, self.datastack_config["name"], skeleton_version=self.skvn, output_format='dict', verbose_level=1)
            self.test_failed()
        except ValueError as e:
            if verbose_level >= 2:
                printer.print(e.args[0])
            test_result = self.eval_one_test_result(e.args[0] == 'Invalid root id: ' + str(self.sample_invalid_node_rid) + ' (perhaps it doesn\'t exist; the error is unclear)')
            return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)
        return (0, 0, 1, 0)

    def run_test_invalid_request_3(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        if not self.datastack_config["supervoxel_rid"]:
            self.eval_one_test_result('"Skipped": "Test supervoxel root id not provided', skipped=True)
            return (0, 0, 0, 1)
        try:
            sk = self.skclient.get_skeleton(self.datastack_config["supervoxel_rid"], self.datastack_config["name"], skeleton_version=self.skvn, output_format='dict', verbose_level=1)
            self.test_failed()
        except ValueError as e:
            if verbose_level >= 2:
                printer.print(e.args[0])
            test_result = self.eval_one_test_result(e.args[0] == 'Invalid root id: ' + str(self.datastack_config["supervoxel_rid"]) + ' (perhaps this is an id corresponding to a different level of the PCG, e.g., a supervoxel id)')
            return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)
        return (0, 0, 1, 0)

    #====================================================================================================
    # Skeleton request tests

    def run_test_retrieval_1(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        start_time = default_timer()
        sk = self.skclient.get_skeleton(self.single_rid, self.datastack_config["name"], skeleton_version=self.skvn, output_format='dict', verbose_level=1)
        elapsed_time = default_timer() - start_time
        # if verbose_level >= 2:
            # display(sk)
        test_result = np.array([0, 0, 0, 0])
        test_result1 = self.eval_one_test_result(sk is not None and isinstance(sk, dict))
        test_result += (1, 0, 0, 0) if test_result1 else (0, 0, 1, 0)
        test_result2 = self.eval_one_test_result(elapsed_time > 5 and elapsed_time < 90, True)
        if not test_result2:
            printer.print(bold_fmt_begin + f"Skeletonization time {elapsed_time:>.1f}s fell outside expected range (5s - 90s). This could indicate that a skeleton was found in the cache when not expected (if too fast) or vs/va (if too slow)." + fmt_end)
        test_result += (1, 0, 0, 0) if test_result2 else (0, 1, 0, 0)
        return test_result

    def run_test_retrieval_2(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        start_time = default_timer()
        sk = self.skclient.get_skeleton(self.single_rid, self.datastack_config["name"], skeleton_version=self.skvn, output_format='dict', verbose_level=1)
        elapsed_time = default_timer() - start_time
        # if verbose_level >= 2:
            # display(sk)
        test_result = np.array([0, 0, 0, 0])
        test_result1 = self.eval_one_test_result(sk is not None and isinstance(sk, dict))
        test_result += (1, 0, 0, 0) if test_result1 else (0, 0, 1, 0)
        test_result2 = self.eval_one_test_result(elapsed_time < 5, True)
        if not test_result2:
            printer.print(bold_fmt_begin + f"Skeletonization time {elapsed_time:>.1f}s fell outside expected range (<5s). This could indicate that a skeleton was found in the cache when not expected (if too fast) or vs/va (if too slow)." + fmt_end)
        test_result += (1, 0, 0, 0) if test_result2 else (0, 1, 0, 0)
        return test_result

    def run_test_retrieval_3(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        start_time = default_timer()
        sk = self.skclient.get_skeleton(self.single_rid, self.datastack_config["name"], skeleton_version=self.skvn, output_format='swc', verbose_level=1)
        elapsed_time = default_timer() - start_time
        # if verbose_level >= 2:
            # display(sk)
        test_result = np.array([0, 0, 0, 0])
        test_result1 = self.eval_one_test_result(sk is not None and isinstance(sk, pd.DataFrame))
        test_result += (1, 0, 0, 0) if test_result1 else (0, 0, 1, 0)
        test_result2 = self.eval_one_test_result(elapsed_time < 5, True)
        if not test_result2:
            printer.print(bold_fmt_begin + f"Skeletonization time {elapsed_time:>.1f}s fell outside expected range (<5s). This could indicate that a skeleton was found in the cache when not expected (if too fast) or vs/va (if too slow)." + fmt_end)
        test_result += (1, 0, 0, 0) if test_result2 else (0, 1, 0, 0)
        return test_result

    def run_test_retrieval_4(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        if not self.datastack_config["single_vertex_rid"]:
            self.eval_one_test_result('"Skipped": "Test single vertex root id not provided', skipped=True)
            return (0, 0, 0, 2)
        start_time = default_timer()
        sk = self.skclient.get_skeleton(self.datastack_config["single_vertex_rid"], self.datastack_config["name"], skeleton_version=self.skvn, output_format='dict', verbose_level=1)
        elapsed_time = default_timer() - start_time
        # if verbose_level >= 2:
            # display(sk)
        test_result = np.array([0, 0, 0, 0])
        test_result1 = self.eval_one_test_result(sk is not None and isinstance(sk, dict))
        test_result += (1, 0, 0, 0) if test_result1 else (0, 0, 1, 0)
        test_result2 = self.eval_one_test_result(elapsed_time > 5 and elapsed_time < 90, True)
        if not test_result2:
            printer.print(bold_fmt_begin + f"Skeletonization time {elapsed_time:>.1f}s fell outside expected range (5s - 90s). This could indicate that a skeleton was found in the cache when not expected (if too fast) or vs/va (if too slow)." + fmt_end)
        test_result += (1, 0, 0, 0) if test_result2 else (0, 1, 0, 0)
        return test_result

    #====================================================================================================
    ## Inspect the cache after generating new skeletons

    def run_test_cache_status_2_1(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        rids_exist = self.skclient.skeletons_exist(skeleton_version=self.skvn, root_ids=self.valid_rids)
        if verbose_level >= 2:
            printer.print(json.dumps(rids_exist, indent=4))
        if not self.fast_run:
            if self.datastack_config["single_vertex_rid"]:
                test_result = self.eval_one_test_result(rids_exist == {
                    self.datastack_config["bulk_rids"][0]: True,
                    self.datastack_config["bulk_rids"][1]: False,
                    self.datastack_config["single_vertex_rid"]: True,
                })
            else:
                test_result = self.eval_one_test_result(rids_exist == {
                    self.datastack_config["bulk_rids"][0]: True,
                    self.datastack_config["bulk_rids"][1]: False,
                })
            if not test_result:
                printer.print( \
warning_fmt_begin + "NOTE: This test might erroneously fail if the test suite is run multiple times in close succession since the asynchronous skeletonizations initiated by the earlier run \
might complete between the time when the cache is cleared at the beginning of this run and the time this test is run." + fmt_end)
            return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)
        return (1, 0, 0, 0)

    def run_test_cache_status_2_2(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        cache_contents = self.skclient.get_cache_contents(skeleton_version=self.skvn, root_id_prefixes=self.valid_rids)
        if verbose_level >= 2:
            printer.print(json.dumps(cache_contents, indent=4))
        if not self.fast_run:
            if self.datastack_config["single_vertex_rid"]:
                test_result = self.eval_one_test_result(cache_contents == {
                    "num_found": 2,
                    "files": [
                        f"skeleton__v4__rid-{self.datastack_config['bulk_rids'][0]}__ds-{self.datastack_config['name']}__res-1x1x1__cs-True__cr-7500.h5.gz",
                        f"skeleton__v4__rid-{self.datastack_config['single_vertex_rid']}__ds-{self.datastack_config['name']}__res-1x1x1__cs-True__cr-7500.h5.gz",
                    ]
                })
            else:
                test_result = self.eval_one_test_result(cache_contents == {
                    "num_found": 1,
                    "files": [
                        f"skeleton__v4__rid-{self.datastack_config['bulk_rids'][0]}__ds-{self.datastack_config['name']}__res-1x1x1__cs-True__cr-7500.h5.gz",
                    ]
                })
            if not test_result:
                printer.print( \
warning_fmt_begin + "NOTE: This test might erroneously fail if the test suite is run multiple times in close succession since the asynchronous skeletonizations initiated by the earlier run \
might complete between the time when the cache is cleared at the beginning of this run and the time this test is run." + fmt_end)
            return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)
        return (1, 0, 0, 0)

    # Small bulk skeleton request tests
    ## This routine truncates the request list to a small number (10 at the time of this writing), returns any skeletons that are available, and submits the rest to the asynchronous queue

    def run_test_bulk_retrieval_1(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        result = self.skclient.get_bulk_skeletons(self.datastack_config["bulk_rids"], skeleton_version=self.skvn, output_format='dict')
        # We can't test(both root ids but only one was generated by the previous tests above.
        # The other root id will be asyncronously triggered by this test but won't be available for 20-60 seconds afterwards.
        test_result = self.eval_one_test_result(str(self.datastack_config["bulk_rids"][0]) in result.keys())
        return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)

    def run_test_bulk_retrieval_2(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        result = self.skclient.get_bulk_skeletons(self.datastack_config["bulk_rids"], skeleton_version=self.skvn, output_format='dict', verbose_level=1)
        # We can't test(both root ids but only one was generated by the previous tests above.
        # The other root id will be asyncronously triggered by this test but won't be available for 20-60 seconds afterwards.
        test_result = self.eval_one_test_result(str(self.datastack_config["bulk_rids"][0]) in result.keys())
        return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)

    def run_test_bulk_retrieval_3(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        result = self.skclient.get_bulk_skeletons(self.larger_bulk_rids, skeleton_version=self.skvn, output_format='dict', verbose_level=1)
        # We can't test(both root ids but only one was generated by the previous tests above.
        # The other root id will be asyncronously triggered by this test but won't be available for 20-60 seconds afterwards.
        test_result = self.eval_one_test_result(str(self.datastack_config["bulk_rids"][0]) in result.keys())
        return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)

    # Asynchronous bulk skeleton request tests
    ## This routine submits a large number of requests and returns only the estimated time to complete the job; it doesn't return any skeletons.
    ### The estimated job time depends on the number of parallel workers available on the server with each skeleton allocated 60s for estimation purposes.
    ### For example, with 10 workers, 1–10 skeletons would take 60s, 11–20 skeletons would take 120s, etc.
    ### At the time of this writing, all servers are configured to use 30 workers.

    def run_test_bulk_async_request_1(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        result = self.skclient.generate_bulk_skeletons_async(self.datastack_config["bulk_rids"], skeleton_version=self.skvn)
        if verbose_level >= 2:
            printer.print(type(result), result)
        if not self.fast_run:
            test_result = self.eval_one_test_result(result == 60.0)
        else:
            test_result = self.eval_one_test_result(result == 0.0)
        return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)

    def run_test_bulk_async_request_2(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        result = self.skclient.generate_bulk_skeletons_async(self.datastack_config["bulk_rids"], skeleton_version=self.skvn, verbose_level=1)
        if verbose_level >= 2:
            printer.print(type(result), result)
        if not self.fast_run:
            test_result = self.eval_one_test_result(result == 60.0)
        else:
            test_result = self.eval_one_test_result(result == 0.0)
        return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)

    def run_test_bulk_async_request_3(self):
        if verbose_level >= 1:
            printer.print(inspect.stack()[0][3])
        result = self.skclient.generate_bulk_skeletons_async(self.larger_bulk_rids, skeleton_version=self.skvn, verbose_level=1)
        if verbose_level >= 2:
            printer.print(type(result), result)
        if not self.fast_run:
            test_result = self.eval_one_test_result(result == 60.0)
        else:
            test_result = self.eval_one_test_result(result == 0.0)
        return (1, 0, 0, 0) if test_result else (0, 0, 1, 0)

    def test_integration(self, server_address, fast_run=False):
        # DEBUG, I'm trying to figure out how the loop above waits for the version to stabilize but then it reverts by the time the code reachs approximately line 216!
        client = cc.CAVEclient(self.datastack_config["name"], server_address=CAVE_CLIENT_SERVER)
        client.materialize.version = self.datastack_config["materialization_version"]
        skclient = cc.skeletonservice.SkeletonClient(server_address, self.datastack_config["name"], over_client=client, auth_client=client.auth, verify=False)
        if verbose_level >= 1:
            printer.print(f"SkeletonService server and version B: {CAVE_CLIENT_SERVER} , {server_address} , v{skclient._server_version} , v{skclient.server_version} , v{skclient._get_version()} , v{skclient.get_version()} , v{this_skeletonservice_version}")
        if skclient._server_version != this_skeletonservice_version:
            printer.print(fail_fmt_begin + "ERROR: SkeletonService version mismatch! This should be impossible at this point! Rerunning the version update wait..." + fmt_end)
            wait_for_skeletonservice_updated_version_deployment(self.kube, self.datastack_config["name"], self.datastack_config["materialization_version"])

        try:
            sksv_version, results = self.run_one_server_test(server_address, fast_run)
            return sksv_version, results
        except Exception as e:
            if verbose_level >= 2:
                printer.print(f"Error running test on {server_address}: {e}")
            
            # [20250523_155310.279] Error running test on https://api3.em.brain.allentech.org: 401 Client Error: UNAUTHORIZED for url: https://api3.em.brain.allentech.org/skeletoncache/api/v1/v1dd/precomputed/skeleton/4/info content: b'{\n  "error": "invalid_token",\n  "message": "Unauthorized - Token is Invalid or Expired"\n}\n'
            if e.args[0] == 401 or "UNAUTHORIZED" in str(e) or "Token is Invalid or Expired" in str(e):
                print("Please manually authenticate the test using the provided URL:", e)

            # Run an artificial failed test to generate a failure message
            self.eval_one_test_result(False)

            client = cc.CAVEclient(self.datastack_config["name"], server_address=CAVE_CLIENT_SERVER)
            client.materialize.version = self.datastack_config["materialization_version"]
            skclient = cc.skeletonservice.SkeletonClient(server_address, self.datastack_config["name"], over_client=client, auth_client=client.auth, verify=False)
            sksv_version = skclient.get_version()
            
            return sksv_version, (0, 0, 1, 0)
    
    def run(self, server, fast_run=False, verbose_level_=0):
        global verbose_level, printer
        verbose_level = verbose_level_
        if not printer:
            # If the tests are run by directly creating a test object and calling run(), such as from a notebook,
            # then the printer will not have been initialized in __main__, so we need to do that here.
            # If the tests are run via a bash script, entering through __main__, then the printer will have been initialized there.
            printer = SessionedPrinter(False)

        sksv_version, (num_passed, num_suspicious, num_failed, num_skipped) = self.test_integration(server, fast_run)
        if verbose_level >= 1:
            printer.print(f"Test results against {self.datastack_config['name']} on {server}: Passed/Suspicious/Failed/Skipped: {num_passed}, {num_suspicious}, {num_failed}, {num_skipped}")
        return sksv_version, num_passed, num_suspicious, num_failed, num_skipped

def dispatch_slack_msg(icon, msg):
    logs_url = f"https://console.cloud.google.com/kubernetes/job/{url_location_specifier}default/skeletoncache-integration-tester/logs"
    session_timestamp_link = f"<{logs_url}|_[{printer.session_timestamp}]_>"
    
    # target will be "DEV" for less serious reporting or "PROD" for critical production failures
    target = "PROD" if icon != ":white_check_mark:" and args.server in PROD_SERVERS else "DEV"

    # When run on Kuberetes, the Slack webhook id environment variable will (ought to) be passed in,
    # but when run locally, it will be picked up from the default global variable.
    slack_webhook_id = os.getenv(target + "_SLACK_WEBHOOK_ID", DEFAULT_SLACK_WEBHOOK_ID)
    slack_url = f"https://hooks.slack.com/services/{slack_webhook_id}"

    json_content = {
        "text": icon + " " + session_timestamp_link + " " + msg
    }
    
    result = requests.post(slack_url, json=json_content)
    printer.print(f"Slack message dispatched to webhook ending in '{slack_webhook_id[-4:]}' with requests.post status & text: {result.status_code} {result.text}")

def wait_for_skeletonservice_updated_version_deployment(kube, datastack_name, materialization_version):
    # Wait for the various skeleton service components to be fully deployed
    if this_skeletonservice_version:
        max_wait_time = 60 * 10
        sleep_time_s = 60
        num_attempts = max_wait_time // sleep_time_s
        while True:
            num_attempts -= 1
            if num_attempts <= 0:
                dispatch_slack_msg(":bangbang:", f"SkeletonService version check timed out after {max_wait_time} seconds. Integration tests cannot be performed. Confirm that _SkeletonService.requirements.[in|txt]_ requires v{this_skeletonservice_version}.")
                printer.print(fail_fmt_begin + f"ERROR! SkeletonService version check timed out after {max_wait_time} seconds. Integration tests cannot be performed. Confirm that _SkeletonService.requirements.[in|txt]_ requires v{this_skeletonservice_version}." + fmt_end)
                if not kube:
                    sys.exit(1)
                sys.exit(0)  # Prevent Kubernetes from rerunning the job since something external is presumably in error and preventing a clean deployment
            
            client = cc.CAVEclient(datastack_name, server_address=CAVE_CLIENT_SERVER)
            client.materialize.version = materialization_version
            skclient = cc.skeletonservice.SkeletonClient(args.server, datastack_name, over_client=client, auth_client=client.auth, verify=False)
            
            sksv_version_a = skclient._server_version
            sksv_version_b = skclient.server_version
            sksv_version_c = skclient._get_version()
            sksv_version_d = skclient.get_version()
            printer.print(f"Comparing deployed and local SkeletonService versions: [v{sksv_version_a}|v{sksv_version_b}|v{sksv_version_c}|v{sksv_version_d}] ==? v{this_skeletonservice_version}")
            if sksv_version_a == this_skeletonservice_version and sksv_version_b == this_skeletonservice_version and sksv_version_c == this_skeletonservice_version and sksv_version_d == this_skeletonservice_version:
                printer.print("The SkeletonService versions match. Proceeding with the test...")
                break
            printer.print(fail_fmt_begin + f"SkeletonService version mismatch. Deployed [v{sksv_version_a}|v{sksv_version_b}|v{sksv_version_c}|v{sksv_version_d}] != local v{this_skeletonservice_version}. Various components are not all fully deployed yet." + fmt_end)
            printer.print(f"Sleeping for {sleep_time_s} seconds to allow the components to fully deploy...")
            time.sleep(sleep_time_s)
            printer.print("Woke up. Rechecking the deployed SkeletonService version...")

    # DEBUG, I'm trying to figure out how the loop above waits for the version to stabilize but then sometimes reverts by the time the code reaches approximately line 216!
    client = cc.CAVEclient(datastack_name, server_address=CAVE_CLIENT_SERVER)
    client.materialize.version = materialization_version
    skclient = cc.skeletonservice.SkeletonClient(args.server, datastack_name, over_client=client, auth_client=client.auth, verify=False)
    if verbose_level >= 1:
        printer.print(f"SkeletonService server and version A: {CAVE_CLIENT_SERVER} , {args.server} , v{skclient._server_version} , v{skclient.server_version} , v{skclient._get_version()} , v{skclient.get_version()} , v{this_skeletonservice_version}")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kube", default=False, action="store_true", help="Inform the tests that they are running in a Kubernetes pod")
    parser.add_argument("-s", "--server")
    parser.add_argument("-d", "--datastack_config")
    parser.add_argument("-z", "--zone")
    parser.add_argument("-c", "--cluster_name")
    parser.add_argument("-f", "--fast_run", default=False, action="store_true", help="Run the tests in fast mode (no skeleton generation)")
    parser.add_argument("-v", "--verbose_level", type=int, default=0, help="Set the verbosity level of the tests (0: no output, 1: basic output, 2: detailed output)")
    args = parser.parse_args()

    verbose_level = args.verbose_level

    printer = SessionedPrinter(args.kube)

    if verbose_level >= 1:
        printer.print(f"datastack_config: `{args.datastack_config}`")
    datastack_config = ast.literal_eval(args.datastack_config)

    url_location_specifier = f"{args.zone}/{args.cluster_name}/"

    printer.print(f"Running SkeletonService integration tests with Kubernetes environment {'not ' if not args.kube else ''}indicated...")

    # Confirm that the datastack is valid
    # if args.datastack not in DATASTACKS:
    #     dispatch_slack_msg(":bangbang:", f"Invalid datastack name: {args.datastack}. Valid datastack options: {', '.join(DATASTACKS)}.")
    #     printer.print(fail_fmt_begin + f"ERROR! Invalid datastack name: *{args.datastack}*. Valid datastack options: *{', '.join(DATASTACKS)}*." + fmt_end)
    #     if not args.kube:
    #         sys.exit(1)
    #     sys.exit(0)  # Prevent Kubernetes from rerunning the job since this is a deterministic failure
    
    # Confirm that the server is valid
    # if args.server not in SERVERS:
    #     dispatch_slack_msg(":bangbang:", f"Invalid server address: {args.server}. Valid server options: {', '.join(SERVERS)}.")
    #     printer.print(fail_fmt_begin + f"ERROR! Invalid server address: *{args.server}*. Valid server options: *{', '.join(SERVERS)}*." + fmt_end)
    #     if not args.kube:
    #         sys.exit(1)
    #     sys.exit(0)  # Prevent Kubernetes from rerunning the job since this is a deterministic failure
    
    wait_for_skeletonservice_updated_version_deployment(args.kube, datastack_config["name"], datastack_config["materialization_version"])

    # Run the integration tests
    test = SkeletonsServiceIntegrationTest(args.kube, datastack_config)
    sksv_version, num_passed, num_suspicious, num_failed, num_skipped = test.run(args.server, args.fast_run, verbose_level)

    # Report the results
    num_total = num_passed + num_suspicious + num_failed + num_skipped
    assert num_total <= TOTAL_NUM_TESTS, f"Total number of tests run ({num_total}) > number of tests implemented ({TOTAL_NUM_TESTS})."
    if num_total < TOTAL_NUM_TESTS:
        dispatch_slack_msg(":bangbang:", f"Total number of tests run ({num_total}) < number of tests implemented ({TOTAL_NUM_TESTS}). This indicates that the testing sequence ended early.")
        printer.print(fail_fmt_begin + f"ERROR! Total number of tests run ({num_total}) < number of tests implemented ({TOTAL_NUM_TESTS}). This indicates that the testing sequence ended early." + fmt_end)
        
    if num_failed > 0:
        dispatch_slack_msg(":exclamation:", f"SkeletonService v{sksv_version} integration test results against *{datastack_config['name']}* on *{args.server}*:\n:white_check_mark:    {num_passed} passed\n:warning:    {num_suspicious} suspicious\n:x:    {num_failed} failed\n:no_entry_sign:    {num_skipped} skipped")
        printer.print(fail_fmt_begin + f"ALERT! SkeletonService v{sksv_version} integration test results against {datastack_config['name']} on {args.server}: Passed/Suspicious/Failed/Skipped: {num_passed}, {num_suspicious}, {num_failed}, {num_skipped}" + fmt_end)
        sys.exit(1)  # Force Kubernetes to rerun the job since this might be a non-deterministic failure
    elif num_suspicious > 0 or num_skipped > 0:
        dispatch_slack_msg(":warning::no_entry_sign:", f"SkeletonService v{sksv_version} integration test results against *{datastack_config['name']}* on *{args.server}*: No tests failed, but {num_suspicious} {'was' if num_suspicious == 1 else 'were'} suspicious and {num_skipped} {'was' if num_skipped == 1 else 'were'} skipped.")
        printer.print(warning_fmt_begin + f"WARNING SkeletonService v{sksv_version} integration test results against {datastack_config['name']} on {args.server}: No tests failed, but {num_suspicious} {'was' if num_suspicious == 1 else 'were'} suspicious and {num_skipped} {'was' if num_skipped == 1 else 'were'} skipped." + fmt_end)
        sys.exit(1)  # Force Kubernetes to rerun the job since this might be a non-deterministic failure
    else:
        dispatch_slack_msg(":white_check_mark:", f"SkeletonService v{sksv_version} integration test results against *{datastack_config['name']}* on *{args.server}*: All tests passed. :tada:")
        printer.print(ok_fmt_begin + f"SkeletonService v{sksv_version} integration test results against {datastack_config['name']} on {args.server}: All tests passed." + fmt_end)

    sys.exit(0)  # Prevent Kubernetes from rerunning the job
