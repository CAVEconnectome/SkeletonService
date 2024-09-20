# SkeletonService

SkeletonService returns skeletons in a variety of formats. It also caches any requested and generated skeletons in a bucket so as to avoid repeated work. Any one skeleton will only be generated once and will then be retrieved from the cache on all future requests.

SkeletonService is not generally accessed directly, say via Python, but rather through a RESTful interface consisting of specific URLs where the service is hosted, likely in the cloud (presented later in this document). However, it can be accessed from Python if desired (presented first).

## Python Interface

Here's how to use SkeletonService in a direct Python fashion. To reemphasize the point, this is not a common end-user use case. That would generally involve CAVEclient, as shown farther below. For a direct Python usage, we start by creating a service object and initializing a few basic parameters:
```
import skeletonservice.datasets.service as service

sksv = service.SkeletonService()

datastack_name = 'minnie65_phase3_v1'

# The bucket indicates where the cache resides.
# It will steadily accumulate skeletons as they are requested for a variety of root ids and formats.
bucket = 'gs://minnie65_skeletons/'
```

We can now use the service object to generate and retrieve skeletons. For the most part, only the `output_format` parameter needs to be adjusted to obtain skeletons of different formats. The `output_format` options are:
* `none`: This output directs the SkeletonService to generate a skeleton file and store it in the cache if one has not yet been generated, but to otherwise dispense with returning the skeleton to the user. This approach can be useful when pregenerating a large number of skeletons batch-style.
* `json`: A Python dictionary containing a JSON description of a skeleton.
* `arrays`: A literal subset of the `json` format offering a minimal set of skeleton attributes.
* `precomputed`: Amongst other possible uses, this format is relied upon by NeuroGlancer for 3D rendering and analysis.
* `h5`: A skeleton conforming to the H5 file spec: https://docs.fileformat.com/misc/h5/ . See note below about H5 skeletons.
* `swc`: A skeleton conforming to the SWC file spec: https://swc-specification.readthedocs.io/en/latest/ . See note below about SWC skeletons.

Here's an example of obtaining a JSON skeleton:
```
skeleton_json = sksv.get_skeleton_by_datastack_and_rid(
  datastack_name=datastack_name,
  rid=864691135397503777,
  output_format='json',
  bucket=bucket,
  root_resolution=[1,1,1],
  collapse_soma=True,
  collapse_radius=7500,
)
```

As indicated above, an `arrays` skeleton would have the same format as a `json` skeleton, but with fewer dictionary keys and associated data.

Note that H5 skeletons are not returned as a file, but rather as a byte-stream underlying such a file. The end user will have to convert the bytes file to an H5 file object:
  ```
  import h5py

  sk_h5_bytes = sksv.get_skeleton_by_datastack_and_rid(
    'minnie65_phase3_v1',
    864691135397503777,
    'h5',
    'gs://minnie65_skeletons/',
    root_resolution=[1,1,1],
    collapse_soma=True,
    collapse_radius=7500,
  )
  sk = h5py.File(sk_h5_bytes)
  ```

Note that SWC skeletons are not returned as a file, but rather as a byte-stream underlying such a file. The end user will have to convert the bytes file to an SWC file object, or alternatively simply as a Pandas Dataframe:
  ```
  from io import StringIO
  import pandas as pd

  sk_swc_bytes = sksv.get_skeleton_by_datastack_and_rid(
    'minnie65_phase3_v1',
    864691135397503777,
    'swc',
    'gs://minnie65_skeletons/',
    root_resolution=[1,1,1],
    collapse_soma=True,
    collapse_radius=7500,
  )
  sk_df = pd.read_csv(sk_swc_bytes, sep=" ",
    names=["id", "type", "x", "y", "z", "radius", "parent"])  # This is the standard SWC column header
  ```

While the examples above show how to access SkeletonService directly as an imported module, this is not the most likely use case in a deployed scenario. The service would conventionally reside in the cloud and be accessed in a RESTful way via http requests and parameterized URLs. Assuming one uses CAVEclient for such applications, here is how it would look in a more realistic scenario:
```
import os
import caveclient as cc

client = cc.CAVEclient(
    'minnie65_phase3_v1',
    server_address=os.environ.get("GLOBAL_SERVER_URL", "https://global.daf-apis.com"),
)
sk = client.skeleton.get_skeleton(
    864691135617152361,
    'minnie65_phase3_v1',
    output_format='arrays',
)
```