# Import flask dependencies
from flask import Blueprint, jsonify, render_template, current_app
from skeletonservice.datasets.models import DataSet
from skeletonservice.datasets.schemas import DataSetSchema
import neuroglancer

mod_datasets = Blueprint("datasets", __name__, url_prefix="/datasets")

__version__ = "0.22.29"


@mod_datasets.route("/")
def index():
    datasets = DataSet.query.all()
    return render_template("datasets.html", datasets=datasets, version=__version__)


@mod_datasets.route("/dataset/<datasetname>")
def dataset_view(datasetname):
    dataset = DataSet.query.filter(DataSet.name == datasetname).first_or_404()
    state = neuroglancer.ViewerState()
    state.layers["img"] = neuroglancer.ImageLayer(
        source="precomputed://" + dataset.image_source
    )
    if dataset.pychunkedgraph_viewer_source is not None:
        state.layers["seg"] = neuroglancer.SegmentationLayer(
            source="graphene://" + dataset.pychunkedgraph_viewer_source
        )
    else:
        state.layers["seg"] = neuroglancer.SegmentationLayer(
            source="precomputed://" + dataset.flat_segmentation_source
        )
    state.layers["ann"] = neuroglancer.SkeletonLayer()
    state.layout = "xy-3d"
    ng_url = neuroglancer.to_url(state, prefix=current_app.config["NEUROGLANCER_URL"])
    return render_template(
        "dataset.html", dataset=dataset, ng_url=ng_url, version=__version__
    )


@mod_datasets.route("/api/datasets")
def get_datasets():
    datasets = DataSet.query.all()
    return jsonify([d.name for d in datasets])


@mod_datasets.route("/api/dataset/<dataset>", methods=["GET"])
def get_dataset(dataset):
    dataset = DataSet.query.filter_by(name=dataset).first_or_404()
    schema = DataSetSchema()
    return schema.jsonify(dataset)
