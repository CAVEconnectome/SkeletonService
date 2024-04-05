spelunker_state = {
    "dimensions": {"x": [4e-9, "nm"], "y": [4e-9, "nm"], "z": [4e-8, "nm"]},
    "crossSectionScale": 0.5,
    "projectionOrientation": [
        -0.5596503019332886,
        0.16869042813777924,
        0.09972338378429413,
        0.8052268624305725,
    ],
    "projectionScale": 1339591.339963096,
    "layers": [
        {
            "type": "image",
            "source": "",
            "tab": "source",
            "shader": "#uicontrol float black slider(min=0, max=1, default=0.0)\n#uicontrol float white slider(min=0, max=1, default=1.0)\nfloat rescale(float value) {\n  return (value - black) / (white - black);\n}\nvoid main() {\n  float val = toNormalized(getDataValue());\n  if (val < black) {\n    emitRGB(vec3(0,0,0));\n  } else if (val > white) {\n    emitRGB(vec3(1.0, 1.0, 1.0));\n  } else {\n    emitGrayscale(rescale(val));\n  }\n}\n",
            "name": "img",
        },
        {"type": "segmentation", "source": "", "tab": "source", "name": "seg"},
        {
            "type": "skeleton",
            "source": {
                "url": "local://skeletons",
                "transform": {
                    "outputDimensions": {
                        "x": [4e-9, "m"],
                        "y": [4e-9, "m"],
                        "z": [4e-8, "m"],
                    }
                },
            },
            "tab": "source",
            "skeletons": [],
            "name": "ann",
        },
    ],
    "showSlices": False,
    "selectedLayer": {"visible": True, "layer": "seg"},
    "layout": "xy-3d",
}
