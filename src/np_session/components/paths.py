import pathlib


NPEXP_ROOT = pathlib.Path("//allen/programs/mindscope/workgroups/np-exp")
INCOMING_ROOT = pathlib.Path(
    "//allen/programs/braintv/production/incoming/neuralcoding"
)

TEMPLATES_ROOT = pathlib.Path(
    "//allen/programs/mindscope/workgroups/dynamicrouting/ben/npexp_data_manifests"
)

MVR_RELATIVE_PATH = pathlib.Path("c$/ProgramData/AIBS_MPE/mvr/data")
NEWSCALE_RELATIVE_PATH = pathlib.Path("c$/MPM_data")
CAMVIEWER_RELATIVE_PATH = pathlib.Path("c$/Users/svc_neuropix/cv3dImages")  # NP.0 only
CAMSTIM_RELATIVE_PATH = pathlib.Path("c$/ProgramData/AIBS_MPE/camstim/data")
SYNC_RELATIVE_PATH = pathlib.Path("c$/ProgramData/AIBS_MPE/sync/data")

NEUROPIXELS_DATA_RELATIVE_PATH = pathlib.Path(
    "c$/ProgramData/AIBS_MPE/neuropixels_data"
)
NPEXP_PATH = pathlib.Path("//allen/programs/mindscope/workgroups/np-exp")

QC_PATHS = (
    pathlib.Path(
        "//allen/programs/braintv/workgroups/nc-ophys/corbettb/NP_behavior_pipeline/QC"
    ),
    pathlib.Path("//allen/programs/mindscope/workgroups/openscope/GLO_QC"),
    pathlib.Path("//allen/programs/mindscope/workgroups/openscope/Illusion_QC"),
)
"Item 0 is used as default - currently Corbett's folder."
