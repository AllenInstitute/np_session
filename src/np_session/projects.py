import enum


class Project(enum.Enum):
    "All specific project names (used on lims) associated with each umbrella project."
    VAR = ("VariabilitySpontaneous", "VariabilityAim1")
    GLO = ("OpenScopeGlobalLocalOddball",)
    ILLUSION = ("OpenScopeIllusion",)
    DR = (
        "DynamicRoutingSurgicalDevelopment",
        "DynamicRoutingDynamicGating",
        "DynamicRoutingTask1Production",
    )
