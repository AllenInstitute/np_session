# np_session


### *For use on internal Allen Institute network*


```python
from np_session import Session

# initialize with a lims session ID or a string containing one: 
>>> session = Session('c:/1116941914_surface-image1-left.png') 
>>> session.lims.id
1116941914
>>> session.folder
'1116941914_576323_20210721'
>>> session.project
'BrainTV Neuropixels Visual Behavior'
>>> session.is_ecephys_session
True
>>> session.rig.acq # (see `np_config.Rig`)
'W10DT713843'

# some properties are objects with richer information:

# - `pathlib` objects for filesystem paths:
>>> session.lims_path.as_posix()
'//allen/programs/braintv/production/visualbehavior/prod0/specimen_1098595957/ecephys_session_1116941914'
>>> session.data_dict['es_id']
'1116941914'

# - `datetime` objects for easy date manipulation:
>>> session.date
datetime.date(2021, 7, 21)

# - dictionaries from lims (loaded lazily):
>>> session.mouse
Mouse(576323)
>>> session.mouse.lims
LIMS2MouseInfo(576323)
>>> session.mouse.lims.id
1098595957
>>> session.mouse.lims['full_genotype']
'wt/wt'

# ...with a useful string representation:
>>> str(session.mouse)
'576323'
>>> str(session.project)
'NeuropixelVisualBehavior'
>>> str(session.rig)
'NP.0'
```
