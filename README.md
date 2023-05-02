# np_session


### *For use on internal Allen Institute network*

A lightweight package for handling file paths and metadata associated with
Mindscope Neuropixels experiments with data on local rig-connected machines or
the /allen network.
Provides an interface that can be used by other applications.

```python
>>> from np_session import Session

# initialize with a lims session ID or a string containing one: 
>>> session = Session('c:/1116941914_surface-image1-left.png') 
>>> session.lims.id
1116941914
>>> session.folder
'1116941914_576323_20210721'
>>> session.is_ecephys
True
>>> session.rig.acq # hostnames reflect the computers used during the session, not necessarily the current machines
'W10DT05515'

# some properties are objects with richer information:
>>> session.mouse
Mouse(576323)
>>> session.project
Project('NeuropixelVisualBehavior')

# - `pathlib` objects for filesystem paths:
>>> session.lims_path.as_posix()
'//allen/programs/braintv/production/visualbehavior/prod0/specimen_1098595957/ecephys_session_1116941914'
>>> session.data_dict['es_id']
'1116941914'

# - `datetime` objects for easy date manipulation:
>>> session.date
datetime.date(2021, 7, 21)

# - dictionaries from lims (loaded lazily):
>>> session.mouse.lims
LIMS2MouseInfo(576323)
>>> session.mouse.lims.id
1098595957
>>> session.mouse.lims['full_genotype']
'wt/wt'

# with useful string representations:
>>> str(session.mouse)
'576323'
>>> str(session.project)
'NeuropixelVisualBehavior'
>>> str(session.rig)        # from `np_config` package
'NP.0'

```
