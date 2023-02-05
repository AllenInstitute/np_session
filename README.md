```python
from np_session import Session

# initialize with a lims session ID or a string containing one: 
>>> session = Session('c:/1116941914_surface-image1-left.png') 
>>> session.id
'1116941914'
>>> session.folder
'1116941914_576323_20210721'
>>> session.project
'BrainTV Neuropixels Visual Behavior'
>>> session.is_ecephys_session
True

# some properties are objects with richer information:

# - `pathlib` objects for filesystem paths:
>>> session.lims_path.as_posix()
'//allen/programs/braintv/production/visualbehavior/prod0/specimen_1098595957/ecephys_session_1116941914'
>>> session.data_dict['es_id']
'1116941914'

# - `datetime` objects for easy date manipulation:
>>> session.date
datetime.date(2021, 7, 21)

# - dictionaries from databases (loaded lazily):
>>> session.mouse['id']
1098595953
>>> session.mouse['full_genotype']
'wt/wt'
>>> session.lims['stimulus_name']
'EPHYS_1_images_H_3uL_reward'
>>> session.mtrain

# - rig info (see `np_config.Rig`)
>>> session.rig.acq
'W10DT713843'

# with useful string representation:
>>> str(session.mouse)
'576323'
>>> str(session)
'1116941914_576323_20210721'
>>> str(session.rig)
'NP.0'
```