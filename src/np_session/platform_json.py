import dataclasses
import datetime
import hashlib
import json
import os
import pathlib
import re
import shutil
import time
import warnings
from typing import Dict, List, Sequence, Tuple, Union, Optional

import pandas as pd
import PIL

import data_getters as dg
import mtrain
import nptk
import strategies
from data_validation import *


TEMPLATES_ROOT = pathlib.Path("//allen/programs/mindscope/workgroups/dynamicrouting/ben/npexp_data_manifests")

WSE_DATETIME_FORMAT = '%Y%m%d%H%M%S' # should match the pattern used throughout the WSE

INCOMING_ROOT = pathlib.Path("//allen/programs/braintv/production/incoming/neuralcoding")

@dataclasses.dataclass
class D2Checklist:
    "Has properties that should all evaluate to True for a D2 session to be ready for upload. `None` means not-yet-checked."
    
    session:str = dataclasses.field(repr=True, compare=True, hash=True)
    ready:bool = dataclasses.field(default=False, repr=True, init=False)
    
    inserted_probes_checked:bool|None = dataclasses.field(default=None, repr=False)
    "Confirmed by visual inspection of the post-insertion image."
    files_exist:bool|None = dataclasses.field(default=None,repr=False)
    "..in np-exp folder. Only for inserted probes."
    files_validated:bool|None = dataclasses.field(default=None,repr=False)
    "..using CB's QC funcs. Only for inserted probes."
    qc_finished:bool|None = dataclasses.field(default=None,repr=False)
    "Only for inserted probes."
    extra_files_removed:bool|None = dataclasses.field(default=None,repr=False)
    "..from np-exp. For all probes."
    platform_json_updated:bool|None = dataclasses.field(default=None,repr=False)
    "..with inserted probes, on np-exp."
    other_platform_jsons_renamed:bool|None = dataclasses.field(default=None,repr=False)
    "..so that their `files` contents aren't uploaded prematurely."
    
    @property # type: ignore[error]
    def ready(self) -> bool:
        "..for trigger file to be written."
        return all(self.__dict__.values())
    
    @ready.setter
    def ready(self, dummy_var):
        "Note: has to exist for dataclass field to be usable."
        dummy_var
    
    @property
    def triggered(self) -> bool:
        "Trigger file has been written to start upload to lims."
        if not hasattr(self, '_triggered'):
            self._triggered = False
        return self._triggered
    
    @triggered.setter
    def triggered(self, value:bool):
        "Can only be set True once, then cannot be un-set."
        if hasattr(self, '_triggered') and self._triggered:
            print(f"Trigger file has already been written for {self.session}.")
            return
        if not isinstance(value, bool):
            raise TypeError(f"`triggered` must be a bool, not {type(value)}")
        self._triggered = value


class PlatformJson(SessionFile):
    
    class IncompleteInfoFromPlatformJson(Exception):
        pass
    # files_template: dict
    
    def __init__(self, path: Union[str, pathlib.Path] = None):
        if path:
            if isinstance(path,str) and path.endswith('.json'):
                self.path = pathlib.Path(path)
            elif isinstance(path, pathlib.Path) and path.suffix == '.json':
                self.path = path
            else:
                raise TypeError(f"{self.__class__} path must be a path ending in .json")
        else:
            raise ValueError(f"{self.__class__} requires a path to a json file")
        
        super().__init__(self.path)
            

    @property
    def backup(self) -> pathlib.Path:
        # probably a good idea to create a backup before modifying anything
        # but don't overwrite an existing backup
        return self.path.with_suffix('.bak')
    
    @property
    def contents(self) -> Dict:
            count = 0
            while count < 10:
                try:
                    with self.path.open('r') as f:
                        json_contents = json.load(f)
                        break
                except json.JSONDecodeError:
                    count += 1
                    time.sleep(0.1)
            else:
                raise json.JSONDecodeError(f"Could not decode {self.path}")
            return json_contents
    
    def update(self, **kwargs):
        "Update the contents of the json file with a backup of the original preserved"""
        # ensure a backup of the original first
        shutil.copy2(self.path, self.backup) if not self.backup.exists() else None
        
        # must copy contents to avoid breaking class property which pulls from .json
        contents = self.contents 
        
        for k,v in kwargs.items():
            contents.update({k:v})
        
        with self.path.open('w') as f:
            json.dump(dict(contents), f, indent=4)
        print(f"updated {self.path.name} with {kwargs.keys()}")
    
    @property
    def files(self) -> Dict[str, Dict[str,str]]:
        return self.contents['files']
    
    @property
    def exp_start(self) -> datetime.datetime:
        """Start time of experiment - not relevant for D2 files"""
        fields_to_try = ['workflow_start_time','CartridgeLowerTime','ExperimentStartTime','ProbeInsertionStartTime',]
        start_time = ''
        while fields_to_try:
            start_time = self.contents.get(fields_to_try.pop(0), '')
            if start_time != '':
                break
        else:
            # platform json file's creation time
            return datetime.datetime.fromtimestamp(self.path.stat().st_ctime)
        # workflow start time from platform json
        return datetime.datetime.strptime(start_time, WSE_DATETIME_FORMAT)
    
    @property
    def exp_end(self) -> datetime.datetime:
        """End time of experiment - not relevant for D2 files"""
        # try to get workflow end time from platform json
        # fields in order of preference for estimating exp end time (for recovering
        # files created during exp based on timestamp)
        fields_to_try = ['platform_json_save_time','workflow_complete_time','ExperimentCompleteTime']
        end_time = ''
        while fields_to_try and end_time == '':
            end_time = self.contents.get(fields_to_try.pop(0), '')
        if end_time == '':
            raise self.__class__.IncompleteInfoFromPlatformJson(f"End time of experiment could not be determined from {self.path.as_uri()}")
        return datetime.datetime.strptime(end_time, WSE_DATETIME_FORMAT)
    
    @property
    def rig(self):
        return self.contents.get('rig_id',None)
    
    @property
    def experiment(self):
        pretest_mice = {
            "603810":'NP0',
            "599657":'NP1',
            "598796":'NP2',
        }
        if self.session.mouse in pretest_mice.keys():
            return f"Pretest{pretest_mice[self.session.mouse]}" 
        return self.contents.get('experiment', self.session.project)
    
    @property
    def session_type(self) -> Literal['habituation', 'D1', 'D2']:
        if (
            any(h in self.contents.get('stimulus_name','') for h in ['hab','habituation'])
        or any(h in self.contents.get('workflow','') for h in ['hab','habituation'])
        ):
            return 'habituation'
        elif 'D2' in self.path.stem:
            return 'D2'
        else:
            return 'D1'

    @property
    def is_ecephys_session(self) -> bool:
        return self.session_type == 'D1'
    
    @property
    def probe_letters_inserted(self) -> List[str]:
        "From CB QC"
        probes_inserted = []
        insertion_notes = self.contents.get('InsertionNotes',dict())
        for probe_letter in 'ABCDEF':
            probe = f'Probe{probe_letter}'
            if (
                probe not in insertion_notes.keys()
                or insertion_notes[probe].get('FailedToInsert',0) == 0
            ):
                # assume that no notes means probe was inserted
                probes_inserted.append(probe_letter)
        return probes_inserted
    
    @probe_letters_inserted.setter
    def probe_letters_inserted(self, inserted:str|Sequence[str]):
        inserted = "".join(i.upper() for i in inserted)
        if not all([probe_letter in 'ABCDEF' for probe_letter in inserted]):
            raise ValueError(f"probe_letters_inserted must be a sequence of letters A-F")
        insertion_notes = self.contents.get('InsertionNotes',dict())
        for probe_letter in 'ABCDEF':
            probe = f'Probe{probe_letter}'
            probe_notes = insertion_notes.get(probe, dict())
            if probe_letter not in inserted:
                probe_notes['FailedToInsert'] = 1
            elif probe_notes['FailedToInsert'] != 0:
                probe_notes['FailedToInsert'] = 0
            insertion_notes.update({probe:probe_notes})
        self.update(InsertionNotes=insertion_notes)
        
    @property
    def script(self) -> pathlib.Path:
        """Platform json 'script_name' is a path to the script that was run, which is
        (we expect) the name of the mtrain *stage*. For the actual script that was run,
        see self.mtrain_session['script'] """
        script = self.contents.get('script_name', None)
        if not script:
            return None
        script = pathlib.Path(script)
        
        camstim_root = [p for p in script.parents[-1::-1] if 'camstim' in p.name.lower()][0]
        return self.src_pkl.parent / script.relative_to(camstim_root)
    
    @property
    def mtrain_session(self) -> dict:
        """Info from MTrain on the last behavior session for the mouse on the experiment
        day specified in the platform json file.  This is used to get the foraging_id"""
        try:
            mouse_info = mtrain.MTrain(self.session.mouse)
        except mtrain.MouseNotInMTrainError:
            return None
        return mouse_info.last_behavior_session_on(self.exp_start.date())
    
    # foraging id
    # -------------------------------------------------------------------------- #
    # there are few ways to get the foraging id - it's currently written into the
    # platform json file y the WSE, but may be incorrect (DR experiments create a new
    # foraging id that the WSE isn't aware of) or missing (variability project).
    # Functions here find the foraging id in a variety of ways - we'll just choose one
    # to use.
    
    @property
    def foraging_id_from_contents(self) -> Optional[str]:
        """Foraging ID currently in the platform json. May be in the 'foraging_id' field
        or 'foraging_id_list'.
        
        Not all mice have foraging IDs (e.g. variability project)"""
        
        from_field = self.contents.get('foraging_id', None)
        if from_field:
            if foraging_id := contains_foraging_id(from_field):
                return foraging_id
        
        from_list = [contains_foraging_id(s) for s in self.contents.get('foraging_id_list', [])]
        if len(from_list) == 1:
            return from_list[0]
    
    @property
    def foraging_id_mtrain(self) -> str:
        """Foraging ID recorded for the last behavior session of the experiment day for
        this mouse (mouse/day from platform json). 
        
        Not all mice have foraging IDs (e.g. variability project)"""
        if self.mtrain_session:
            return self.mtrain_session['id']
        return None
    
    @property
    def foraging_id_lims(self) -> str:
        """Foraging ID from lims based on start/stop time of experiment and mouse ID
        (from platform json), obtained from the behavior session that ran at the time. 
        
        Not all mice have foraging IDs (e.g. variability project)"""
        try:
            from_lims = dg.get_foraging_id_from_behavior_session(
                self.session.mouse,
                self.exp_start,
                self.exp_end,
            )
        except dg.MultipleBehaviorSessionsError:
            from_lims = None
        except dg.NoBehaviorSessionError:
            from_lims = None
    
        return from_lims
    
    @property 
    def foraging_id_pkl(self) -> str:
        matches = get_files_created_between(self.src_pkl, strsearch="*.pkl", start=self.exp_start, end=self.exp_end)
        for match in matches:
            if foraging_id := contains_foraging_id(match.as_posix()):
                return foraging_id
        
    @property
    def foraging_id(self):
        """Final foraging ID to use in platform json - currently using ID from 
        behavior session in lims if available, then mtrain, platform json itself"""
        return self.foraging_id_lims or self.foraging_id_mtrain or self.foraging_id_pkl or self.foraging_id_from_contents
    
    @property
    def has_qc(self) -> bool:
        "Does each inserted probe have unit_metrics and probe_noise QC files?"
        for qc_path in QC_PATHS:
            probe_noise = qc_path / self.session.folder / "probe_noise"
            unit_metrics = qc_path / self.session.folder / "unit_metrics"
            if probe_noise.exists() and unit_metrics.exists():
                probe_noise_paths = list(probe_noise.glob("*"))
                unit_metrics_paths = list(unit_metrics.glob("*"))
                for probe_letter in self.probe_letters_inserted:
                    if not any([f"robe{probe_letter}" in p.name for p in probe_noise_paths]):
                        break
                    if not any([f"robe{probe_letter}" in p.name for p in unit_metrics_paths]):
                        break
                else:
                    return True
        return False
    
    # - ------------------------------------------------------------------------------------ #
    
    @property
    def mon(self):
        return nptk.ConfigHTTP.hostname(f'{self.rig}-Mon')
    @property
    def sync(self):
        return nptk.ConfigHTTP.hostname(f'{self.rig}-Sync')
    @property
    def stim(self):
        return nptk.ConfigHTTP.hostname(f'{self.rig}-Stim')
    @property
    def acq(self):
        return nptk.ConfigHTTP.hostname(f'{self.rig}-Acq')
    # no src_acq because it depends on probe letter (A:/ B:/)
    
    @property
    def src_video(self) -> pathlib.Path:
        return pathlib.Path(fR"\\{self.mon}\{MVR_RELATIVE_PATH}")
    
    @property
    def src_motor_locs(self) -> pathlib.Path:
        return pathlib.Path(fR"\\{self.mon}\{NEWSCALE_RELATIVE_PATH}")
    
    @property
    def src_image(self) -> pathlib.Path:
        if self.rig == 'NP.0':
            return pathlib.Path(fR"\\{self.mon}\{CAMVIEWER_RELATIVE_PATH}")
        return pathlib.Path(fR"\\{self.mon}\{MVR_RELATIVE_PATH}")
    
    @property
    def src_pkl(self) -> pathlib.Path:
        return pathlib.Path(fR"\\{self.stim}\{CAMSTIM_RELATIVE_PATH}")
    
    @property
    def src_sync(self) -> pathlib.Path:
        return pathlib.Path(fR"\\{self.sync}\{SYNC_RELATIVE_PATH}") 
    
    def write_trigger(self):
        """Write a trigger file to lims incoming/trigger"""
        with open(INCOMING_ROOT / "trigger" / f"{self.session.id}.ecp", "w") as f:
            f.writelines(f"sessionid: {self.session.id}\n")
            f.writelines(f"location: '{INCOMING_ROOT.as_posix()}'")
    
# entries ------------------------------------------------------------------------------ #

# depending on the data type of each entry, the method to find the corresponding
# original data files will be quite different
# - from each entry in platform.json "files" field we create an Entry object of a
#   specific subtype, using the factory method below e.g. entry_from_dict(self, entry)

class Entry:
        
    def __init__(self, entry:Union[Dict,Tuple]=None, platform_json:PlatformJson=None):
        # entry in platform json 'files' has the format:
        #   'ephys_raw_data_probe_A': {
        #          'directory_name': '1208053773_623319_20...7_probeABC'}
        # we'll call the first key the 'descriptive_name'
        # second is 'dir_or_file_type' (values are 'directory_name', 'filename')
        # the value is the name of the directory or file: 'dir_or_file_name'
        
        self.descriptive_name = d = entry[0] if isinstance(entry, tuple) else list(entry.keys())[0]
        self.dir_or_file_type: str = list(entry[1].keys())[0] if isinstance(entry, tuple) else list(entry[d].keys())[0]
        self.dir_or_file_name: str = list(entry[1].values())[0] if isinstance(entry, tuple) else list(entry[d].values())[0]
                
        # we'll need some general info about the experiment:
        self.platform_json: PlatformJson = Files(platform_json.path) if not isinstance(platform_json, Files) else platform_json
        
        self.actual_data: pathlib.Path = self.platform_json.path.parent / self.dir_or_file_name
        
        if not self.platform_json.dict_expected.get(self.descriptive_name,None):
            # necessary fix for some projects (OSIllusion)
            if self.descriptive_name == 'isi_registration_coordinates':
                self.descriptive_name = 'isi _registration_coordinates'
            elif self.descriptive_name == 'isi _registration_coordinates':
                self.descriptive_name = 'isi_registration_coordinates'
        
    def __eq__(self, other):
        # when comparing entries we want to know whether they have the same
        # descriptive name key and the same file/folder name
        return self.descriptive_name == other.descriptive_name and self.dir_or_file_name == other.dir_or_file_name
            
    def __dict__(self):
        return {self.descriptive_name: {self.dir_or_file_type:self.dir_or_file_name}}
     
    def __str__(self):
        return self.dir_or_file_name
    
    def __repr__(self):
        return f"{self.__class__.__name__}({self.__dict__()})"
    
    def __hash__(self):
        return hash(str(self.__dict__()))
    
    @property
    def suffix(self) -> str:
        return self.dir_or_file_name.replace(self.platform_json.session.folder,'')

    @property
    def expected_data(self) -> pathlib.Path:
        """Presumed path to the data in the same folder as the platform json file"""
        return self.platform_json.path.parent / self.platform_json.dict_expected[self.descriptive_name][self.dir_or_file_type]
    
    @property
    def correct(self) -> bool:
        """Check entry dict matches template and specified file exists"""
        return self.correct_dict and self.correct_data
    
    @property
    def correct_dict(self) -> bool:
        return self.__dict__() == self.__dict__()
        # return self in [self.platform_json.entry_from_dict(entry) for entry in self.platform_json.expected.items()]
    
    @property
    def correct_data(self) -> bool:
        # exists mainly to be overloaded by ephys entry
        return self.expected_data.exists()
    @property
    def sd9(self):
        sd9 = pathlib.Path("//10.128.54.19/sd9")
        try:
            if sd9.exists() and self.platform_json.path.parent.exists():
                return sd9 / self.platform_json.path.parent.name
        except OSError:
            pass
        return None
    @property
    def sources(self) -> List[pathlib.Path]:
        sources = []
        if self.origin:
            sources.append(self.origin)
        if self.npexp.exists() and self.z.exists():
            if self.npexp.stat().st_size == self.z.stat().st_size:
                sources.append(self.npexp)
            else:
                print(f'Copies differ between {self.dir_or_file_name} on np-exp ({self.npexp.stat().st_size} B) and Z: drive ({self.z.stat().st_size})')
                if self.npexp.stat().st_size > self.z.stat().st_size:
                    print(f'Using copy on Z: drive')
                    sources.append(self.z)
                if self.npexp.stat().st_size < self.z.stat().st_size:
                    print(f'Using copy on np-exp')
                    sources.append(self.npexp)
        elif self.npexp.exists():
            sources.append(self.npexp)    
        elif self.z.exists():
            sources.append(self.z)
        elif self.sd9:
            sources.append(self.sd9)
        return sources

    @property
    def origin(self) -> pathlib.Path:
        """Path to original file for this entry"""
        raise NotImplementedError # should be implemented by subclasses
    
    @property
    def npexp(self) -> pathlib.Path:
        """Path to possible copy on np-exp"""
        return NPEXP_PATH / self.platform_json.session.folder / self.dir_or_file_name 
    
    @property
    def z(self) -> pathlib.Path:
        """Path to possible copy on z-drive/neuropixels_data"""
        return pathlib.Path(f"//{self.platform_json.sync}/{NEUROPIXELS_DATA_RELATIVE_PATH}") / self.platform_json.session.folder / self.dir_or_file_name 
        
    @property
    def lims(self) -> Union[pathlib.Path,None]:
        if self.dir_or_file_type == 'filename':
            return SessionFile(self.expected_data).lims_path
        else:
            raise NotImplementedError
    
    def rename():
        """Rename the current data in the same folder as the platform json file"""
        pass
        # TODO
        
    def copy(self, dest: Union[str, pathlib.Path]=None):
        """Copy original file to a specified destination folder"""
        # TODO add checksum of file/dir to db
        if not self.sources:
            print("Copy aborted - no files found at origin or backup locations")
            return
        
        if dest is None:
            dest = self.expected_data

        dest = pathlib.Path(dest)
        
        for source in self.sources:
        
            if not source.exists():
                continue 
            
            if source == dest:
                continue
            
            if source.is_dir() and dest.is_dir():
                larger = get_largest_dir([source,dest])
                
                if larger is None:
                    # both the same size
                    print(f"Original data and copy in folder are the same size")

                if source == larger:
                    pass
                elif dest == larger:
                    print(f"{source} is smaller than {dest} - copy manually if you really want to overwrite")
                    return
                
            if source.is_file() and dest.is_file():
                
                if dest.stat().st_size == 0:
                    pass
                elif dest.stat().st_size < source.stat().st_size:
                    pass
                elif dest.stat().st_size == source.stat().st_size:
                    hashes = []
                    for idx, file in enumerate([dest, source]):
                        print(f"Generating checksum for {file} - may take a while.." )
                        with open(file,'rb') as f:
                            hashes.append(hashlib.md5(f.read()).hexdigest())
                     
                    if hashes[0] == hashes[1]:
                        print(f"Original data and copy in folder are identical")
                        return
                
                elif dest.stat().st_size > source.stat().st_size:
                    print(f"{source} is smaller than {dest} - copy manually if you really want to overwrite")
                    return
                        
            # do the actual copying
            if self.dir_or_file_type == 'directory_name':
                print(f"Copying {source} to {dest}")
                if not STAGING:
                    shutil.copytree(source,dest, dirs_exist_ok=True)
                else:
                    for source_sub in source.rglob('*'):
                        dest_sub = dest / source_sub.relative_to(source) 
                        dest_sub.parent.mkdir(parents=True, exist_ok=True)
                        dest_sub.symlink_to(source_sub)
                print('Copying complete')
            
            if self.dir_or_file_type == 'filename':
                print(f"Checksumming and copying {source} to {dest}")
                strategies.copy_file(source,dest) if not STAGING else dest.symlink_to(source)
                print('Copying complete')

            if self.correct_data:
                break
    
# -------------------------------------------------------------------------------------- #
class EphysSorted(Entry):
    
    lims_upload_labels = [f"ephys_raw_data_probe_{c}_sorted" for c in 'ABCDEF']
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.probe_letter = self.descriptive_name[-1].upper() # A-F           
        # self.source = self.npexp
    
    @property
    def expected_data(self) -> pathlib.Path:
        """Presumed path to the data in the same folder as the platform json file"""
        return self.platform_json.path.parent / self.platform_json.dict_expected_d2[self.descriptive_name][self.dir_or_file_type]
    
    @property
    def origin(self) -> pathlib.Path:
        return self.npexp / self.dir_or_file_name
        
    @property 
    def lims(self) -> Union[pathlib.Path,None]:
        if not hasattr(self, '_lims'):
            if not (lims := self.platform_json.session.lims_path):
                self._lims = None
            else:
                glob = list(lims.glob(f'*/*_probe{self.probe_letter}'))
                self._lims = glob[0] if glob else None
        return self._lims
    
    # def copy(self, *args, **kwargs):
    #     for f:
    #         super().copy(*args, **kwargs)

class EphysRaw(Entry):
    
    probe_drive_map = {
        'A':'A',
        'B':'A',
        'C':'A',
        'D':'B',
        'E':'B',
        'F':'B'
    }
    probe_group_map = {
        'A':'_probeABC',
        'B':'_probeABC',
        'C':'_probeABC',
        'D':'_probeDEF',
        'E':'_probeDEF',
        'F':'_probeDEF'
    }
    
    lims_upload_labels = [f'ephys_raw_data_probe_{c}' for c in 'ABCDEF']
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.probe_letter = self.descriptive_name[-1].upper() # A-F           
        self.source = pathlib.Path(f"//{self.platform_json.acq}/{self.probe_drive_map[self.probe_letter]}")
    
    @property 
    def lims(self) -> Union[pathlib.Path,None]:
        return self.platform_json.session.lims_path / self.dir_or_file_name if self.platform_json.session.lims_path else None
    
    @property
    def origin(self) -> pathlib.Path:
        
        def filter_hits(hits:List[pathlib.Path]) -> List[pathlib.Path]:
            return [h for h in hits if not any(f in h.as_posix() for f in ['_temp', '_pretest'])]
        
        # search for folder on acq drive with matching session folder and/or creation time
        hits = []
        hits += self.source.glob(f"{self.platform_json.session.folder}{self.probe_group_map[self.probe_letter]}*")
        glob = f"*{self.platform_json.session.folder}*"
        hits += get_dirs_created_between(self.source,glob,self.platform_json.exp_start,self.platform_json.exp_end)
        
        single_hit = return_single_hit(filter_hits(hits))
        if single_hit:
            return single_hit
        
        # in case none returned above, search more generally without the session folder name
        hits = get_dirs_created_between(self.source,'*',self.platform_json.exp_start,self.platform_json.exp_end)
        single_hit = return_single_hit(filter_hits(hits))      
        if single_hit:
            return single_hit   
        
        # TODO if multiple folders found, find the largest
        # TODO locate even if no folders with matching session folder or creation time
    
    @property 
    def platform_json_on_z_drive(self) -> bool:
        # raw probe data isn't stored on the z drive like other data, so this flag will
        # be queried by other functions
        if any(s in str(self.platform_json.path.parent).lower() for s in ['neuropixels_data', 'z:/', 'z:\\']):
            return True
        return False
    
    def copy(self, *args, **kwargs):
        if self.platform_json_on_z_drive:
            print(f"Copying not implemented for {self.__class__.__name__} to {self.expected_data}: these data don't live on Z: drive")
        else:
            super().copy(*args, **kwargs)
            
    @property
    def correct_data(self) -> bool:
        # overloaded to return True if the platform json being examined is on the
        # z-drive and data exists at origin
        if self.platform_json_on_z_drive and self.origin:
            return True
        return super().correct_data
    
# -------------------------------------------------------------------------------------- #
class Sync(Entry):
    
    lims_upload_labels = ['synchronization_data']
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source = self.platform_json.src_sync
        
    @property
    def origin(self) -> pathlib.Path:
        glob = f"*{self.expected_data.suffix}"
        hits = get_files_created_between(self.source,glob,self.platform_json.exp_start,self.platform_json.exp_end)
        if hits:
            return return_single_hit(hits)
       
        # try again with different search
        glob = f"*{self.platform_json.session.folder}*.sync"
        start = self.platform_json.exp_start
        end = self.platform_json.exp_end + datetime.timedelta(minutes=30)
        hits = get_files_created_between(self.source,glob,start,end)
        if hits:
            return return_single_hit(hits)
        # try again with differnt search
        glob = f"*{self.platform_json.session.folder}*.h5"
        start = self.platform_json.exp_start
        end = self.platform_json.exp_end + datetime.timedelta(minutes=30)
        hits = get_files_created_between(self.source,glob,start,end)
        # if not hits:
        #     print(f"No matching sync file found at origin {self.source}")
        return return_single_hit(hits)
        
# -------------------------------------------------------------------------------------- #
class Camstim(Entry):
    
    descriptive_labels = ['behavior','optogenetic','visual','replay']
    lims_upload_labels = [f"{pkl}_stimulus" for pkl in descriptive_labels]
    
    # for reference, not used/doesn't need updating:
    pkl_file_label_descriptive_label_map = {
        'stim':'visual',
        'mapping': 'visual',
        'behavior': 'behavior',
        'opto': 'optogenetic',
        }
        
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source = self.platform_json.src_pkl
        self.pkl_file_label = self.dir_or_file_name.split('.')[-2] 
        self.descriptive_label = self.descriptive_name.split('_')[0]
    
    @property
    def origin(self) -> pathlib.Path:
        hits = []
        
        # visual ----------------------------------------------------------------------------- #
        if self.descriptive_label == 'visual':
            
            def exclude_other_labels(matches: Sequence[pathlib.Path]) -> list[pathlib.Path]:
                other_labels = (
                    set(self.descriptive_labels + list(self.pkl_file_label_descriptive_label_map))
                    - set((self.descriptive_label,self.pkl_file_label))
                )
                return [m for m in matches if not any(s in m.name for s in other_labels)]
            
            # When all processing completes, camstim Agent class passes data and uuid to
            # /camstim/lims BehaviorSession class, and write_behavior_data() writes a
            # final .pkl with default name YYYYMMDDSSSS_mouseID_foragingID.pkl
            # - if we have a foraging ID, we can search for that
            foraging_pkl = None
            #* this is our preferred visual pkl
            if foraging_id := self.platform_json.foraging_id:
                matches = list(self.source.glob(f"*{foraging_id}*.pkl"))
                matches = exclude_other_labels(matches)
                if foraging_pkl := return_single_hit(matches):
                    return foraging_pkl
                    
            # if no foraging ID is found from lims (behavior session can be recorded
            # with no date_of_acquisition, which makes it tricky to locate), there may be a foraging_id in the platform json
            # and a pkl with that foraging ID might exist
            if foraging_id := self.platform_json.foraging_id_from_contents:
                matches = list(self.source.glob(f"*{foraging_id}*.pkl"))
                matches = exclude_other_labels(matches)
                if foraging_pkl := return_single_hit(matches):
                    return foraging_pkl
            
            # otherwise, we can search for any pkl created during the timeframe of the experiment
            # and check whether it has a foraging ID in its name
            glob = ("*.pkl")
            matches = get_files_created_between(self.source,glob,self.platform_json.exp_start,self.platform_json.exp_end)
            matches = exclude_other_labels(matches)
            if foraging_pkls := [pkl for pkl in matches if contains_foraging_id(pkl.name)]:
                return return_single_hit(foraging_pkls)
            
            mtrain_stage_pkl = None
            #* this is second preference visual pkl if the foraging pkl is not found
            mtrain_stage = self.platform_json.contents.get('stimulus_name', None)
            mtrain_stage = self.platform_json.script.stem if (self.platform_json.script and mtrain_stage is None) else mtrain_stage
            if mtrain_stage:
                glob = f"*{mtrain_stage}*.pkl"
                matches = get_files_created_between(self.source,glob,self.platform_json.exp_start,self.platform_json.exp_end)
                if matches:
                    matches = exclude_other_labels(matches)
                    mtrain_stage_pkl = return_single_hit(matches)
            if mtrain_stage_pkl:
                return mtrain_stage_pkl

            #* last resort is to search for any pkl with 'stim' or 'mapping' in its name - created
            #* within the timeframe of the experiment - using the logic for the other pkls below:
            
        # optogenetic, behavior, replay ---------------------------------------------------------- #
        glob = f"*{self.pkl_file_label}*.pkl"
        hits += get_files_created_between(self.source,glob,self.platform_json.exp_start,self.platform_json.exp_end)   
        # if len(hits) == 0:
        #     print(f"No matching {self.pkl_file_label}.pkl found")
        
        return return_single_hit(hits)

# -------------------------------------------------------------------------------------- #
class VideoTracking(Entry):
    cams = ['behavior','eye', 'face']
    lims_upload_labels =[f"{cam}_tracking" for cam in cams]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source = self.platform_json.src_video
        self.cam = self.dir_or_file_name.split('.')[-2] 
        
    @property
    def origin(self) -> pathlib.Path:
        glob = f"*{self.cam}*{self.expected_data.suffix}"
        start = self.platform_json.exp_start
        end = self.platform_json.exp_end + datetime.timedelta(seconds=10)
        hits = get_files_created_between(self.source,glob,start,end)
        # if not hits:
        #     print(f"No matching video info json at origin {self.source}")
        return return_single_hit(hits)
    
class VideoInfo(Entry):
    # preference would be to inherit from VideoTracking
    # but then this class wouldn't be a direct subclass of Entry
    # and Entry.__subclasses__() no longer returns this class
    
    cams = ['beh','eye', 'face']
    lims_upload_labels =[f"{cam}_cam_json" for cam in cams]
        
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source = self.platform_json.src_video
        self.cam = self.dir_or_file_name.split('.')[-2] 
    
    @property
    def origin(self) -> pathlib.Path:
        hits = []
        glob = f"*{self.cam}*{self.expected_data.suffix}"
        start = self.platform_json.exp_start
        end = self.platform_json.exp_end + datetime.timedelta(seconds=10)
        hits = get_files_created_between(self.source,glob,start,end)
        # if not hits:
        #     print(f"No matching video info json at origin {self.source}")
        return return_single_hit(hits)
    
# -------------------------------------------------------------------------------------- #
class SurfaceImage(Entry):

    imgs = ['pre_experiment','brain','pre_insertion','post_insertion','post_stimulus','post_experiment']
    lims_upload_labels =[f"{img}_surface_image_{side}" for img in imgs for side in ['left','right'] ] # order of left/right is important for self.original
    
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert hasattr(self,'descriptive_name')
        self.source = self.platform_json.src_image
        self.side = self.descriptive_name.split('_')[-1]
        
    def copy(self, *args, **kwargs):
        super().copy(*args, **kwargs)
        dest = args[0] or kwargs.get('dest',None) or self.expected_data
        
        def compress_image(src:pathlib.Path, dest:pathlib.Path):
            im = PIL.Image.open(src)
            im.save(dest, format='JPEG', bitmap_format= 'JPEG', quality=50)
            print(f"converted {src} ({src.stat().st_size//1024**2} MB) to {dest.name} ({dest.stat().st_size//1024**2} MB)")
        
        if (img := pathlib.Path(dest)).stat().st_size > 2*1024**2:
            compress_image(img, img)
            
    @property
    def total_imgs_per_exp(self):
        return sum('_surface_image_' in descriptive_name for descriptive_name in self.platform_json.dict_template.keys())
    
    @property
    def origin(self) -> pathlib.Path:
        if not self.total_imgs_per_exp:
            print(f"Num. total images needs to be assigned")
            return None
        glob = f"*"
        hits = get_files_created_between(self.source,glob,self.platform_json.exp_start,self.platform_json.exp_end)
        
        if len(hits) == 0:
            # print(f"No matching surface image found at origin {self.source}")
            return None
        
        right_labels_only = True if all('right' in hit.name for hit in hits) else False
        lefts_labels_only = True if all('left' in hit.name for hit in hits) else False
        equal_right_left_labels = True if sum('left' in hit.name for hit in hits) == sum('right' in hit.name for hit in hits) else False

        # need to know how many surface images there should be in total for this experiment
        if len(hits) == self.total_imgs_per_exp and equal_right_left_labels:
            # we have all expected left/right pairs of images
            # hits is sorted by creation time, so we just have to work out which pair
            # matches this entry (self), then grab the left or right image from the pair
            img_idx0 = self.lims_upload_labels.index(self.descriptive_name)
            #decsriptors are in order left, then right - return right or left of a pair
            img_idx1 = img_idx0 - 1 if img_idx0%2 else img_idx0 + 1
            return hits[img_idx0] if self.side in hits[img_idx0].name else hits[img_idx1]
        
        if len(hits) == 0.5*self.total_imgs_per_exp and right_labels_only or lefts_labels_only:
            # we have only the left or the right image for each pair
            img_idx = self.lims_upload_labels.index(self.descriptive_name)//2
            # regardless of which self.side this entry is, we have no choice but to
            # return the image that we have (relabeled inaccurately as the other side in half the cases)
            return hits[img_idx]
        print(f"{len(hits)} images found - can't determine which is {self.dir_or_file_name}")
        
# --------------------------------------------------------------------------------------
class NewscaleLog(Entry):
    lims_upload_labels = ['newstep_csv']
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)   
        self.source = self.platform_json.src_motor_locs
        
    @property
    def origin(self) -> pathlib.Path:
        log = self.source / 'log.csv'
        if log.exists():
            return log
        else:
            pass
            # print(f"No matching newscale log found at origin {self.source}")
    
    
    # TODO trim the MPM logs or copy only what's needed
    ##* the lines below will extract relevant motor locs without using pandas,
    ##* but it's unreasonably slow with 500k lines in the csv file
    # with log.open('r') as o:
    #     locs = csv.reader(o)
    #     with file.open('w') as n:
    #         locs_from_exp_date = csv.writer(n)
            
    #     for row in locs:
    #         sys.stdout.write(f"{locs.line_num}\r")
    #         sys.stdout.flush()
            
    #         if self.exp_start.strftime(R"%Y/%m/%d") in row[0]:
    #             # find csv entries recorded on the same day as the
    #             # experiment
    #             locs_from_exp_date.writerow(row)
    
# --------------------------------------------------------------------------------------
class Notebook(Entry):
    lims_upload_labels = [
                'area_classifications',
                'fiducial_image',
                'overlay_image',
                'insertion_location_image',
                'isi_registration_coordinates',
                'isi _registration_coordinates',
                'probelocator_insertions_in_vasculature_image_space',
                'probelocator_insertions_in_rig_image_space',

                ]
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)   
    
    @property
    def origin(self) -> pathlib.Path:    
        return self.z
        
# --------------------------------------------------------------------------------------
class Surgery(Entry):
    lims_upload_labels = ['surgery_notes','post_removal_surgery_image','final_surgery_image']
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)   
    
    @property
    def origin(self) -> pathlib.Path:    
        pass
        #TODO surgery notes 
    
    def copy(self):
        super().copy()
         # create an empty txt file for surgery notes if it doesn't exist
        if self.descriptive_name == 'surgery_notes':
            self.expected_data.touch()
# -------------------------------------------------------------------------------------- #
    
class Files(PlatformJson):
    """
        A subclass with more-specific methods for fixing the files manifest part of the
        platform.json, that run on initialization.
        
        Correcting the platform json and its corresponding session folder of data is a
        multi-step process:
        
            1. Deal with the data:
                for each entry in a reference/template platform json, if the expected
                file/dir doesn't exist in the session folder, copy it from the original
                source
                - seek user input to find correct file and copy it with correct name
                
            * all template entries should now have correct corresponding files 
                all(entry.correct_data for entry in Files(*.json).new_entries)
                
            
            2. Deal with the platform json 'files' dict:
                we could replace the 'files' dict with the template dict, but there may
                be entries in the files dict that we don't want to lose
                    - find entries not in template 
                    - decide what to do with their data
                    - decide whether or not to delete from the files dict
                    
                there may also be incorrect entries in the files dict that 
                correspond to incorrect data
                    - find entries that don't match template 
                    - decide whether to delete their data
                                 
            3. Update files dict with template, replacing incorrect existing entries with correct
               versions and leaving additional entries intact
            * all template entries should now be in the files dict
                Files(*.json).missing == {}
    """
    class PlatformJsonFilesTemplateNotFoundError(FileNotFoundError):
        pass
    
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs) 
        # create a placeholder for a correct set of files and their entries in
        # the 'files' dict (list is populated by the fix() function below, if necessary)
        self.entries_corrected:List[Entry] = [] 
        self.print_summary()
        
    def fix(self):
        if STAGING:
            # in Staging  mode, we do all file operations in a 'virtual' session
            # folder: we'll try to create a complete session folder of correctly named
            # experiment files, but instead of modifying the session folder the platform
            # json lives in, we'll copy it to a new folder, and instead of copying data
            # to the folder (which can take time for ephys, videos..), we'll just create
            # symlinks to what we identify are the candidate correct files. from there,
            # further validation can run on the symlinks to check their contents
            
            # - create a blank slate to work from:
            self.remake_staging_folder()
            # - now replace the linked file with the new one so that all calls to the
            #   platform json's path/parent folder will resolve to the new virtual one.
            #   this will save us from making a lot of 'if STAGING:' checks
            self.path = self.staging_folder / self.path.name
        
        # 1. Deal with the missing data/files
        self.fix_data()
        # 2. Deal with the platform json 'files' dict
        self.fix_dict()
        if self.correct_data and self.correct_dict:
            #3. Update the contents of the files dict in the platform json
            self.write()
            # (this also appends the project codename ie. OpenScopeIllusion)
            
        self.print_summary()
        # optional steps (not implemented yet)):
        # - checksum data
        # - copy data to lims incoming
        
    def print_summary(self):
        """Print a summary of session folder status"""
        
        if self.correct_dict:
            print("Platform json entries are correct")
        
        def print_staging_msg():
            print("Candidate data files have been found:")
            print(f" - check files in staging folder: {self.staging_folder}")
            print("   - if correct: set STAGING = False and re-run")
            print("   - if not: manually add files to the session folder on np-exp")
            
        if self.correct_data:
            if STAGING:
                print_staging_msg()
            else:
                print("All data files are present and correct")
        else:
            print("Some data files are missing or incorrect")
            
            if self.correct_data_ready:
                if STAGING:
                    print_staging_msg()
            else:
                print("Files need to be found: try running obj.fix()")
        if self.dict_expected_d2 == self.dict_folder_d2:
            print("All sorted probe folders are present (without info on validity):\n - Run obj.add_d2() to squash D2 upload into D1")
    
    def remake_staging_folder(self):
        self.staging_folder.mkdir(parents=True,exist_ok=True)
        [p.unlink() for p in self.staging_folder.rglob('*') if p.is_symlink()]
        # - copy the original platform json to the staging folder 
        if self.backup.exists():
            shutil.copy(self.backup,self.staging_folder)
        else:
            shutil.copy2(self.path, self.staging_folder)
            
    @property
    def staging_folder(self) -> pathlib.Path:
        """Where symlinks to data are created, instead of modifying original data.
        Created anew each time we run in Staging mode."""
        return STAGING_ROOT / self.session.folder
    
    @property
    def dict_current(self) -> dict:
        return self.contents['files']
    
    @property
    def dict_template(self) -> dict: 
        template_path = TEMPLATES_ROOT / self.session_type / f"{self.experiment}.json"
        if not template_path.exists():
            raise self.PlatformJsonFilesTemplateNotFoundError(f"File manifest template not found for {self.experiment}")
        with template_path.open('r') as f:
            return json.load(f)['files']
        
    @property
    def dict_expected(self) -> dict:
        # convert template dict to str
        # replace % with session string
        # switch ' and " so we can convert str back to dict with json.loads()
        return json.loads(str(self.dict_template).replace('%',str(self.session.folder)).replace("'",'"'))
        
    @property
    def dict_missing(self) -> dict:
        return {k:v for k,v in self.dict_expected.items() if k not in self.dict_current}
    
    @property
    def dict_extra(self) -> dict:
        return {k:v for k,v in self.dict_current.items() if k not in self.dict_expected}
    
    @property
    def dict_incorrect(self) -> dict:
        return {k:v for k,v in self.dict_current.items() if k in self.dict_expected.keys() and v != self.dict_expected[k]}
    
    @property
    def dict_corrected(self) -> dict:
        return {k:v for e in self.entries_corrected for k,v in e.__dict__().items()} if self.entries_corrected else {}
    
    @property
    def dict_folder(self) -> dict:
        return {k:v for e in self.entries_current for k,v in e.__dict__().items()} if self.entries_current else {}
    
    
    # Entry lists match dict entries to files and functions for finding/copying data ------- #
   
    def entry_from_dict(self, entry:Union[Dict,Tuple]) -> Entry:
        descriptive_name = Entry(entry,self).descriptive_name
        for entry_class in Entry.__subclasses__():
            if descriptive_name in entry_class.lims_upload_labels:
                return entry_class(entry,self)
        raise ValueError(f"{descriptive_name} is not a recognized platform.json[files] entry-type")
    
    def entry_from_file(self, path:Union[str,pathlib.Path]) -> Entry:
        path = pathlib.Path(path)
        for entry in self.dict_expected.items():
            dir_or_file_name = list(entry[1].values())[0]
            if dir_or_file_name == path.name:
                return self.entry_from_dict(entry)
        
    @property
    def entries_current(self) -> List[Entry]:
        return ([entry for entry in [self.entry_from_dict(item) for item in self.dict_current.items()]
            + [self.entry_from_file(path) for path in self.path.parent.iterdir()] if entry and entry.correct_data])
    
    @property
    def entries_expected(self) -> List[Entry]:
        return [self.entry_from_dict(entry) for entry in self.dict_expected.items()]
    
    @property
    def entries_expected_dict(self) -> dict[str,Entry]:
        return {entry[0] : self.entry_from_dict(entry) for entry in self.dict_expected.items()}
    
    @property
    def entries_missing(self) -> List[Entry]:
        if self.entries_corrected:
            return [e for e in self.entries_expected if e not in self.entries_corrected or not e.correct_data]
        return [e for e in self.entries_expected if e not in self.entries_current or not e.correct_data]

    # checks on status of current and located data ----------------------------------------- #
    @property
    def correct_data(self) -> bool:
        return all([e.correct_data for e in self.entries_expected])
    
    @property
    def correct_dict(self) -> bool:
        return (
            all(e in self.dict_current.keys() for e in self.dict_expected.keys())
            or 
            all(e in self.dict_corrected.keys() for e in self.dict_expected.keys())
        )
 
    @property
    def correct_data_ready(self) -> bool:
        # if self.entries_corrected:
        #     return all([e.correct_data for e in self.entries_corrected]) 
        # else:
        return all([e.correct_data for e in self.entries_expected]) 
        # return False


    def fix_data(self):
        """
            1. Deal with the data:
                for each entry in a reference/template platform json, if the expected
                file/dir doesn't exist in the session folder, copy it from the original
                source
                - seek user input to find correct file and copy it with correct name
                
            * all template entries should now have correct corresponding files 
                Files.correct_data = all(entry.correct_data for entry in Files(*.json).new_entries)
        """
        for entry in self.entries_expected:
            if entry.correct_data:
                self.entries_corrected.append(entry)
                continue

            entry.copy()
            
            if entry.correct_data:
                print(f"fixed {entry.dir_or_file_name}")
                self.entries_corrected.append(entry)
                continue
            print(f"need help finding {entry.dir_or_file_name}")
        
        if self.correct_data_ready:
            return
        MANUAL_MODE = False
        if not MANUAL_MODE:
           return
       
        entry_types = [e.__class__ for e in self.entries_expected if not e.correct_data]
        print(f"\n{'-'*50}\nestimated experiment start: {self.exp_start}")
        print(f"estimated experiment end: {self.exp_end}\n{'-'*50}\n")
        print("opening existing session folders on z-drive and np-exp")
        os.startfile(self.z_drive_path.parent) if self.z_drive_path else None
        self.npexp_path.parent.mkdir(exist_ok=True) # we'll dump files here regardless
        os.startfile(self.npexp_path.parent)
        
        for entry_type in entry_types:
            entries = [e for e in self.entries_expected if not e.correct_data and e.__class__ == entry_type]
            print(f"\nopening {entries[0].source}")
            os.startfile(entries[0].source)
            print("\nlocate files and copy into np-exp with correct name:\n")
            [print(e) for e in entries]
            while True:
                _ = input("Press enter to re-check, or Ctrl+C to exit")
                [e.copy() for e in entries]
                if all(e.npexp.exists() for e in entries):
                    break
                
    def fix_dict(self):
        """            
        2. Deal with the platform json 'files' dict:
            we could replace the 'files' dict with the template dict, but there may
            be entries in the files dict that we don't want to lose
                - find entries not in template 
                - decide what to do with their data
                - decide whether or not to delete from the files dict
                
            there may also be incorrect entries in the files dict that 
            correspond to incorrect data
                - find entries that don't match template 
                - decide whether to delete their data"""
                
        extra = [self.entry_from_dict({k:v}) for k,v in self.dict_extra.items()]
        for entry in extra:
            if entry.actual_data.exists():
                self.entries_corrected.append(entry)
                continue
            print(f"{entry.descriptive_name} removed from platform.json: specified data does not exist {entry.dir_or_file_name} ")

    def write(self, files: dict[str, dict[str, str]]=None):
        """Overwrite existing platform json, with a backup of the original preserved"""
        # ensure a backup of the original first
        shutil.copy2(self.path, self.backup) if not self.backup.exists() else None
        
        print(f"updating {self.path} with {len(self.dict_missing)} new entries and {len(self.dict_incorrect)} corrected entries")
        contents = self.contents # must copy contents to avoid breaking class property (Which pulls from .json)
        
        # update entries
        if files and all((self.path.parent / e).exists() for v in files.values() for _, e in v.items()):
            contents["files"] = files
        elif files:
            raise FileNotFoundError("all files in dict must exist in platform json folder")
        else:
            contents['files'] = {**self.dict_corrected} or {**self.dict_folder}
        contents['project'] = self.experiment
        if self.foraging_id:
            contents['foraging_id'] = self.foraging_id
        
        with self.path.open('w') as f:
            json.dump(dict(contents), f, indent=4)
        print(f"updated {self.path.name}")

    def push_from_here(self):
        "Write a for lims upload trigger file that points to the platform json's parent folder."
        if len(list(self.path.parent.glob("*_platform*.json"))) > 1:
            raise ValueError("session folder contains multiple platform jsons: lims will ingest data specified in all of them once triggered - ensure they're correct")

        # write a trigger file to incoming/trigger --------------------------------------------- #
        with open(INCOMING_ROOT / 'trigger' / f"{self.session.id}.ecp", 'w') as f:
            f.writelines('sessionid: ' + self.session.id + "\n")
            f.writelines("location: '" + self.path.parent.as_posix() + "'")
        print(f"Trigger file written for {self.path.name}")
    
    
    @property
    def dict_expected_d2(self) -> dict:  
        platform_files = {}
        for probe_letter in self.probe_letters_inserted:
            probe_folder = f'{self.session.folder}_probe{probe_letter}_sorted'
            probe_key = f"ephys_raw_data_probe_{probe_letter}_sorted"
        
            # add probe entry to files dict
            platform_files.update(
                {probe_key:
                    {"directory_name": probe_folder}
                    }
                )    
            
        return platform_files
    
    @property
    def dict_folder_d2(self) -> dict:
        """Return dict_expected for entries actually in the same folder as the json"""
        dict_folder = {}
        for k,v in self.dict_expected_d2.items():
            if (self.path.parent / v['directory_name']).exists():
                # add probe entry to files dict
                dict_folder.update({k:v})
                
        return dict_folder
    
    @property
    def triggered(self) -> tuple[int]:
        file = self.path.parent / '.triggered'
        if not file.exists():
            return ()
        with file.open('r') as f:
            return tuple(int(i) for i in f.readlines())
        
    @triggered.setter
    def triggered(self, value: int|tuple[int]):
        if isinstance(value, int):
            value = (value,)
        previous = self.triggered
        file = self.path.parent / '.triggered'
        file.touch()
        with file.open('w') as f:
            f.writelines(str(i) + '\n' for i in {*value,*previous})
        
    @property
    def entries_d2(self) -> list:
        return [self.entry_from_dict({k:v}) for k,v in self.dict_expected_d2.items()]
    
    def add_d2(self):
        if self.dict_expected_d2 != self.dict_folder_d2:
            print("not all sorted probe folders are present - aborting D2 platform json update")
            return
        contents = self.contents # must copy contents to avoid breaking class property (Which pulls from .json)
        contents['files'] = {**contents['files'], **self.dict_expected_d2}
        with self.path.open('w') as f:
            json.dump(dict(contents), f, indent=4)
        print(f"updated {self.path.name} with `files`")
    
    def make_d2(self):
        "Update `files` list in platformD1 to upload only sorted data from D2."
        self.update(files=dict(),project=self.experiment)
        if self.foraging_id:
            self.update(foraging_id=self.foraging_id)
        self.add_d2()
        
    def add_missing_d1_files(self):
        to_upload = {}
        missing = self.missing_from_lims_ready_on_npexp()
        
        for k,v in self.dict_expected.items():
            filename = f"{tuple(v.values())[0]}"
            if filename in missing:
                print(f"{filename} is missing from LIMS")
                to_upload[k] = v
                
        contents = self.contents # must copy contents to avoid breaking class property (Which pulls from .json)
        contents['files'] = {**contents['files'], **to_upload}
        with self.path.open('w') as f:
            json.dump(dict(contents), f, indent=4)
        print(f"updated {self.path.name} with `files`")
                
    def upload_missing_d1_only(self, override_missing_data=False):
        if 1 in self.triggered:
            print(f"{self.path.parent} already triggered for D1 upload")
            return
        if not self.correct_data and not override_missing_data:
            print(f"{self.session.folder} not all correct data present - try running `obj.fix()` or add kwarg `override_missing_data=True`")
            return
        if not hasattr(self, 'd1_df'):
            self.make_summary_dataframes()
        if self.d1_df.loc['ALL', 'on lims'] == True:
            print(f"{self.session.folder} already has all files on LIMS")
            return
        self.update(files=dict(),project=self.experiment)
        if self.foraging_id:
            self.update(foraging_id=self.foraging_id)
        self.add_missing_d1_files()
        self.ensure_single_platform_json()
        self.push_from_here()
        
    @property
    def dict_expected_d0(self) -> dict:  
        entries: list[str] = EphysRaw.lims_upload_labels
        return {k:v for k,v in self.dict_expected.items() if k in entries}
                
    @property
    def dict_folder_d0(self) -> dict:
        """Return dict_expected for entries actually in the same folder as the json"""
        dict_folder = {}
        for k,v in self.dict_expected_d0.items():
            if (self.path.parent / v['directory_name']).exists():
                # add probe entry to files dict
                dict_folder.update({k:v})
                
        return dict_folder
    
    def add_d0(self):
        if self.dict_expected_d0 != self.dict_folder_d0:
            print("not all sorted probe folders are present - aborting D0 platform json update")
            return
        contents = self.contents # must copy contents to avoid breaking class property (Which pulls from .json)
        contents['files'] = {**contents['files'], **self.dict_expected_d0}
        with self.path.open('w') as f:
            json.dump(dict(contents), f, indent=4)
        print(f"updated {self.path.name} with `files`")
        
    def make_d0_manifest(self):
        "Update `files` list in platformD1 to upload only raw data."
        self.update(files=dict(),project=self.experiment)
        if self.foraging_id:
            self.update(foraging_id=self.foraging_id)
        self.add_d0()
        
    def validate_d0(self):
        "temp - move to Entry"
        dir_sizes_gb = [
            round(dir_size(self.path.parent / v['directory_name']) / 1024**3) 
            for v in self.dict_expected_d0.values()
            ]
        if not all(gb > 300 for gb in dir_sizes_gb):
            print(f"not all raw data folders are > 300GB")
            return False
        diffs = [abs(dir_sizes_gb[0] - size) for size in dir_sizes_gb]
        if not all(diff <= 2 for diff in diffs):
            print(f"raw data folders are not all the same size")
            return False
        return True
            
    def upload_d0_only(self):
        if 0 in self.triggered:
            print(f"{self.path.parent} already triggered for D0 upload")
            return
        if not hasattr(self, 'd1_df'):
            self.make_summary_dataframes()
        if self.d1_df.loc['ALL', 'on lims'] == True:
            print(f"{self.session.folder} already has all files on LIMS")
            return
        if not self.validate_d0():
            print(f"{self.session.folder} D0 upload aborted")
            return
        self.make_d0_manifest()
        self.ensure_single_platform_json()
        self.push_from_here()
        
    def ensure_single_platform_json(self):
        """Ensure there is only one platform json in the session folder"""
        if len(jsons := list(self.path.parent.glob("*_platform*.json"))) > 1:
            for json in jsons:
                if json != self.path:
                    json.replace(json.with_suffix(".json.bak"))
                    print(f"renamed {json.name}")
                    
    def make_summary_dataframes(self, fast:bool=False):
        print(f"making summary dataframes for {self.session.folder}")
        # make d1 df --------------------------------------------------------------------------- #
        d1_df = pd.DataFrame(
        data = [ 
                (
                e.suffix, 
                # e.origin is not None and e.origin.exists(), #! skip for speed
                e.npexp.exists(),
                e.lims.exists() if e.lims else False,
                ) 
                for e 
                in self.entries_expected],
        columns=[
                self.session.folder,
                # 'at origin', #! skip for speed
                'on npexp',
                'on lims',
                ],
        )
        d1_df.loc['SUM'] = d1_df.sum()
        d1_df.loc['SUM',self.session.folder] = 'SUM'
        d1_df.loc['ALL'] = d1_df.all()
        d1_df.loc['ALL',self.session.folder] = 'ALL'

        d1_df.set_index(self.session.folder, inplace=True)

        # make d2 df --------------------------------------------------------------------------- #
        d2_df = pd.DataFrame(
        data=[
                (
                e.suffix, 
                e.npexp.exists(),
                e.lims.exists() if e.lims else 0,
                ) 
                for e 
                in self.entries_d2],
        columns=[
                self.session.folder,
                'on npexp',
                'on lims',
                ],
        )
        d2_df.loc['SUM'] = d2_df.sum()
        d2_df.loc['SUM',self.session.folder] = 'SUM'
        d2_df.loc['ALL'] = d2_df.all()
        d2_df.loc['ALL',self.session.folder] = 'ALL'

        d2_df.set_index(self.session.folder, inplace=True)
        self.d1_df = d1_df
        self.d2_df = d2_df
        
    def missing_from_lims_ready_on_npexp(self) -> list[str]:
        if not hasattr(self, 'd1_df'):
            print("Summary dataframes haven't been made yet - run `make_summary_dataframes` first")
        missing = list(self.d1_df.loc[(self.d1_df['on npexp'] == True) & (self.d1_df['on lims'] == False)].index)
        missing.remove('ALL') if 'ALL' in missing else None
        missing.remove('SUM') if 'SUM' in missing else None
        missing = [self.session.folder + m for m in missing]
        return missing

# class D2(Files):
    # """ This doesn't really make sense - we don't need to open a platform D1 json to
    # make this. But if we have the sorted data ready to go, we can push to lims from here"""
    # def __init__(self,*args,**kwargs):
    #     super().__init__(*args,**kwargs) 
    
    
        
def get_created_timestamp_from_file(file:Union[str, pathlib.Path]):
    timestamp = pathlib.Path(file).stat().st_ctime
    return datetime.datetime.fromtimestamp(timestamp)

def get_dirs_created_between(dir: Union[str, pathlib.Path], strsearch, start:datetime.datetime, end:datetime.datetime) -> List[pathlib.Path]:
    """"Returns a list of Path objects, sorted by creation time"""
    hits = []
    glob_matches = pathlib.Path(dir).glob(strsearch)
    for match in glob_matches:
        if match.is_dir():
            t = get_created_timestamp_from_file(match)
            if start <= t <= end:
                hits.append(match)
    return sorted(hits, key=get_created_timestamp_from_file)
    
def get_files_created_between(dir: Union[str, pathlib.Path], strsearch, start:datetime.datetime, end:datetime.datetime) -> List[pathlib.Path]:
    """"Returns a list of Path objects, sorted by creation time"""
    hits = []
    glob_matches = pathlib.Path(dir).rglob(strsearch)
    for match in glob_matches:
        t = get_created_timestamp_from_file(match)
        if start <= t <= end:
            hits.append(match)
    return sorted(hits, key=get_created_timestamp_from_file)

def return_single_hit(hits:List[pathlib.Path]) -> pathlib.Path:
    """Return a single hit if possible, or None if no hits.
    
    Processes the output from get_files[or dirs]_created_between() according to some
    common rule(s) 
    - (Current) take the largest filesize,
    - (add?) look for session folder string, 
    - (add?) exclude pretest/temp, 
    """
    if not hits:
        return None
    if len(hits) == 1:
        return hits[0]
    
    if len(hits) > 1 and all(h.is_file() for h in hits):
        sizes = [h.stat().st_size for h in hits if h]
        if all(s == sizes[0] for s in sizes):
            return hits[0] # all matches the same size
        else: 
            return hits[sizes.index(max(sizes))] # largest file size
    
    if len(hits) > 1 and all(h.is_dir() for h in hits):
        largest = get_largest_dir(hits)
        if largest is None:
            return hits[0] # all matches the same size
        else:
            return largest

def dir_size(dir:Union[str, pathlib.Path]) -> int:
    """Returns the size of a directory in bytes"""    
    if not dir.is_dir():
        raise ValueError(f"Not a directory: {dir}")
    return sum(f.stat().st_size for f in pathlib.Path(dir).rglob('*') if f.is_file())

def get_largest_dir(dirs:List[pathlib.Path]) -> Union[None,pathlib.Path]:
    """Return the largest directory from a list of directories, or None if all are the same size"""
    if len(dirs) == 1:
        return dirs[0]
            
    dir_sizes = [dir_size(dir) for dir in dirs]
    
    if all(sizes == dir_sizes[0] for sizes in dir_sizes):
        return None
    
    max_size_idx = dir_sizes.index(max(dir_sizes))    
    return dirs[max_size_idx]

def contains_foraging_id(string: str) -> Union[str, None]:
        """Check if a string contains the expected format for a foraging id"""
        foraging_id_re = R"([0-9,a-f]{8}-[0-9,a-f]{4}-[0-9,a-f]{4}-[0-9,a-f]{4}-[0-9,a-f]{12})"
        match = re.findall(foraging_id_re, string)
        return match[0] if match else None
    
def session_to_platform_json_path(session:Union[int, str],root:Union[str,pathlib.Path]=NPEXP_PATH)->pathlib.Path:
    """Converts a session id or folder str to a platform json file"""
    
    num_underscore = len(str(session).split('_')) - 1
    if num_underscore==2:
        file_glob = session
    elif num_underscore==0:
        file_glob = f"{session}_*_*"
    else: 
        raise ValueError(f"provide a sessionID or a full folder string: 'sessionID_mouseID_YYYYMMDD'")
    
    root = pathlib.Path(root)
    results = [path for path in root.glob(f"{file_glob}/*platform*.json")]
    if not results or len(results) == 0:
        print("session not found on np-exp - please correct")
        return
    if len(results) > 1:
        print(f"multiple platform jsons found in session folder on np-exp, returning {results[0]} ")
    return results[0]

def find_platform_json(session:Union[int,str]) -> pathlib.Path:
    """Look for platform jsons in various places and return one"""
    json_path = session_to_platform_json_path(session, NPEXP_PATH)
    if not json_path:
        try:
            json_path = session_to_platform_json_path(session, pathlib.Path("//10.128.54.19/sd9"))
        except:
            json_path = None
            pass
    if not json_path:
        for comp in ["//w10dtsm112719/","//w10dtsm18306/", "//w10dtsm18307/"]:
            json_path = session_to_platform_json_path(session, (comp+"c$/ProgramData/AIBS_MPE/neuropixels_data"))
            if json_path and json_path.exists():
                break
    return json_path 

def summary_df(platform_jsons:Sequence[PlatformJson], hide_completed_uploads=True) -> pd.DataFrame:
        
    df = {}

    df = pd.DataFrame(
        columns = [
                'session',
                'D1 npexp count',
                'All D1 on npexp',
                'D1 lims count',
                'All D1 on lims',
                'D2 npexp count',
                'All D2 on npexp',
                'D2 lims count',
                'All D2 on lims',
            ],
    )
    df.set_index('session', inplace=True)

    for files in platform_jsons:
        if not files.path.parent.exists():
            continue # in case we deleted a bad folder
        if not hasattr(files, 'd1_df'):
            files = Files(files.path)
            files.make_summary_dataframes()
        session = files.session.folder
        try:
            
            df.loc[session] = [
                    files.d1_df.loc['SUM','on npexp'],
                    bool(files.d1_df.loc['ALL','on npexp']),
                    files.d1_df.loc['SUM','on lims'],
                    bool(files.d1_df.loc['ALL','on lims']),
                    files.d2_df.loc['SUM','on npexp'],
                    bool(files.d2_df.loc['ALL','on npexp']),
                    files.d2_df.loc['SUM','on lims'],
                    bool(files.d2_df.loc['ALL','on lims']),
                ]    
        except:
            pass
        
    if hide_completed_uploads:
        return df.loc[(df['All D1 on lims'] == False) | (df['All D2 on lims'] == False)]
    else:
        return df
    
if __name__=="__main__":
    STAGING = False
    sessionID = "1222995723_632293_20221101"
    npexp_json_path = session_to_platform_json_path(sessionID, NPEXP_PATH)
    j = Files(npexp_json_path)
    print(j.entries_expected_dict['visual_stimulus'].origin)
    print(j.entries_expected_dict['behavior_stimulus'].origin)
    # j.fix()
    [print(e) for e in j.entries_missing]
    os.startfile(j.path.parent)
    j.exp_start
    j.exp_end