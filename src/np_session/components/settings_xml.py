"""
>>> path = '//allen/programs/mindscope/workgroups/np-exp/1222995723_632293_20221101/1222995723_632293_20221101_probeABC/Record Node 101/settings.xml'
>>> et = ET.parse(pathlib.Path(path))
>>> hostname(et)
'W10DT713843'
>>> date_time(et)
(datetime.date(2022, 11, 1), datetime.time(14, 55, 18))
>>> open_ephys_version(et)
'0.6.2'
>>> settings_xml_md5(path)
'6a118788a0ee875cc4183ce0bf68aa65'
>>> probe_serial_numbers(et)
(18005117142, 18005117312, 18194822412, 18005117641, 18005107542, 18005118212)
>>> probe_letters(et)
('A', 'B', 'C', 'D', 'E', 'F')
>>> probe_types(et)
('Neuropixels 1.0', 'Neuropixels 1.0', 'Neuropixels 1.0', 'Neuropixels 1.0', 'Neuropixels 1.0', 'Neuropixels 1.0')
>>> isinstance(settings_xml_info_from_path(path), SettingsXmlInfo)
True
"""
from __future__ import annotations

import dataclasses
import datetime
import doctest
import functools
import hashlib
import pathlib
import xml.etree.ElementTree as ET


@dataclasses.dataclass
class SettingsXmlInfo:
    """Info from a settings.xml file from an Open Ephys recording."""
    path: pathlib.Path
    probe_serial_numbers: tuple[int, ...]
    probe_types: tuple[str, ...]
    probe_letters: tuple[str, ...]
    hostname: str
    date: datetime.date
    start_time: datetime.time
    open_ephys_version: str
    settings_xml_md5: str

def settings_xml_info_from_path(path: str | pathlib.Path) -> SettingsXmlInfo:
    """Info from a settings.xml file from an Open Ephys recording."""
    path = pathlib.Path(path)
    et = ET.parse(path)
    return SettingsXmlInfo(
        path=path,
        probe_serial_numbers=probe_serial_numbers(et),
        probe_types=probe_types(et),
        probe_letters=probe_letters(et),
        hostname=hostname(et),
        date=date_time(et)[0],
        start_time=date_time(et)[1],
        open_ephys_version=open_ephys_version(et),
        settings_xml_md5=settings_xml_md5(path),
    )
    
def get_tag_text(et: ET.ElementTree, tag: str) -> str | None:
    result = [element.text for element in et.getroot().iter() if element.tag == tag.upper()]
    if not (result and any(result)):
        result = [element.attrib.get(tag.lower()) for element in et.getroot().iter()]
    return str(result[0]) if (result and any(result)) else None

def get_tag_attrib(et: ET.ElementTree, tag: str, attrib: str) -> str | None:
    result = [element.attrib.get(attrib) for element in et.getroot().iter() if element.tag == tag.upper()]
    return str(result[0]) if (result and any(result)) else None

def hostname(et: ET.ElementTree) -> str:
    result = (
        # older, pre-0.6.x:
        get_tag_text(et, 'machine')
        # newer, 0.6.x:
        or get_tag_attrib(et, 'MACHINE', 'name')
    )
    if not result:
        raise LookupError(f'No hostname: {result!r}')
    return result

@functools.lru_cache(maxsize=None)
def date_time(et: ET.ElementTree) -> tuple[datetime.date, datetime.time]:
    """Date and recording start time."""
    result = get_tag_text(et, 'date')
    if not result:
        raise LookupError(f'No datetime found: {result!r}')
    dt = datetime.datetime.strptime(result, '%d %b %Y %H:%M:%S')
    return dt.date(), dt.time()

@functools.lru_cache(maxsize=None)
def probe_attrib_dicts(et: ET.ElementTree) -> tuple[dict[str, str], ...]:
    return tuple(
        probe_dict.attrib
        for probe_dict in et.getroot().iter()
        if 'probe_serial_number' in probe_dict.attrib
    )

def probe_attrib(et: ET.ElementTree, attrib: str) -> tuple[str, ...]:
    return tuple(probe[attrib] for probe in probe_attrib_dicts(et))

def probe_serial_numbers(et: ET.ElementTree) -> tuple[int, ...]:
    return tuple(int(_) for _ in probe_attrib(et, 'probe_serial_number'))

def probe_types(et: ET.ElementTree) -> tuple[str, ...]:
    try:
        return probe_attrib(et, 'probe_name')
    except KeyError:
        return tuple('unknown' for _ in probe_attrib_dicts(et))
    
def probe_idx(et: ET.ElementTree) -> tuple[int, ...]:
    """Try to reconstruct index from probe slot and port.
    
    Normally 2 slots: each with 3 ports in use.
    """
    slots, ports = probe_attrib(et, 'slot'), probe_attrib(et, 'port')
    result = tuple((int(s) - int(min(slots))) * len(set(ports)) + int(p) - 1 for s, p in zip(slots, ports))
    if not all(idx in range(6) for idx in result):
        raise ValueError(f'probe_idx: {result!r}, slots: {slots}, ports: {ports}')
    return result

def probe_letters(et: ET.ElementTree) -> tuple[str, ...]:
    return tuple('ABCDEF'[idx] for idx in probe_idx(et))

def open_ephys_version(et: ET.ElementTree) -> str:
    result = get_tag_text(et, 'version')
    if not result:
        raise LookupError(f'No version found: {result!r}')
    return result

def settings_xml_md5(path: str | pathlib.Path) -> str:
    return hashlib.md5(pathlib.Path(path).read_bytes()).hexdigest()

if __name__ == '__main__':
    doctest.testmod(verbose=True)