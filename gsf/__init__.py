# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Read generic sensor format (GSF) sonar files.

These files have binary records that have the data in big engian.
"""

import datetime
import inspect
import os
import struct
import sys

__version__ = '0.1'

CHECKSUM_MASK = 0x80000000
RESERVED_MASK = 0x7FC00000
TYPE_MASK = 0x003FFFFF

GSF_HEADER = 1
GSF_SWATH_BATHYMETRY_PING = 2
GSF_SOUND_VELOCITY_PROFILE = 3
GSF_PROCESSING_PARAMETERS = 4
GSF_SENSOR_PARAMETERS = 5
GSF_COMMENT = 6
GSF_HISTORY = 7
GSF_NAVIGATION_ERROR = 8
GSF_SWATH_BATHY_SUMMARY = 9
GSF_SINGLE_BEAM_PING = 10
GSF_HV_NAVIGATION_ERROR = 11
GSF_ATTITUDE = 12

RECORD_TYPES = {
    1: 'HEADER',
    2: 'SWATH_BATHYMETRY_PING',
    3: 'SOUND_VELOCITY_PROFILE',
    4: 'PROCESSING_PARAMETERS',
    5: 'SENSOR_PARAMETERS',
    6: 'COMMENT',
    7: 'HISTORY',
    8: 'NAVIGATION_ERROR',
    9: 'SWATH_BATHY_SUMMARY',
    10: 'SINGLE_BEAM_PING',
    11: 'HV_NAVIGATION_ERROR',
    12: 'ATTITUDE',
}


class Error(Exception):
  pass


def Checkpoint():
  """Get a string saying where this function was called from."""

  frame = inspect.currentframe().f_back
  filename = os.path.basename(inspect.stack()[1][1])
  line = frame.f_lineno
  code = frame.f_code.co_name
  return '%s:%d: %s() CHECKPOINT' % (filename, line, code)


def GsfHeader(data):
  version = data.rstrip('\0')
  version_major, version_minor = version.split('v')[1].split('.')
  version_major = int(version_major.lstrip('0'))
  version_minor = int(version_minor.lstrip('0'))
  return {
      'record_type': GSF_HEADER,
      'version': version,
      'version_major': version_major,
      'version_minor': version_minor
  }


def GsfAttitude(data):
  sec = struct.unpack('>I', data[:4])[0]
  nsec = struct.unpack('>I', data[4:8])[0]
  base_time = datetime.datetime.utcfromtimestamp(sec + 1e-9 * nsec)
  num_measurements = struct.unpack('>h', data[8:10])[0]

  result = {
      'record_type': GSF_ATTITUDE,
      'sec': sec,
      'nsec': nsec,
      'datetime': base_time,
      'times': [],
      'pitches': [],
      'rolls': [],
      'heaves': [],
      'headings': [],
  }

  if not num_measurements:
    return result

  base = 10
  # 5 2-byte values.
  record_size = 10
  for rec_num in range(num_measurements):
    start = base + rec_num * record_size
    end = base + (rec_num + 1) * record_size

    fields = struct.unpack('>4hH', data[start:end])
    time_raw, pitch_raw, roll_raw, heave_raw, heading_raw = fields
    offset = time_raw / 1000.0
    time = base_time + datetime.timedelta(seconds=offset)

    result['times'].append(time)
    result['pitches'].append(pitch_raw / 100.0)
    result['rolls'].append(roll_raw / 100.0)
    result['heaves'].append(heave_raw / 100.0)
    result['headings'].append(heading_raw / 100.0)

  return result


def GsfComment(data):
  """Decode a GSF Comment record from binary data.

  Record type 5.

  Args:
    data: String of binary data.
  """
  sec = struct.unpack('>I', data[:4])[0]
  nsec = struct.unpack('>I', data[4:8])[0]
  size = struct.unpack('>I', data[8:12])[0]
  comment = data[12:12 + size].rstrip('\0')
  return {
      'record_type': GSF_COMMENT,
      'sec': sec,
      'nsec': nsec,
      'datetime': datetime.datetime.utcfromtimestamp(sec + 1e-9 * nsec),
      'comment': comment
  }


def GsfHistory(data):
  sec = struct.unpack('>I', data[:4])[0]
  nsec = struct.unpack('>I', data[4:8])[0]
  name_size = struct.unpack('>h', data[8:10])[0]
  name = data[10:10 + name_size].rstrip('\0')
  base = 10 + name_size

  operator_size = struct.unpack('>h', data[base:base+2])[0]
  base += 2
  if operator_size:
    operator = data[base:base + operator_size].rstrip('\0')
    base += operator_size
  else:
    operator = ''

  command_size = struct.unpack('>h', data[base:base+2])[0]
  base += 2
  if command_size:
    command = data[base:base + command_size].rstrip('\0')
    base += command_size
  else:
    command = ''

  comment_size = struct.unpack('>h', data[base:base+2])[0]
  base += 2
  if comment_size:
    comment = data[base:base + comment_size].rstrip('\0')
  else:
    comment = ''

  return {
      'record_type': GSF_HISTORY,
      'sec': sec,
      'nsec': nsec,
      'datetime': datetime.datetime.utcfromtimestamp(sec + 1e-9 * nsec),
      'name': name,
      'operator': operator,
      'command': command,
      'comment': comment
  }


# TODO(schwehr): GSF_RECORD_HV_NAVIGATION_ERROR


def GsfHvNavigationError(data):
  # TODO(schwehr): Check the length of data is at least 26.
  sec = struct.unpack('>I', data[:4])[0]
  nsec = struct.unpack('>I', data[4:8])[0]
  when = datetime.datetime.utcfromtimestamp(sec + 1e-9 * nsec)
  record_id = struct.unpack('>i', data[8:12])[0]
  horizontal_error = struct.unpack('>i', data[12:16])[0] / 1000.0
  vertical_error = struct.unpack('>i', data[16:20])[0] / 1000.0
  sep_uncertainty = struct.unpack('>H', data[20:22])[0] / 100.0
  spare = data[22:24]
  len_pos_type = struct.unpack('>H', data[24:26])[0]
  # TODO(schwehr): Check the length of data is 26 + len_pos_type.
  if not len_pos_type:
    pos_type = ''
  else:
    pos_type = data[26:]

  return {
      'record_type': GSF_HV_NAVIGATION_ERROR,
      'sec': sec,
      'nsec': nsec,
      'datetime': when,
      'record_id': record_id,
      'horizontal_error': horizontal_error,
      'vertical_error': vertical_error,
      'sep_uncertainty': sep_uncertainty,
      'spare': spare,
      'position_type': pos_type,
  }

# TODO(schwehr): GSF_RECORD_NAVIGATION_ERROR


def GsfNavigationError(data):
  # if len(data) != 20:
  #   raise
  sec = struct.unpack('>I', data[:4])[0]
  nsec = struct.unpack('>I', data[4:8])[0]
  when = datetime.datetime.utcfromtimestamp(sec + 1e-9 * nsec)
  record_id = struct.unpack('>i', data[8:12])[0]
  longitude_error = struct.unpack('>i', data[12:16])[0] / 10.0
  latitude_error = struct.unpack('>i', data[16:20])[0] / 10.0

  return {
      'record_type': GSF_NAVIGATION_ERROR,
      'sec': sec,
      'nsec': nsec,
      'datetime': when,
      'record_id': record_id,
      'longitude_error': longitude_error,
      'latitude_error': latitude_error,
  }


# TODO(schwehr): GSF_RECORD_PROCESSING_PARAMETERS
# TODO(schwehr): GSF_RECORD_SENSOR_PARAMETERS
# TODO(schwehr): GSF_RECORD_SINGLE_BEAM_PING


def GsfSvp(data):
  # if len(data) < ??:
  #   raise
  sec = struct.unpack('>I', data[:4])[0]
  nsec = struct.unpack('>I', data[4:8])[0]
  when = datetime.datetime.utcfromtimestamp(sec + 1e-9 * nsec)
  application_sec = struct.unpack('>I', data[8:12])[0]
  application_nsec = struct.unpack('>I', data[12:16])[0]
  application_when = datetime.datetime.utcfromtimestamp(
      application_sec + 1e-9 * application_nsec)

  longitude = struct.unpack('>i', data[16:20])[0] / 1.0e7
  latitude = struct.unpack('>i', data[20:24])[0] / 1.0e7

  num_points = struct.unpack('>I', data[24:28])[0]
  depth = []
  sound_speed = []

  for i in range(num_points):
    start = 28 + 8 * i
    end = 28 + 8 * (i + 1)
    depth_raw, sound_speed_raw = struct.unpack('>2I', data[start:end])
    depth.append(depth_raw / 100.0)
    sound_speed.append(sound_speed_raw / 100.0)

  return {
      'record_type': GSF_SOUND_VELOCITY_PROFILE,
      'sec': sec,
      'nsec': nsec,
      'datetime': when,
      'application_sec': application_sec,
      'application_nsec': application_nsec,
      'application_datetime': application_when,
      'latitude': latitude,
      'longitude': longitude,
      'depth': depth,
      'sound_speed': sound_speed,
  }


# TODO(schwehr): GSF_RECORD_SWATH_BATHYMETRY_PING
# TODO(schwehr): GSF_RECORD_SWATH_BATHY_SUMMARY


class GsfFile(object):
  """A simple GSF file reader."""

  def __init__(self, filename):
    self.filename = filename
    self.src = open(filename, 'rb')
    self.size = os.path.getsize(filename)

  def __iter__(self):
    return GsfIterator(self)


class GsfIterator(object):

  def __init__(self, gsf_file):
    self.gsf_file = gsf_file

  def __iter__(self):
    return self

  def __next__(self):
    if self.gsf_file.src.tell() >= self.gsf_file.size:
      raise StopIteration

    record_header_text = self.gsf_file.src.read(8)
    data_size = struct.unpack('>I', record_header_text[:4])[0]
    record_id = struct.unpack('>I', record_header_text[4:])[0]
    record_type = record_id & TYPE_MASK
    reserved = record_id & RESERVED_MASK
    have_checksum = record_id & CHECKSUM_MASK
    checksum = None
    header_data = record_header_text
    if have_checksum:
      checksum_text = self.gsf_file.src.read(4)
      checksum = struct.unpack('>I', checksum_text)
      header_data += checksum_text

    data = self.gsf_file.src.read(data_size)

    record = {
        'size_total': len(header_data) + len(data),
        'size_data': len(data),
        'record_type': record_type,
        'record_type_str': RECORD_TYPES[record_type],
        'reserved': reserved,
        'checksum': checksum,
        'header_data': header_data,
        'data': data
    }

    # TODO(schwehr): Wrap in try, except and handle malformed records.
    if record_type == GSF_HEADER:
      record.update(GsfHeader(data))
    elif record_type == GSF_SWATH_BATHYMETRY_PING:
      pass
    elif record_type == GSF_SOUND_VELOCITY_PROFILE:
      pass
    elif record_type == GSF_PROCESSING_PARAMETERS:
      pass
    elif record_type == GSF_SENSOR_PARAMETERS:
      pass
    elif record_type == GSF_COMMENT:
      record.update(GsfComment(data))
    elif record_type == GSF_HISTORY:
      record.update(GsfHistory(data))
    elif record_type == GSF_NAVIGATION_ERROR:
      pass
    elif record_type == GSF_SWATH_BATHY_SUMMARY:
      pass
    elif record_type == GSF_SINGLE_BEAM_PING:
      pass
    elif record_type == GSF_HV_NAVIGATION_ERROR:
      pass
    elif record_type == GSF_ATTITUDE:
      record.update(GsfAttitude(data))
    else:
      raise Error('Unknown record_type: %d' % record_type)

    return record

  next = __next__

