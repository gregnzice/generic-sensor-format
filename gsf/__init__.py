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

These files have binary records that have the data in big endian.
"""

import datetime
import inspect
import os
import struct
import sys

__version__ = '0.2'

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

# Subrecord identifiers
DEPTH_ARRAY = 1
ACROSS_TRACK_ARRAY = 2
ALONG_TRACK_ARRAY = 3
TRAVEL_TIME_ARRAY = 4
BEAM_ANGLE_ARRAY = 5
MEAN_CAL_AMPLITUDE_ARRAY = 6
MEAN_REL_AMPLITUDE_ARRAY = 7
ECHO_WIDTH_ARRAY = 8
QUALITY_FACTOR_ARRAY = 9
RECEIVE_HEAVE_ARRAY = 10
NOMINAL_DEPTH_ARRAY = 14
QUALITY_FLAGS_ARRAY = 15
BEAM_FLAGS_ARRAY = 16
SIGNAL_TO_NOISE_ARRAY = 17
BEAM_ANGLE_FORWARD_ARRAY = 18
VERTICAL_ERROR_ARRAY = 19
HORIZONTAL_ERROR_ARRAY = 20
INTENSITY_SERIES_ARRAY = 21
SECTOR_NUMBER_ARRAY = 22
DETECTION_INFO_ARRAY = 23
INCIDENT_BEAM_ADJ_ARRAY = 24
SYSTEM_CLEANING_ARRAY = 25
DOPPLER_CORRECTION_ARRAY = 26
SONAR_VERT_UNCERTAINTY_ARRAY = 27
SCALE_FACTORS = 100
R2SONIC_2022_SPECIFIC = 151
R2SONIC_2024_SPECIFIC = 152
R2SONIC_2O2O_SPECIFIC = 153


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

SUBRECORD_TYPES = {
        1: 'DEPTH_ARRAY',
        2: 'ACROSS_TRACK_ARRAY',
        3: 'ALONG_TRACK_ARRAY',
        4: 'TRAVEL_TIME_ARRAY',
        5: 'BEAM_ANGLE_ARRAY',
        6: 'MEAN_CAL_AMPLITUDE_ARRAY',
        7: 'MEAN_REL_AMPLITUDE_ARRAY',
        8: 'ECHO_WIDTH_ARRAY',
        9: 'QUALITY_FACTOR_ARRAY',
        10: 'RECEIVE_HEAVE_ARRAY',
        14: 'NOMINAL_DEPTH_ARRAY',
        16: 'BEAM_FLAGS_ARRAY',
        18: 'BEAM_ANGLE_FORWARD_ARRAY',
        19: 'VERTICAL_ERROR_ARRAY',
        20: 'HORIZONTAL_ERROR_ARRAY',
        21: 'INTENSITY_SERIES_ARRAY',
        22: 'SECTOR_NUMBER_ARRAY',
        151: 'R2SONIC_2022_SPECIFIC',
        152: 'R2SONIC_2024_SPECIFIC',
        100: 'SCALE_FACTORS',
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

def GsfProcessingParameters(data):
    param_size = [0]
    param_text = []
    sec = struct.unpack('>I', data[:4])[0]
    nsec = struct.unpack('>I', data[4:8])[0]
    when = datetime.datetime.utcfromtimestamp(sec + 1e-9 * nsec)
    num_params = struct.unpack('>H', data[8:10])[0]
    for i in range(num_params):
        param_size.append(struct.unpack('>H', data[10 + sum(param_size):12 +
                                                   sum(param_size)])[0]+2)
        param_text.append(struct.unpack('>' + str(param_size[-1] - 3) + 's',
                                        data[12 + sum(param_size[:-1]):
                                             12 + sum(param_size) - 3])[0]
                          .decode("utf-8"))
    return {'record_type': GSF_PROCESSING_PARAMETERS,
            'datetime': when,
            # 'num_parameters': num_params,
            # 'param_size': param_size[1:],
            'param_text': param_text,
            }
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


def GsfSbp(data, num_factors, scale_factors):
    sec = struct.unpack('>I', data[:4])[0]
    nsec = struct.unpack('>I', data[4:8])[0]
    when = datetime.datetime.utcfromtimestamp(sec + 1e-9 * nsec)
    longitude = struct.unpack('>i', data[8:12])[0] / 1.0e7
    latitude = struct.unpack('>i', data[12:16])[0] / 1.0e7
    num_beams = struct.unpack('>H', data[16:18])[0]
    center_beam = struct.unpack('>H', data[18:20])[0]
    ping_flags = struct.unpack('>B', data[20:21])[0]
    LEADING_BIT_MASK = 0b10000000
    VALUE_MASK = 0b01111111
    value = ping_flags & VALUE_MASK
    has_leading_bit = value & LEADING_BIT_MASK
    if has_leading_bit:
        ping_flag = 1
    else:
        ping_flag = 0
    tide_corrector = struct.unpack('>h', data[24:26])[0]
    depth_corrector = struct.unpack('>I', data[26:30])[0]
    heading = struct.unpack('>H', data[30:32])[0]/100.
    pitch = struct.unpack('>h', data[32:34])[0]/100.
    roll = struct.unpack('>h', data[34:36])[0]/100.
    heave = struct.unpack('>h', data[36:38])[0]/100.
    course = struct.unpack('>H', data[38:40])[0]/100.
    speed = struct.unpack('>H', data[40:42])[0]/100.
    height = struct.unpack('>i', data[42:46])[0]
    separation = struct.unpack('>i', data[46:50])[0]
    gps_tide_corrector = struct.unpack('>i', data[50:54])[0]
    # Now we get into the subrecords
    subrecord_ids = []
    subrecord_sizes = []
    sensor_record = {}
    index = 56
    while index < len(data):
        subrecord_ids.append(SUBRECORD_TYPES[
                                             struct.unpack('>B', data[
                                                     index:index+1])[0]])
        subrecord_sizes.append(struct.unpack('>I', b'\x00' +
                                             data[index+1:index+4])[0])
        if subrecord_ids[-1] == 'SCALE_FACTORS':
            num_factors = struct.unpack('>I', data[index+4:index+8])[0]
            comflag = []
            recID = []
            SM = []
            SO = []
            for i in range(num_factors):
                recID.append(SUBRECORD_TYPES[struct.unpack('>B',
                                                           data[index + 8
                                                                + i * 12:
                                                                index + 9
                                                                + i * 12]
                                                           )[0]])

                comflag.append(int((struct.unpack('>B', data[index + 9 + i *
                                                             12:index + 10 +
                                                             i * 12])[0]
                                   & 0b11110000) / 16))

                SM.append(struct.unpack('>I',
                                        data[index + 12
                                             + i * 12:
                                             index + 16
                                             + i * 12])[0])
                SO.append(struct.unpack('>i',
                                        data[index + 16
                                             + i * 12:
                                             index + 20
                                             + i * 12])[0])
            scale_factors = [comflag, recID, SM, SO]
        if (subrecord_ids[-1] == 'DEPTH_ARRAY'):
            depth_array = []
            idx = list(scale_factors[1]).index('DEPTH_ARRAY')
            multiplier = scale_factors[0][idx]
            for i in range(num_beams):
                if multiplier == 2:
                    depth_array.append(
                            struct.unpack('>H',
                                          data[index + multiplier
                                               * i + 4:index +
                                               multiplier * i +
                                               multiplier + 4])[0]
                            / scale_factors[2][idx] - scale_factors[3][idx])
        if (subrecord_ids[-1] == 'ACROSS_TRACK_ARRAY'):
            across_track_array = []
            idx = list(scale_factors[1]).index('ACROSS_TRACK_ARRAY')
            multiplier = scale_factors[0][idx]
            for i in range(num_beams):
                if multiplier == 2:
                    across_track_array.append(
                            struct.unpack('>h',
                                          data[index + multiplier
                                               * i + 4:index +
                                               multiplier * i +
                                               multiplier + 4])[0]
                            / scale_factors[2][idx] - scale_factors[3][idx])

        if (subrecord_ids[-1] == 'ALONG_TRACK_ARRAY'):
            along_track_array = []
            idx = list(scale_factors[1]).index('ALONG_TRACK_ARRAY')
            multiplier = scale_factors[0][idx]
            for i in range(num_beams):
                if multiplier == 2:
                    along_track_array.append(
                            struct.unpack('>h',
                                          data[index + multiplier
                                               * i + 4:index +
                                               multiplier * i +
                                               multiplier + 4])[0]
                            / scale_factors[2][idx] - scale_factors[3][idx])

        if (subrecord_ids[-1] == 'TRAVEL_TIME_ARRAY'):
            travel_time_array = []
            idx = list(scale_factors[1]).index('TRAVEL_TIME_ARRAY')
            multiplier = scale_factors[0][idx]
            for i in range(num_beams):
                if multiplier == 4:
                    travel_time_array.append(
                            struct.unpack('>I',
                                          data[index + multiplier
                                               * i + 4:index +
                                               multiplier * i +
                                               multiplier + 4])[0]
                            / scale_factors[2][idx] - scale_factors[3][idx])

        if (subrecord_ids[-1] == 'BEAM_ANGLE_ARRAY'):
            beam_angle_array = []
            idx = list(scale_factors[1]).index('BEAM_ANGLE_ARRAY')
            multiplier = 2
            for i in range(num_beams):
                if multiplier == 2:
                    beam_angle_array.append(
                            struct.unpack('>h',
                                          data[index + multiplier
                                               * i + 4:index +
                                               multiplier * i +
                                               multiplier + 4])[0]
                            / scale_factors[2][idx] - scale_factors[3][idx])

        if (subrecord_ids[-1] == 'MEAN_REL_AMPLITUDE_ARRAY'):
            mean_rel_amplitude_array = []
            idx = list(scale_factors[1]).index('MEAN_CAL_AMPLITUDE_ARRAY')
            multiplier = scale_factors[0][idx]
            for i in range(num_beams):
                if multiplier == 2:
                    mean_rel_amplitude_array.append(
                            struct.unpack('>h',
                                          data[index + multiplier
                                               * i + 4:index +
                                               multiplier * i +
                                               multiplier + 4])[0]
                            / scale_factors[2][idx] - scale_factors[3][idx])

        if (subrecord_ids[-1] == 'QUALITY_FACTOR_ARRAY'):
            quality_factor_array = []
            idx = list(scale_factors[1]).index('QUALITY_FACTOR_ARRAY')
            for i in range(num_beams):

                quality_factor_array.append(int(
                        struct.unpack('>B',
                                      data[index +
                                           i + 4:index +
                                           i +
                                           1 + 4])[0]
                        / scale_factors[2][idx] - scale_factors[3][idx]))

        if (subrecord_ids[-1] == 'BEAM_FLAGS_ARRAY'):
            beam_flags_array = []
            idx = list(scale_factors[1]).index('BEAM_FLAGS_ARRAY')
            for i in range(num_beams):

                beam_flags_array.append(int(
                        struct.unpack('>B',
                                      data[index +
                                           i + 4:index +
                                           i +
                                           1 + 4])[0]
                        / scale_factors[2][idx] - scale_factors[3][idx]))

        if (subrecord_ids[-1] == 'BEAM_ANGLE_FORWARD_ARRAY'):
            beam_angle_forward_array = []
            idx = list(scale_factors[1]).index('BEAM_ANGLE_FORWARD_ARRAY')
            multiplier = 2
            for i in range(num_beams):
                if multiplier == 2:
                    beam_angle_forward_array.append(
                            struct.unpack('>h',
                                          data[index + multiplier
                                               * i + 4:index +
                                               multiplier * i +
                                               multiplier + 4])[0]
                            / scale_factors[2][idx] - scale_factors[3][idx])

        if (subrecord_ids[-1] == 'VERTICAL_ERROR_ARRAY'):
            vertical_error_array = []
            idx = list(scale_factors[1]).index('VERTICAL_ERROR_ARRAY')
            multiplier = 2
            for i in range(num_beams):
                if multiplier == 2:
                    vertical_error_array.append(
                            struct.unpack('>H',
                                          data[index + multiplier
                                               * i + 4:index +
                                               multiplier * i +
                                               multiplier + 4])[0]
                            / scale_factors[2][idx] - scale_factors[3][idx])

        if (subrecord_ids[-1] == 'HORIZONTAL_ERROR_ARRAY'):
            horizontal_error_array = []
            idx = list(scale_factors[1]).index('HORIZONTAL_ERROR_ARRAY')
            multiplier = 2
            for i in range(num_beams):
                if multiplier == 2:
                    horizontal_error_array.append(
                            struct.unpack('>H',
                                          data[index + multiplier
                                               * i + 4:index +
                                               multiplier * i +
                                               multiplier + 4])[0]
                            / scale_factors[2][idx] - scale_factors[3][idx])

        if (subrecord_ids[-1] == 'SECTOR_NUMBER_ARRAY'):
            sector_number_array = []
            idx = list(scale_factors[1]).index('SECTOR_NUMBER_ARRAY')
            for i in range(num_beams):

                sector_number_array.append(int(
                        struct.unpack('>B',
                                      data[index +
                                           i + 4:index +
                                           i +
                                           1 + 4])[0]
                        / scale_factors[2][idx] - scale_factors[3][idx]))

        if (subrecord_ids[-1] == 'INTENSITY_SERIES_ARRAY'):

            bits_per_sample = struct.unpack('>B', data[index:index + 1])[0]
            applied_corrections = struct.unpack('>I', data[index + 1:
                                                           index + 5])[0]
            spare1 = struct.unpack('>16s', data[index + 5:index + 21])[0]
            MODEL_NUMBER = struct.unpack('>4s', data[index+25:index+29])[0]
            MODEL_NUMBER = MODEL_NUMBER.decode("utf-8")
            SERIAL_NUMBER = struct.unpack('>6s', data[index+37:index+43])[0]
            SERIAL_NUMBER = SERIAL_NUMBER.decode("utf-8")
            sec2 = struct.unpack('>I', data[index+49:index+53])[0]
            nsec2 = struct.unpack('>I', data[index+53:index+57])[0]
            when2 = datetime.datetime.utcfromtimestamp(sec2 + 1e-9 * nsec2)
            ping_number = struct.unpack('>I', data[index+57:index+61])[0]
            ping_period = struct.unpack('>I', data[index+61:index+65])[0]
            sound_speed = struct.unpack('>I', data[index+65:index+69])[0]/100.
            frequency = struct.unpack('>I', data[index+69:index+73])[0]/1000.
            tx_power = struct.unpack('>I', data[index+73:index+77])[0]
            tx_pulse_width = struct.unpack('>I', data[index+77:index+81])[0]
            beamwidth_vert = struct.unpack('>I', data[index+81:index+85])[0]
            beamwidth_horiz = struct.unpack('>I', data[index+85:index+89])[0]
            tx_steering_vert = struct.unpack('>I', data[index+89:index+93])[0]
            tx_steering_horiz = struct.unpack('>I', data[index+93:index+97])[0]
            tx_misc_info = struct.unpack('>I', data[index+97:index+101])[0]
            rx_bandwidth = struct.unpack('>I', data[index+101:index+105])[0]
            rx_sample_rate = struct.unpack('>I', data[index+105:index+109])[0]
            rx_range = struct.unpack('>I', data[index+109:index+113])[0]
            rx_gain = struct.unpack('>I', data[index+113:index+117])[0]
            rx_spreading = struct.unpack('>I', data[index+117:index+121])[0]
            rx_absorption = struct.unpack('>I', data[index+121:index+125])[0]
            rx_mount_tilt = struct.unpack('>I', data[index+125:index+129])[0]
            rx_misc_info = struct.unpack('>I', data[index+129:index+133])[0]
            num_beams = struct.unpack('>H', data[index+135:index+137])[0]
            more_info1 = struct.unpack('>I', data[index+137:index+141])[0]
            more_info2 = struct.unpack('>I', data[index+141:index+145])[0]
            more_info3 = struct.unpack('>I', data[index+145:index+149])[0]
            more_info4 = struct.unpack('>I', data[index+149:index+153])[0]
            more_info5 = struct.unpack('>I', data[index+153:index+157])[0]
            more_info6 = struct.unpack('>I', data[index+157:index+161])[0]
            spare2 = struct.unpack('>32s', data[index+161:index+193])[0]

            offset = 0
            multiplier = 0
            sample_count = []
            detect_sample = []
            samples_array = [[] for i in range(num_beams)]
            for i in range(num_beams):
                sample_count.append(struct.unpack('>H',
                                                  data[index + 193 + offset:
                                                       index + 193 + offset
                                                       + 2])[0])
                detect_sample.append(struct.unpack('>H',
                                                   data[index + 193 + offset +
                                                        2:index + 193
                                                        + offset + 2 + 2])[0])
                for j in range(sample_count[-1]):
                    samples_array[i].append(struct.unpack('>H',
                                                          data[index + 197 +
                                                               offset + 2 * j:
                                                               index + 197 +
                                                               offset + 2 * j
                                                               + 2])[0])
                multiplier = 2 + 2 + 8 + sample_count[-1] * 2
                offset += multiplier

            intensity_record = {'bits_per_sample': bits_per_sample,
                                'applied_corrections': applied_corrections,
                                'model_number': MODEL_NUMBER,
                                'serial_number': SERIAL_NUMBER,
                                'datetime': when2,
                                'ping_number': ping_number,
                                'ping_period': ping_period,
                                'sound_speed': sound_speed,
                                'frequency': frequency,
                                'tx_power': tx_power,
                                'tx_pulse_width': tx_pulse_width,
                                'beamwidth_vert': beamwidth_vert,
                                'beamwidth_horiz': beamwidth_horiz,
                                'tx_steering_vert': tx_steering_vert,
                                'tx_steering_horiz': tx_steering_horiz,
                                'tx_misc_info': tx_misc_info,
                                'rx_bandwidth': rx_bandwidth,
                                'rx_sample_rate': rx_sample_rate,
                                'rx_range': rx_range,
                                'rx_gain': rx_gain,
                                'rx_spreading': rx_spreading,
                                'rx_absorption': rx_absorption,
                                'rx_mount_tilt': rx_mount_tilt,
                                'rx_misc_info': rx_misc_info,
                                'num_beams': num_beams,
                                'more_info1': more_info1,
                                'more_info2': more_info2,
                                'more_info3': more_info3,
                                'more_info4': more_info4,
                                'more_info5': more_info5,
                                'more_info6': more_info6,
                                'sample_count': sample_count,
                                'detect_sample': detect_sample,
                                'samples_array': samples_array,
                                }

        if (subrecord_ids[-1] == 'R2SONIC_2024_SPECIFIC'
           or subrecord_ids[-1] == 'R2SONIC_2022_SPECIFIC'):
            MODEL_NUMBER = struct.unpack('>4s', data[index+4:index+8])[0]
            MODEL_NUMBER = MODEL_NUMBER.decode("utf-8")
            SERIAL_NUMBER = struct.unpack('>6s', data[index+16:index+22])[0]
            SERIAL_NUMBER = SERIAL_NUMBER.decode("utf-8")
            sec2 = struct.unpack('>I', data[index+28:index+32])[0]
            nsec2 = struct.unpack('>I', data[index+32:index+36])[0]
            when2 = datetime.datetime.utcfromtimestamp(sec2 + 1e-9 * nsec2)
            ping_number = struct.unpack('>I', data[index+36:index+40])[0]
            ping_period = struct.unpack('>I', data[index+40:index+44])[0]
            sound_speed = struct.unpack('>I', data[index+44:index+48])[0]/100.
            frequency = struct.unpack('>I', data[index+48:index+52])[0]/1000.
            tx_power = struct.unpack('>I', data[index+52:index+56])[0]
            tx_pulse_width = struct.unpack('>I', data[index+56:index+60])[0]
            beamwidth_vert = struct.unpack('>I', data[index+60:index+64])[0]
            beamwidth_horiz = struct.unpack('>I', data[index+64:index+68])[0]
            tx_steering_vert = struct.unpack('>I', data[index+68:index+72])[0]
            tx_steering_horiz = struct.unpack('>I', data[index+72:index+76])[0]
            tx_misc_info = struct.unpack('>I', data[index+76:index+80])[0]
            rx_bandwidth = struct.unpack('>I', data[index+80:index+84])[0]
            rx_sample_rate = struct.unpack('>I', data[index+84:index+88])[0]
            rx_range = struct.unpack('>I', data[index+88:index+92])[0]
            rx_gain = struct.unpack('>I', data[index+92:index+96])[0]
            rx_spreading = struct.unpack('>I', data[index+96:index+100])[0]
            rx_absorption = struct.unpack('>I', data[index+100:index+104])[0]
            rx_mount_tilt = struct.unpack('>I', data[index+104:index+108])[0]
            rx_misc_info = struct.unpack('>I', data[index+108:index+112])[0]
            num_beams = struct.unpack('>H', data[index+114:index+116])[0]
            sensor_record = {'model_number': MODEL_NUMBER,
                             'serial_number': SERIAL_NUMBER,
                             'datetime': when2,
                             'ping_number': ping_number,
                             'ping_period': ping_period,
                             'sound_speed': sound_speed,
                             'frequency': frequency,
                             'tx_power': tx_power,
                             'tx_pulse_width': tx_pulse_width,
                             'beamwidth_vert': beamwidth_vert,
                             'beamwidth_horiz': beamwidth_horiz,
                             'tx_steering_vert': tx_steering_vert,
                             'tx_steering_horiz': tx_steering_horiz,
                             'tx_misc_info': tx_misc_info,
                             'rx_bandwidth': rx_bandwidth,
                             'rx_sample_rate': rx_sample_rate,
                             'rx_range': rx_range,
                             'rx_gain': rx_gain,
                             'rx_spreading': rx_spreading,
                             'rx_absorption': rx_absorption,
                             'rx_mount_tilt': rx_mount_tilt,
                             'rx_misc_info': rx_misc_info,
                             'num_beams': num_beams,
                             }


        index += subrecord_sizes[-1] + 4

    return {
            'record_type': GSF_SWATH_BATHYMETRY_PING,
            'sec': sec,
            'nsec': nsec,
            'datetime': when,
            'latitude': latitude,
            'longitude': longitude,
            'number_of_beams': num_beams,
            'center_beam': center_beam,
            'ping_flag': ping_flag,
            'tide_corrector': tide_corrector,
            'depth_corrector': depth_corrector,
            'heading': heading,
            'pitch': pitch,
            'roll': roll,
            'heave': heave,
            'course': course,
            'speed': speed,
            'height': height,
            'separation': separation,
            'gps_tide_corrector': gps_tide_corrector,
            'subrecord_ids': subrecord_ids,
            'num_factors': num_factors,
            'scale_factors': scale_factors,
            'subrecord_sizes': subrecord_sizes,
            'sensor_record': sensor_record,
            'depth_array': depth_array,
            'across_track_array': across_track_array,
            'along_track_array': along_track_array,
            'travel_time_array': travel_time_array,
            'beam_angle_array': beam_angle_array,
            'mean_rel_amplitude_array': mean_rel_amplitude_array,
            'quality_factor_array': quality_factor_array,
            'beam_flags_array': beam_flags_array,
            'beam_angle_forward_array': beam_angle_forward_array,
            'vertical_error_array': vertical_error_array,
            'horizontal_error_array': horizontal_error_array,
            'sector_number_array': sector_number_array,
            'intensity_record': intensity_record,
            }




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
