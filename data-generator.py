import json
import numpy.random
from zeus import client
from faker import Faker
import sys
import types
import dateutil.parser
import datetime
import argparse
import time
from dateutil.tz import *

parser = argparse.ArgumentParser(description='Generate data and send to Zeus.')
parser.add_argument("-c",'--config_file', required=True,
                   help='Configuration file')
parser.add_argument("-t",'--zeus_token', required=True,
                   help='Zeus token')
parser.add_argument("-n",'--dry_run', dest='dry_run', action='store_true', required=False,
                   help='Print only, do not send to zeus')

args = parser.parse_args()


in_config_filename = args.config_file
zeus_token = args.zeus_token
print("dry_run " + str(args.dry_run))

fakegen = Faker()



in_config = json.loads(open(in_config_filename, "r").read())


zeroset = set(["standard_cauchy", "standard_exponential", "standard_normal","geo_location","hex_color", "safe_hex_color",
              "rgb_color","company",
              "currency_code", "iso8601", "mime_type", "file_name","file_extension","ipv4","url","company_email","free_email",
              "domain_name","ipv6","safe_email", "user_name", "email", "mac_address", "word", "sentence", "password", "locale",
              "md5", "sha1", "sha256", "uuid4", "language_code", "null_boolean",
              "name","name_male", "name_female",
              "prefix", "prefix_male", "prefix_female",
              "suffix", "suffix_male", "suffix_female", 
              "first_name", "first_name_male", "first_name_female",
              "last_name", "last_name_male", "last_name_female",
              "phone_number", "ssn", "user_agent", "linux_processor", "linux_platform_token", "mac_processor",
              "credit_card_security_code", "credit_card_number", "credit_card_expire", "credit_card_provider"])
oneset = set(["chisquare", "exponential", "geometric", "pareto", "poisson", "power", "rayleigh", "standard_t", "weibull", "zipf",
             "numerify","date", "text", "words", "boolean"])
twoset = set(["randint", "beta", "binomial","f", "gamma", "gumbel", "laplace", "logistic", "lognormal",
             "negative_binomial", "noncentral_chisquare", "normal", "uniform", "vonmises", "wald", "paragraph"])
threeset = set(["hypergeometric", "noncentral_f", "triangular","geo_range"])


if type(in_config) != type({}):
  raise Exception("Configuration is not a json map")
  
if "timestamp" not in in_config:
  raise Exception("The timestamp field is required")
timestamp_config = in_config["timestamp"]
if ("generate" not in timestamp_config):
  raise Exception("'generate' not specified for timestamp")
if ("arrival_function" not in timestamp_config):
  raise Exception("'arrival_function' not specified for timestamp")

if timestamp_config["generate"] not in ("live", "one-time"):
  raise Exception("generate config for timestamp should be either 'live' or 'one-time'")

if "duration" not in timestamp_config:
  raise Exception("duration not specified for timestamp")
else:
  #print("duration = " + str(timestamp_config["duration"]))
  pass

if timestamp_config["generate"] == "one-time":
  if "start_time" not in timestamp_config:
    raise Exception("one-time data generation, but start_time not specified for timestamp")
else:
  if "start_time" in timestamp_config:
    raise Exception("live data generation, but start_time specified for timestamp")
def get_geo_location():
  return str(fakegen.latitude()) + ", " + str(fakegen.longitude())
def get_geo_range(lat,long,radius):
  return str(fakegen.geo_coordinate(center=lat, radius=radius)) + ", " + str(fakegen.geo_coordinate(center=long, radius=radius))
def check_field(field_name, field_config):
  if isinstance(field_config,types.UnicodeType):
    field_config = [field_config]
  if isinstance(field_config, types.ListType):
    if (len(field_config) == 0):
      raise Exception("field type for field '" + field_name + "' not specified")
    field_type = field_config[0]
    if len(field_config) == 1:
      field_config.append([])
    if (len(field_config) == 2):
      params = field_config[1]
      if not isinstance(params, types.ListType):
        raise Exception("params not array for field '" + field_name + "', field type '" + field_type + "'")
      if field_type in zeroset:
        if (len(params) != 0):
          raise Exception("number of params incorrect for field '" + field_name + "', field type '" + field_type + "' (should not be specified, or an empty array)")
      elif field_type in oneset:
        if (len(params) != 1):
          raise Exception("number of params incorrect for field '" + field_name + "', field type '" + field_type + "' (should be 1)")
      elif field_type in twoset:
        if (len(params) != 2):
          raise Exception("number of params incorrect for field '" + field_name + "', field type '" + field_type + "' (should be 2)")
      elif field_type in threeset:
        if (len(params) != 3):
          raise Exception("number of params incorrect for field '" + field_name + "', field type '" + field_type + "' (should be 3)")
      else:
        raise Exception("field '" + field_name + "', has an invalid field type '" + field_type + "'")
      if hasattr(numpy.random, field_type):
        field_config.append(getattr(numpy.random, field_type))
      elif hasattr(fakegen, field_type):
        field_config.append(getattr(fakegen, field_type))
      elif field_type == "geo_location":
        field_config.append(get_geo_location)
      elif field_type == "geo_range":
        field_config.append(get_geo_range)
      else:
        raise Exception("Internal error: field '" + field_name + "', field type '" + field_type + "' does not have a function")
      return field_config
    else:
      # too long
      raise Exception("field '" + field_name + "' should map to an array of size 1 or 2")
  else:
    raise Exception("field '" + field_name + "' has invalid config")


check_field("arrival_function", timestamp_config["arrival_function"])
def call_func(call_config):
  if call_config[0] in zeroset:
    return call_config[2]()
  elif call_config[0] in oneset:
    return call_config[2](call_config[1][0])
  elif call_config[0] in twoset:
    return call_config[2](call_config[1][0], call_config[1][1])
  elif call_config[0] in threeset:
    return call_config[2](call_config[1][0], call_config[1][1], call_config[1][2])
  else:
    raise Exception("Internal error: Unexpected call config " + json.dumps(call_config))

delay_val = call_func(timestamp_config["arrival_function"])
if not isinstance(delay_val, int) and not isinstance(delay_val, float):
  raise Exception("timestamp arrival_function does not generate a number")

in_config.pop("timestamp")
def add_delay(timeval, delay):
  return timeval + datetime.timedelta(0,delay)
def get_datetime(datetime_str):
  return dateutil.parser.parse(datetime_str)

def generate_entry(timeval, conf):
  ret_json = {"@timestamp": timeval.isoformat()}
  for field_name, field_config in conf.items():
    try:
      ret_json[field_name] = call_func(field_config)
    except:
      print("Error generating '" + field_name + "', type '" + field_config[0] + "'")
      raise
  return ret_json
  
modified_config = {}
if not args.dry_run:
  z = client.ZeusClient(zeus_token, 'api.ciscozeus.io')
for field_name, field_config in in_config.items():
  mod_field_config = check_field(field_name, field_config)
  modified_config[field_name] = mod_field_config
if timestamp_config["generate"] == "one-time":
  curr_time = get_datetime(timestamp_config["start_time"])
  total_delay = 0
  while(total_delay < timestamp_config["duration"]):
    next_json = generate_entry(curr_time, modified_config)
    try:
      print(json.dumps(next_json))
      if not args.dry_run:
        z.sendLog("temperature",[next_json])
    except:
      print("Problem sending output " + str(next_json))
      raise
    delay_val = call_func(timestamp_config["arrival_function"])
    total_delay += delay_val
    curr_time = add_delay(curr_time, delay_val)
else:
  #live
  curr_time = datetime.datetime.now(tzlocal())
  total_delay = 0
  while(total_delay < timestamp_config["duration"]):
    next_json = generate_entry(curr_time, modified_config)
    #next_json["timestamp"] = str(curr_time)
    try:
      print(json.dumps(next_json))
      if not args.dry_run:
        z.sendLog("temperature",[next_json])
    except:
      print("Problem sending output " + str(next_json))
      raise
    delay_val = call_func(timestamp_config["arrival_function"])
    #print("delay_val " + str(delay_val))
    total_delay += delay_val
    #print("total_delay " + str(total_delay))
    time.sleep(delay_val)