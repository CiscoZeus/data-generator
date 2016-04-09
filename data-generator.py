import json
import numpy.random
from zeus import client
from faker import Faker
import sys
import types
import dateutil.parser

in_config_filename = sys.argv[1]
zeus_token = sys.argv[2]

fakegen = Faker()



in_config = json.loads(open(in_config_filename, "r").read())


zeroset = set(["standard_cauchy", "standard_exponential", "standard_normal","geo_location","hex_color", "safe_hex_color",
              "rgb_color","company",
              "currency", "iso8601","date_time", "mime_type", "file_name","file_extension","ipv4","url","company_email","free_email",
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
             "numerify","date", "text", "words", "sentences","boolean"])
twoset = set(["randint", "beta", "binomial","f", "gamma", "gumbel", "laplace", "logistic", "lognormal", "logseries","multinomial",
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

if timestamp_config["generate"] == "one-time":
  if "start_time" not in timestamp_config:
    raise Exception("one-time data generation, but start_time not specified for timestamp")
else:
  if "start_time" in timestamp_config:
    raise Exception("live data generation, but start_time specified for timestamp")
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
      else:
        raise Exception("Internal error: field '" + field_name + "', field type '" + field_type + "' does not have an function")
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

def get_datetime(datetime_str):
  return dateutil.parser.parse(datetime_str)

def generate_entry(timeval, conf):
  ret_json = {"@timestamp", timeval}
  for field_name, field_config in conf.items():
    ret_json = {field_name:call_func(field_config)}
  

for field_name, field_config in in_config.items():
  check_field(field_name, field_config)
if timestamp_config["generate"] == "one-time":
  curr_time = get_datetime(timestamp_config["start_time"])
  total_delay = 0
  while(total_delay < timestamp_config["duration"]):
    next_json = generate_entry(curr_time, in_config)
    print(json.dumps(next_json))
    delay_val = call_func(timestamp_config["arrival_function"])
    total_delay += delay_val
    curr_time = add_delay(curr_time, delay_val)
else:
  #live
  curr_time = get_now_datetime()
  total_delay = 0
  while(total_delay < timestamp_config["duration"]):
    next_json = generate_entry(curr_time, in_config)
    print(json.dumps(next_json))
    delay_val = call_func(timestamp_config["arrival_function"])
    total_delay += delay_val
    time.sleep(delay_val)