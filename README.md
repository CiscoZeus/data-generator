# data-generator
Data generator that sends data to Cisco Zeus using the API. Currently has over 90 different data generation options, such as normal, poisson, cauchy, exponential, uniform, pareto, weibull, various text fields, geo location,  etc.

To install:
You need python 2.7
```
git clone https://github.com/CiscoZeus/data-generator
cd data-generator
sudo pip2.7 install -r requirements.txt
```
## Command line parameters
```
usage: python2.7 data-generator.py [-h] -c CONFIG_FILE -t ZEUS_TOKEN [-n]

Generate data and send to Zeus.

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG_FILE, --config_file CONFIG_FILE
                        Configuration file
  -t ZEUS_TOKEN, --zeus_token ZEUS_TOKEN
                        Zeus token
  -n, --dry_run         Print only, do not send to Zeus
```
## Sample configuration
Generate live data, for 100 seconds, with uniform rate of arrival (1 second). For every data point, send geoip.location centered on North Carolina, and temperature value based on a normal distribution with a mean of 55 and standard deviation of 30.

```
python2.7 data-generator.py -c config3.json -t <ZEUS_TOKEN>

{
"timestamp": {
  "generate": "live",
  "duration": 100,
  "arrival_function" : ["uniform",[1,1]]
},
"geoip.location": ["geo_range",[35.227088928223,-80.843132019043,2]],
"temperature":["normal",[55,30]]
}
```
### Other configuration options
Over 90 data generators are available, please see documentation here:

https://support.ciscozeus.io/support/solutions/articles/9000061262-cisco-zeus-data-generator-documentation

Also see the file config.json for examples of usage

