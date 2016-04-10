# data-generator
Data generator that sends data to Cisco Zeus using the API

To install:
You need python 2.7

sudo pip2.7 install -r requirements.txt
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
## Sample configuration file

## Data generators with no parameters

