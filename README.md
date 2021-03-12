# Corona
Code for German Corona-Data processing.

Other languages: [Deutsch](README-de.md)

## How to
The necessary steps to download the input data and to produce the results can be performed using the `update.sh` script (execute via `source update.sh`). The default settings of this script match pavel's setup, but this can be customized via two environment variables (e.g. with `export CORONA=/path/to/my/corona/directory):
* `CORONA`: root directory of this repository
* `GOOGLE_SDK_PATH`: path to google-cloud-sdk, this is used to download the input files

## Requirements

* `google-cloud-sdk`
* `python 3.x`?
    * [datatable](https://github.com/h2oai/datatable)
    * ndjson
    * pytz
* (to be completed)