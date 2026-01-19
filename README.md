# NCEISync
NCEISync: Automated NCEI Data Sync via Web Crawler


# Quick Start
## Requests
- Installed `uv` (**Recommend**, See also: [https://docs.astral.sh/uv/](https://docs.astral.sh/uv/)) or `pipx` (**Untested**)
- Installed `curl` (See also: [https://curl.se/](https://curl.se/))

If you no sure whether there are really, use `uv --version` or `curl --version`.

## Install
```shell
uv tool install 'nceisync-0.1.1-py3-none-any.whl'
```
replace with your actual .whl path.

## Usage
```shell
NCEISync  \
  --url="https://www.ncei.noaa.gov/pub/data/igra" \
  --save-dir="./igra_data" \
  --log="./sync_logs"
```

It mean:
  From `https://www.ncei.noaa.gov/pub/data/igra` download all files (include files in its sub-dir) to `./output`, and log file will be saved to `./log/`;
  If log dir is same as last running, the tool will skip the file exist.

Using `NCEISync --help` for all option

**Notice:**
- The tool use database to record a file have been downloaded. If file exist and have been downloaded, it will be skipped whether it is correct or not.
- The database is in log dir. If you want to re-download all files, please check the log dir is empty or not exist.

