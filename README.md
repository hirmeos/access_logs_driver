# Log processing
[![Build Status](https://travis-ci.org/hirmeos/log_analysis.svg?branch=master)](https://travis-ci.org/hirmeos/log_analysis)


Iterate through a bunch of gzipped Apache HTTP log files
Exclude bots, scrapers, etc., select URLs matching the provided regex(es), and generate a CSV of the relevant log entries.

Take postprocessed logs and strip out multiple hits in sessions, and
resolve URLs to the chosen `URI_SCHEME` (e.g. info:doi).

Logs come in as a CSV of 4-tuples of type
 (timestamp * IP address * URL * user agent)

We strip out entries where the same (IP address * user agent) pair has accessed
a URL within the last `SESSION_TIMEOUT` (e.g. half-hour)

Additionally, we convert the URLs to ISBNs and collate request data by date,
outputting a CSV for ingest via the stats system.

## Run via crontab
```
0 0 * * 0 docker run --rm --name "logs_analyser" --env-file /path/to/config.env -v /path/to/log/files:/logs:ro -v /somewhere/to/store/preprocessing:/usr/src/app/cache -v /somewhere/to/store/output:/usr/src/app/output openbookpublishers/log_analysis
```
