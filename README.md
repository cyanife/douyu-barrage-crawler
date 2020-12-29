# Douyu Barrage Crawler

## Quickstart

### Worker

```shell
python worker.py
```

### Commands

```shell
# create room management table
python crawler.py migrate

# list all crawlers
python crawler.py list

# start crawler
python crawler.py start 9999

# pause crawler
python crawler.py pause 9999

# resume paused crawler
python crawler.py pause 9999 -r

# stop crawler
python crawler.py stop 9999
```
