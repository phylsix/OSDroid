#!/usr/bin/env python
from flask_caching import Cache

config = {
    "CACHE_TYPE": "simple", # Flask-Caching related configs
    "CACHE_DEFAULT_TIMEOUT": 60*50, #50min
}

cache = Cache(config=config)