import os.path
import json

vcap = None

def get_creds():
    global vcap
    if not vcap:
        LOCAL_DIR = os.path.dirname(__file__)
        config_file = os.path.join(LOCAL_DIR, "config.json")
        if os.path.isfile(config_file):
            with open(config_file) as f:
                vcap = json.load(f)
        else:
            raise RuntimeError("missing config")
        print("woot")
        return vcap
    else:
        return vcap