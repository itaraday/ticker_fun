import os.path
import json

vcap = None

def get_creds(env="dev"):
    global vcap
    if not vcap:
        LOCAL_DIR = os.path.dirname(__file__)
        config_file = os.path.join(LOCAL_DIR, f"config_{env}.json")
        if os.path.isfile(config_file):
            with open(config_file) as f:
                vcap = json.load(f)
        else:
            raise RuntimeError("missing config")
        return vcap
    else:
        return vcap