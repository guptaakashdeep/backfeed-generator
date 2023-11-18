from utils import read_yaml

ENV=''
BASE_LOCATION = f'/home/user/utilities/backfeed_generator{ENV}'
BASEL_BACKFEED_CONFIG = f"{BASE_LOCATION}/config/backfeed_config.yaml"


def read_basel_backfeed():
    return read_yaml(BASEL_BACKFEED_CONFIG)