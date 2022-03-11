import logging
import sys

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s | %(name)s | %(message)s")
handler.setFormatter(formatter)
logger.setLevel("INFO")
logger.addHandler(handler)
