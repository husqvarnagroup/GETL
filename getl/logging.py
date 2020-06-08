"""Setup logging environment"""
import logging

# Setup logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)-1s:%(levelname)-8s %(message)s"
)

# Setting the threshold of py4j.java_gateway to WARNING
# To avoid spam in databricks logs
logging.getLogger("py4j.java_gateway").setLevel(logging.WARNING)


def get_logger(name):
    return logging.getLogger(name)
