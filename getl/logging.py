"""Setup logging environment"""
import logging
import logging.config
import re


class SecretWordFilter(logging.Filter):
    def __init__(self, param=None):
        self.param = param

    def filter(self, record):
        msg = str(record.msg)
        for x in self.param:
            if x in msg.lower():
                word_finder = re.compile(rf"({x}':) (\S+)'", re.DOTALL | re.IGNORECASE)
                record.msg = re.sub(word_finder, r"\1 #redacted#", msg)
        return True


LOGGING = {
    "version": 1,
    "formatters": {
        "detailed": {
            "class": "logging.Formatter",
            "format": "%(asctime)s %(name)-1s:%(levelname)-8s %(message)s",
        }
    },
    "filters": {
        "myfilter": {
            "()": SecretWordFilter,
            "param": ["password", "secret", "connurl"],
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "filters": ["myfilter"],
            "formatter": "detailed",
        }
    },
    "root": {"level": "INFO", "handlers": ["console"]},
}

logging.config.dictConfig(LOGGING)
# Setting the threshold of py4j.java_gateway to WARNING
# To avoid spam in databricks logs
logging.getLogger("py4j.java_gateway").setLevel(logging.WARNING)


def get_logger(name):
    return logging.getLogger(name)
