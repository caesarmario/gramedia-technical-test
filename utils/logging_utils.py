####
## Gramedia Digital - Data Engineer Take Home Test
## Mario Caesar // caesarmario87@gmail.com
## Logging utility file for logging purposes
####

# Importing Libraries
import logging
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)

for handler in list(logger.handlers):
    logger.removeHandler(handler)

formatter = logging.Formatter(
    '%(asctime)s - %(filename)s - Line: %(lineno)d - %(levelname)s - %(message)s'
)

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(formatter)
logger.addHandler(stdout_handler)
