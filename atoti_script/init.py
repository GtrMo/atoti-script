import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(process)s --- [%(threadName)s] %(name)s : %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
_LOGGER = logging.getLogger(__name__)


def start_application():
    _LOGGER.info("Starting init script")
    _LOGGER.warning("Failing")
    sys.exit(1)


def main():
    start_application()


def local_main():
    start_application()


if __name__ == "__main__":
    main()
