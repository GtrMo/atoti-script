import logging
import subprocess as sp
import sys


logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(process)s --- [%(threadName)s] %(name)s : %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
_LOGGER = logging.getLogger("Script System info")


def start_application():
    _log_executed_command(["whoami"])
    _log_executed_command(["id", "-u"])
    _log_executed_command(["id", "-g"])
    _log_executed_command(["groups"])
    _log_executed_command(["users"])


def _log_executed_command(command: list[str]):
    output = sp.check_output(command, shell=True).decode()
    _LOGGER.info("`%s`: %s", " ".join(command), output)


def main():
    start_application()


def local_main():
    start_application()


if __name__ == "__main__":
    main()
