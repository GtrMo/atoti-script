import logging
from pathlib import Path
import sys
import json
from typing import cast

import atoti as tt

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(process)s --- [%(threadName)s] %(name)s : %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
_LOGGER = logging.getLogger(__name__)

CUBE_NAME = "sales"

SALES_TABLE_NAME = "sales"
PRODUCTS_TABLE_NAME = "products"
WAREHOUSES_TABLE_NAME = "warehouses"
USERS_TABLE_NAME = "buyers"


def start_application(session: tt.Session):
    _LOGGER.info("Starting init script")
    load_data(session)
    cube = create_cube(session)
    define_measures(session, cube)


def create_cube(session: tt.Session):
    sales_table = session.tables[SALES_TABLE_NAME]
    return session.create_cube(sales_table, name=CUBE_NAME)


def load_data(session: tt.Session):
    from atoti_cloud_engine.data_source_setup import generate_data_source_file

    data_sources_path = cast(Path, generate_data_source_file(Path(".")))
    with data_sources_path.open() as f:
        data_sources = json.load(f)
    databricks = data_sources["databricks"]["options"]

    session.tables[SALES_TABLE_NAME].load_jdbc(
        "select * from visual_cube_builder_catalog.demo.sales",
        url=databricks["url"],
        driver=databricks["driverClassName"],
    )
    session.tables[PRODUCTS_TABLE_NAME].load_jdbc(
        "select * from visual_cube_builder_catalog.demo.products",
        url=databricks["url"],
        driver=databricks["driverClassName"],
    )
    session.tables[WAREHOUSES_TABLE_NAME].load_jdbc(
        "select * from visual_cube_builder_catalog.demo.warehouses",
        url=databricks["url"],
        driver=databricks["driverClassName"],
    )


def define_measures(session: tt.Session, cube: tt.Cube):
    _LOGGER.info("Defining measures")
    l, m = cube.levels, cube.measures  # noqa: E741

    sales_table = session.tables[SALES_TABLE_NAME]
    products_table = session.tables[PRODUCTS_TABLE_NAME]


def main():
    with tt.Session._connect("127.0.0.1") as session:
        start_application(session)


def local_main():
    with tt.Session() as session:
        start_application(session)


if __name__ == "__main__":
    main()
