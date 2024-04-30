import logging
from pathlib import Path
from shutil import rmtree
import sys

import atoti as tt
from atoti.copy_tutorial import _copy_tutorial

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(process)s --- [%(threadName)s] %(name)s : %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
_LOGGER = logging.getLogger(__name__)

CUBE_NAME = "Sales"

SALES_TABLE_NAME = "sales"
PRODUCTS_TABLE_NAME = "products"
SHOPS_TABLE_NAME = "shops"


def start_application(session: tt.Session):
    _LOGGER.info("Starting init script")
    tutorial_path = Path() / "tutorial"
    if tutorial_path.exists():
        rmtree(tutorial_path)

    _copy_tutorial(tutorial_path)
    cube = create_cube(session, tutorial_path / "data")
    define_measures(session, cube)


def create_cube(session: tt.Session, data_path: Path):
    _LOGGER.info("Creating cube")
    sales_table = session.read_csv(
        data_path / "sales.csv", keys=["Sale ID"], table_name=SALES_TABLE_NAME
    )

    products_table = session.read_csv(
        data_path / "products.csv", keys=["Product"], table_name=PRODUCTS_TABLE_NAME
    )
    sales_table.join(
        products_table, sales_table["Product"] == products_table["Product"]
    )

    shops_table = session.read_csv(
        data_path / "shops.csv", keys=["Shop ID"], table_name=SHOPS_TABLE_NAME
    )
    sales_table.join(shops_table, sales_table["Shop"] == shops_table["Shop ID"])

    return session.create_cube(sales_table, name=CUBE_NAME)


def define_measures(session: tt.Session, cube: tt.Cube):
    _LOGGER.info("Defining measures")
    l, m = cube.levels, cube.measures  # noqa: E741

    sales_table = session.tables[SALES_TABLE_NAME]
    products_table = session.tables[PRODUCTS_TABLE_NAME]

    m["Max price"] = tt.agg.max(sales_table["Unit price"])
    m["Amount.SUM"] = tt.agg.sum(sales_table["Quantity"] * sales_table["Unit price"])
    m["Amount.MEAN"] = tt.agg.mean(
        sales_table["Quantity"] * sales_table["Unit price"],
    )
    cost = tt.agg.sum(
        m["Quantity.SUM"] * tt.agg.single_value(products_table["Purchase price"]),
        scope=tt.OriginScope(l["Product"]),
    )
    m["Margin"] = m["Amount.SUM"] - cost
    m["Margin rate"] = m["Margin"] / m["Amount.SUM"]
    m["Cumulative amount"] = tt.agg.sum(
        m["Amount.SUM"], scope=tt.CumulativeScope(level=l["Date"])
    )
    m["Average amount per shop"] = tt.agg.mean(
        m["Amount.SUM"], scope=tt.OriginScope(l["Shop"])
    )

    for measure in [
        m["Amount.MEAN"],
        m["Amount.SUM"],
        m["Average amount per shop"],
        m["Cumulative amount"],
    ]:
        measure.folder = "Amount"


def main():
    with tt.Session._connect("127.0.0.1") as session:
        start_application(session)


def local_main():
    with tt.Session() as session:
        start_application(session)


if __name__ == "__main__":
    main()
