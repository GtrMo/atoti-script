import logging
import sys
import atoti as tt
from pathlib import Path

logging.basicConfig(
    level=15,
    format="%(levelname)s %(process)s --- [%(threadName)s] %(name)s : %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.addLevelName(15, "FINE")
_LOGGER = logging.getLogger("PnlExplained")


def main():
    with tt.Session._connect("127.0.0.1") as session:
        start(session)


def local_main():
    with tt.Session(
        user_content_storage=Path(__file__).with_name("content")
    ) as session:
        start(session)
        session.wait()


def start(session: tt.Session):
    _LOGGER.info("Loading tables")

    position_sensitivity = session.read_csv(
        "s3://data.atoti.io/notebooks/pnl-explained/position_sensitivities.csv",
        keys=["book_id", "instrument_code", "currency", "curve", "tenor"],
        table_name="Position_Sensitivities",
    )
    _log_table(position_sensitivity)

    position_table = session.read_csv(
        "s3://data.atoti.io/notebooks/pnl-explained/position_data.csv",
        keys=["book_id", "instrument_code"],
        table_name="Position",
    )
    _log_table(position_table)

    trading_desk_table = session.read_csv(
        "s3://data.atoti.io/notebooks/pnl-explained/trading_desk.csv",
        keys=["book_id"],
        table_name="Trading_Desk",
    )
    _log_table(trading_desk_table)

    market_data_table = session.read_csv(
        "s3://data.atoti.io/notebooks/pnl-explained/market_data.csv",
        keys=["currency", "curve", "tenor"],
        table_name="Market_Data",
    )
    _log_table(market_data_table)

    position_sensitivity.join(
        trading_desk_table,
        (position_sensitivity["book_id"] == trading_desk_table["book_id"]),
    )

    position_sensitivity.join(
        position_table,
        (position_sensitivity["book_id"] == position_table["book_id"])
        & (position_sensitivity["instrument_code"] == position_table["instrument_code"])
        & (position_sensitivity["currency"] == position_table["currency"])
        & (position_sensitivity["curve"] == position_table["curve"]),
    )

    position_sensitivity.join(
        market_data_table,
        (position_sensitivity["currency"] == market_data_table["currency"])
        & (position_sensitivity["curve"] == market_data_table["curve"])
        & (position_sensitivity["tenor"] == market_data_table["tenor"]),
    )

    cube = session.create_cube(position_sensitivity, "Position_Sensitivities")
    _LOGGER.info("Cube `%s` created", cube.name)

    _LOGGER.info("Creating measures")
    m, h, lvl = cube.measures, cube.hierarchies, cube.levels
    m["last.VALUE"] = tt.agg.single_value(market_data_table["last"])
    m["start_of_day.VALUE"] = tt.agg.single_value(market_data_table["start_of_day"])

    curve_simulation = cube.create_parameter_simulation(
        "Curve Simulation",
        measures={"last parameter": 0.0},
        levels=[lvl["tenor"], lvl[("Position_Sensitivities", "currency", "currency")]],
        base_scenario_name="Last Curve",
    )
    m["last parameter"].formatter = "DOUBLE[#.000]"

    m["effective last"] = tt.agg.sum(
        m["last.VALUE"] + m["last parameter"],
        scope=tt.OriginScope(
            lvl["tenor"], lvl[("Position_Sensitivities", "currency", "currency")]
        ),
    )

    m["Theoretical PnL"] = tt.agg.sum(
        m["sensi.SUM"]
        * (m["effective last"] - m["start_of_day.VALUE"])
        * m["notional.SUM"],
        scope=tt.OriginScope(
            lvl[("Position_Sensitivities", "currency", "currency")],
            lvl[("Position_Sensitivities", "curve", "curve")],
            lvl["tenor"],
            lvl["book_id"],
            lvl["instrument_code"],
        ),
    )

    h["Investment Portfolio Hierarchy"] = {
        "Asset Class": lvl["asset_class"],
        "Sub Asset Class": lvl["sub_asset_class"],
        "Fund": lvl["fund"],
        "Portfolio": lvl["portfolio"],
    }
    h["Trading Book Hierarchy"] = {
        "Business Unit": lvl["business_unit"],
        "Sub Business Unit": lvl["sub_business_unit"],
        "Trading Desk": lvl["trading_desk"],
        "Book": lvl["book"],
    }

    curve_simulation += ("Curve Parallel Shift", None, "EUR", -0.001)
    curve_simulation += ("Curve Inversion", "5Y", "EUR", -0.002)
    curve_simulation += ("Curve Inversion", "6Y", "EUR", -0.002)
    curve_simulation += ("Curve Inversion", "7Y", "EUR", -0.002)
    curve_simulation += ("Curve Inversion", "8Y", "EUR", -0.002)
    curve_simulation += ("Curve Inversion", "9Y", "EUR", -0.002)
    curve_simulation += ("Curve Inversion Stress", "5Y", "EUR", -0.002)
    curve_simulation += ("Curve Inversion Stress", "6Y", "EUR", -0.002)
    curve_simulation += ("Curve Inversion Stress", "7Y", "EUR", -0.004)
    curve_simulation += ("Curve Inversion Stress", "8Y", "EUR", -0.004)
    curve_simulation += ("Curve Inversion Stress", "9Y", "EUR", -0.004)

    _LOGGER.info("UI: %s", session.link)


def _log_table(table: tt.Table):
    _LOGGER.log(15, "Table %s loaded. Head:\n%s\n", table.name, table.head())
