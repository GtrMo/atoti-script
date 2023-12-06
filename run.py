import atoti as tt

from init import start_application


def main():
    session = tt.Session()
    start_application(session)
    cube = session.cubes["sales"]
    m = cube.measures
    print(cube.query(m["Cumulative amount"]))


if __name__ == "__main__":
    main()
