import argparse
import psycopg2
from config.settings import POSTGRES
from config.settings import ROOMS_TABLE_NAME


def _execute_sql(
    query,
    param=None,
):
    conn = psycopg2.connect(
        host=POSTGRES["host"],
        port=POSTGRES["port"],
        user=POSTGRES["user"],
        password=POSTGRES["password"],
        database=POSTGRES["database"],
    )

    try:
        with conn:
            with conn.cursor() as curs:
                if param:
                    curs.execute(query, param)
                    if curs.description:
                        return curs.fetchall()
                else:
                    curs.execute(query)
                    if curs.description:
                        return curs.fetchall()

    except psycopg2.errors.UndefinedTable:
        print(f'Room table "{ROOMS_TABLE_NAME}" dose not exists. Run `migrate` first.')
    except psycopg2.errors.UniqueViolation:
        print(f"Room ID: {param[0]} has already started.")

    finally:
        conn.close()


def _migrate(args):
    query = f"""
        CREATE TABLE IF NOT EXISTS "{ROOMS_TABLE_NAME}" (
        room_id VARCHAR(20) PRIMARY KEY,
        is_paused BOOLEAN NOT NULL
        );
    """
    _execute_sql(query)
    print("Migration finished!")


def _list(args):
    query = f"""
        SELECT * FROM "{ROOMS_TABLE_NAME}";
    """
    res = _execute_sql(query)
    for roomid, is_paused in res:
        print(f"Room ID: {roomid}, Status: {'PAUSED'if is_paused else 'RUNNING'}")


def _start(args):
    query = f"""
        INSERT INTO "{ROOMS_TABLE_NAME}" (room_id, is_paused) VALUES (%s, %s);
    """
    _execute_sql(query, (args.roomid, False))
    print(f"Room ID: {args.roomid}, Status: RUNNING")


def _stop(args):
    query = f"""
        DELETE FROM "{ROOMS_TABLE_NAME}" WHERE room_id=(%s);
    """
    _execute_sql(query, (args.roomid,))
    print(f"Crawler Stopped. Room ID: {args.roomid}")


def _pause(args):
    query = f"""
        UPDATE "{ROOMS_TABLE_NAME}" SET is_paused=(%s) WHERE room_id=(%s) RETURNING *;
    """
    res = _execute_sql(query, (not args.resume, args.roomid))
    print(res)
    if res:
        print(
            f"Room ID: {args.roomid}, Status: {'RUNNING' if args.resume else 'PAUSED'}"
        )
    else:
        print(f"Room not found. ID: {args.roomid}")


def _init_subparsers(parent):
    sp = parent.add_subparsers(title="actions", required=True, dest="{action}")
    sp_migrate = sp.add_parser("migrate", help="migrate %(prog)s management table")
    sp_list = sp.add_parser("list", help="list all %(prog)s")
    sp_start = sp.add_parser("start", help="Starts %(prog)s")
    sp_stop = sp.add_parser("stop", help="Stops %(prog)s")
    sp_pause = sp.add_parser("pause", help="Pauses %(prog)s")

    sp_start.add_argument("roomid", help="Douyu room id", type=str)
    sp_stop.add_argument("roomid", help="Douyu room id", type=str)
    sp_pause.add_argument("roomid", help="Douyu room id", type=str)
    sp_pause.add_argument(
        "-r", "--resume", help="Resume from pause", action="store_true", default=False
    )

    sp_migrate.set_defaults(func=_migrate)
    sp_list.set_defaults(func=_list)

    sp_start.set_defaults(func=_start)
    sp_stop.set_defaults(func=_stop)
    sp_pause.set_defaults(func=_pause)


def _parse_args():
    parser = argparse.ArgumentParser(prog="barrage crawler")
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version="%(prog)s x.x.x",
    )

    _init_subparsers(parser)

    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    # call function
    args.func(args)