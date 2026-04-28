from postgresql_access import DatabaseDict

import psycopg


def postgres_connect(config)->psycopg.Connection:
    db = DatabaseDict(dictionary=config['database'])
    db.set_app_name("update tracker")
    return db.connect()
