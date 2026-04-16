"""Direct psycopg2 adapter satisfying the DatabaseEnv protocol.

psycopg2's native cursor already has execute/fetchall/fetchone,
so no wrapper is needed — just connect and expose .cr and .conn.
"""

import psycopg2


class LambdaEnv:
    """DatabaseEnv backed by a direct psycopg2 connection.

    Usage::

        env = LambdaEnv(os.environ["DATABASE_URL"])
        orchestrator = Orchestrator(env, transport, ...)
    """

    def __init__(self, dsn: str):
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = True
        self.cr = self.conn.cursor()

    def close(self):
        self.cr.close()
        self.conn.close()
