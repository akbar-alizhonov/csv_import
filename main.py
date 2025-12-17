import os
import csv

import psycopg2
from psycopg2.extensions import connection, cursor

from dotenv import load_dotenv

load_dotenv()


def connect_to_db() -> connection:
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        database=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
    )


def execute_query(cur: cursor) -> tuple:
    query: str = """
    WITH ordered_sessions AS (
        SELECT
            s.visitor_session_id,
            s.site_id,
            s.visitor_id,
            s.date_time AS session_date_time,
            s.campaign_id,
            ROW_NUMBER() OVER (
                PARTITION BY s.site_id, s.visitor_id
                ORDER BY s.date_time
            ) AS row_n
        FROM web_data.sessions s
    )
    SELECT
        c.communication_id,
        c.site_id,
        c.visitor_id,
        c.date_time AS communication_date_time,
        os.visitor_session_id,
        os.session_date_time,
        os.campaign_id,
        os.row_n
    FROM web_data.communications c
    LEFT JOIN LATERAL (
        SELECT *
        FROM ordered_sessions os
        WHERE
            os.site_id = c.site_id
            AND os.visitor_id = c.visitor_id
            AND os.session_date_time <= c.date_time
        ORDER BY os.session_date_time DESC
        LIMIT 1
    ) os ON TRUE
    ORDER BY c.communication_id;
    """

    cur.execute(query)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    return rows, columns


def main():
    conn: connection = connect_to_db()
    cur: cursor = conn.cursor()
    rows, columns = execute_query(cur)

    with open("result.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
