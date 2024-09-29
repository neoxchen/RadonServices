import pickle

from psycopg2 import extensions, sql
from tqdm import tqdm

from commons.utils.sql_utils import PostgresClient, LocalPostgresClientFactory

if __name__ == "__main__":
    with open("galaxies.pkl", "rb") as f:
        data = pickle.load(f)

    print(f"Data preview: {data[:5]}")

    postgres_v3: PostgresClient = LocalPostgresClientFactory().create()
    batch_size = 5000
    for i in tqdm(range(0, len(data), batch_size)):
        batch = data[i:i + batch_size]
        batch = [tuple(entry[1:6]) for entry in batch]

        with postgres_v3.cursor() as cursor:
            cursor: extensions.cursor

            query = sql.SQL(f"""
                INSERT INTO galaxies (source_id, ra, dec, gal_prob, bin_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """)
            cursor.executemany(query, batch)
