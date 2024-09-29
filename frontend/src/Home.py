from typing import Any, List

import streamlit as st
from psycopg2 import sql

from interfaces import get_postgres_client


@st.cache_data
def fetch_status_data() -> List[Any]:
    with get_postgres_client().cursor() as cursor:
        cursor.execute(sql.SQL("SELECT status, COUNT(*) FROM galaxies GROUP BY status"))
        results = cursor.fetchall()
        if not results:
            return []
        return results


@st.cache_data
def fetch_galaxy_data(preview_galaxy_id):
    if not preview_galaxy_id:
        return None

    try:
        id_int = int(preview_galaxy_id)
    except ValueError:
        return None

    with get_postgres_client().cursor() as cursor:
        cursor.execute(sql.SQL(f"""
            SELECT * FROM galaxies
            WHERE id={id_int} OR source_id='{preview_galaxy_id}'
        """))
        results = cursor.fetchall()
        if not results:
            return None
        return results[0]


@st.cache_data
def fetch_galaxy_rotation_data(preview_galaxy_id):
    if not preview_galaxy_id:
        return None
    with get_postgres_client().cursor() as cursor:
        cursor.execute(sql.SQL(f"""
            SELECT * FROM fits_data
            WHERE source_id='{preview_galaxy_id}'
        """))
        results = cursor.fetchall()
        if not results:
            return None
        return results[0]


def clear_all_cache():
    fetch_status_data.clear()
    fetch_galaxy_data.clear()
    fetch_galaxy_rotation_data.clear()


# Website starts
st.title("Galaxy Radon Web Controller")
with st.sidebar:
    st.button(label="Clear Cache & Refresh", on_click=clear_all_cache)

st.subheader("Galaxy dataset status:")
status_data = fetch_status_data()
st.table({a[0]: a[1] for a in status_data})
