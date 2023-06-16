import numpy as np
import pandas as pd
import requests
import streamlit as st
from astropy.io import fits
from matplotlib import pyplot as plt

from config import BACKEND_BASE_URL
from sql_util import postgres

# State variables
if "preview_galaxies" not in st.session_state:
    st.session_state.preview_galaxies = []

if "preview_galaxies_result" not in st.session_state:
    st.session_state.preview_galaxies_result = []


# Get recent galaxies
@st.cache_data
def get_recent_galaxies():
    response = requests.get(f"{BACKEND_BASE_URL}/pipelines/all")
    if response.status_code != 200:
        return None

    pipelines = response.json()["pipelines"]
    results = []
    for pipeline_type, containers in pipelines.items():
        for container in containers:
            container_status_response = requests.get(f"{BACKEND_BASE_URL}/pipelines/status/{container['id']}")
            if container_status_response.status_code != 200:
                continue
            container_json = container_status_response.json()["status"]
            if not container_json:
                continue
            results += [[str(galaxy_id), "successful"] for galaxy_id in container_json["successes"]]
            results += [[str(galaxy_id), "failed"] for galaxy_id in container_json["fails"]]

    return results


@st.cache_data
def fetch_galaxy_data(query):
    with postgres() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        return results


# Clear cache
def clear_all_cache():
    get_recent_galaxies.clear()
    fetch_galaxy_data.clear()


# Website starts
st.title("Data Preview")
with st.sidebar:
    st.button(label="Clear Cache & Refresh", on_click=clear_all_cache)


def recent_table():
    results = get_recent_galaxies()
    result_dict = {
        "Galaxy ID": [galaxy_id for galaxy_id, _ in results],
        "Status": [status for _, status in results]
    }
    df = pd.DataFrame(result_dict)
    df_with_selections = df.copy()
    df_with_selections.insert(0, "Select", False)
    edited_df = st.data_editor(
        df_with_selections,
        hide_index=True,
        column_config={"Select": st.column_config.CheckboxColumn(required=True)},
        disabled=df.columns,
        use_container_width=True
    )
    selected_indices = list(np.where(edited_df.Select)[0])
    return df.loc[selected_indices]["Galaxy ID"].tolist()


col1, col2 = st.columns(2)

# UID or source ID galaxy filter
with col1:
    st.subheader("Recently Processed")
    st.session_state.preview_galaxies = recent_table()
    galaxy_id = st.text_input("Enter additional galaxy UID or source ID:", placeholder="e.g. 5937148114675443200")
    if galaxy_id:
        st.session_state.preview_galaxies.append(galaxy_id)

# Additional galaxy filters such as ra, dec
with col2:
    st.subheader("Additional Filters")
    use_ra = st.checkbox("Filter by Right Ascension [0, 360]")
    if use_ra:
        ra_min = st.number_input("Min RA", min_value=0., max_value=360., value=0., step=1.)
        ra_max = st.number_input("Max RA", min_value=0., max_value=360., value=360., step=1.)
        if ra_min > ra_max:
            st.error("Min RA must be less than Max RA")

    use_dec = st.checkbox("Filter by Declination [-90, 90]")
    if use_dec:
        dec_min = st.number_input("Min Dec", min_value=-90., max_value=90., value=-90., step=1.)
        dec_max = st.number_input("Max Dec", min_value=-90., max_value=90., value=90., step=1.)
        if dec_min > dec_max:
            st.error("Min Dec must be less than Max Dec")

limit = st.number_input("Limit", min_value=1, max_value=1000, value=100, step=100)

st.subheader("Generated SQL query:")

where_clauses = []
if st.session_state.preview_galaxies:
    id_where = f"(id IN ({', '.join(st.session_state.preview_galaxies)})"


    def formatter(a):
        alist = a.split(",")
        return ", ".join(f"'{a.strip()}'" for a in alist)


    id_where += f" OR galaxies.source_id IN ({', '.join(formatter(a) for a in st.session_state.preview_galaxies)}))"
    where_clauses.append(id_where)
if use_ra:
    where_clauses.append(f"ra BETWEEN {ra_min} AND {ra_max}")
if use_dec:
    where_clauses.append(f"dec BETWEEN {dec_min} AND {dec_max}")

sql_builder = [
    "SELECT",
    "    id, galaxies.source_id, ra, dec, gal_prob, bin, status,",
    "    rotation_g, rotation_r, rotation_i, rotation_z",
    "FROM galaxies",
    "LEFT JOIN fits_data ON galaxies.source_id=fits_data.source_id"
]
if where_clauses:
    sep = f"\n  AND "
    sql_builder.append(f"WHERE {sep.join(where_clauses)}")
sql_builder.append(f"LIMIT {limit}")

st.code("\n".join(sql_builder))

fetch = st.button("Fetch Data")
if fetch:
    st.session_state.preview_galaxies_result = fetch_galaxy_data("\n".join(sql_builder))

if not st.session_state.preview_galaxies_result:
    st.write("No data to preview")
else:
    st.subheader("Query Result")
    preview_result_df = pd.DataFrame(st.session_state.preview_galaxies_result,
                                     columns=["id", "source_id", "ra", "dec", "gal_prob", "bin", "status",
                                              "rot_g", "rot_r", "rot_i", "rot_z"])
    preview_result_df["ra"] = preview_result_df["ra"].astype(float)
    preview_result_df["dec"] = preview_result_df["dec"].astype(float)
    preview_result_df["gal_prob"] = preview_result_df["gal_prob"].astype(float)


    def selectable_result_table():
        df_with_selections = preview_result_df.copy()
        df_with_selections.insert(0, "Select", False)
        edited_df = st.data_editor(
            df_with_selections,
            hide_index=True,
            column_config={"Select": st.column_config.CheckboxColumn(required=True)},
            use_container_width=True
        )
        selected_indices = list(np.where(edited_df.Select)[0])
        return preview_result_df.loc[selected_indices]


    # Display FITS (and rotation if applicable) for selected galaxies
    result_selected = selectable_result_table()
    for index, row in result_selected.iterrows():
        st.subheader(f"Galaxy {row['source_id']}")
        st.write(f"**Coordinates:** ({row['ra']}, {row['dec']})")
        actual_fits_path = f"/home/neo/data/fits/b{row['bin']}/{row['source_id']}.fits"
        st.write(f"**FITS path:** {actual_fits_path}")

        # If status is fetched, display FITS image without rotation
        if row["status"] != "Fetched" and row["status"] != "Transformed":
            st.write("*FITS data has not been fetched for this galaxy*")
        else:
            # Container path is different because of mounted volume
            container_fits_path = f"/fits-data/b{row['bin']}/{row['source_id']}.fits"
            with fits.open(container_fits_path) as hdu_list:
                fits_data = hdu_list[0].data

            # Annotate image with rotation if transformed
            rotations = None
            if row["status"] == "Transformed":
                rotations = [row[f"rot_{'griz'[i]}"] for i in range(4)]

            columns = st.columns(4)
            for i, band in enumerate(fits_data):
                with columns[i]:
                    if not fits_data[i].any():
                        st.write(f"No FITS data for band {'GRIZ'[i]}")
                        continue
                    st.write(f"Band {'GRIZ'[i]}:")

                    fig, ax = plt.subplots()
                    ax.imshow(band, cmap="gray")
                    plt.axis("off")

                    # Display rotation data if we've completed radon transform
                    if rotations is not None:
                        radians = np.deg2rad(rotations[i])
                        ax.plot([20, 20 * (1 - np.cos(radians))], [20, 20 * (1 - np.sin(radians))], color="red")

                    st.pyplot(fig)
                    if rotations is not None:
                        st.write(f"Rotation: {rotations[i]}")
