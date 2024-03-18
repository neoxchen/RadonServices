import streamlit as st
from matplotlib import pyplot as plt

from commons.constants.fits_constants import FITS_BANDS
from commons.models.fits_interfaces import AbstractFitsInterface, GalaxyFitsData, BandFitsBuilder, LinuxFitsInterface
from commons.utils.sql_utils import AbstractPostgresClientFactory, PostgresClient, ClothoDockerPostgresClientFactory

# postgres_factory: AbstractPostgresClientFactory = LocalPostgresClientFactory()
postgres_factory: AbstractPostgresClientFactory = ClothoDockerPostgresClientFactory()
postgres_client: PostgresClient = postgres_factory.create()

# fits_interface: AbstractFitsInterface = LocalTestingFitsInterface()
fits_interface: AbstractFitsInterface = LinuxFitsInterface()

# State variables
if "preview_galaxies_result" not in st.session_state:
    st.session_state.preview_galaxies_result = []


@st.cache_data
def fetch_galaxy_data(query):
    with postgres_client.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        return results


# Clear cache
def clear_all_cache():
    fetch_galaxy_data.clear()


# Website starts
st.title("Data Preview")
with st.sidebar:
    st.button(label="Clear Cache & Refresh", on_click=clear_all_cache)

col1, col2 = st.columns(2)
# UID or source ID galaxy filter
with col1:
    id_type = st.selectbox("ID type", ["Source ID", "UID"])
    if id_type == "Source ID":
        galaxy_id = st.text_input("Enter source ID", placeholder="e.g. 5937148114675443200")
    else:
        galaxy_id = st.number_input("Enter galaxy UID", placeholder="e.g. 123", value=None)

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
    sql_id_type = "source_id" if id_type == "Source ID" else "id"
    sql_id_value = f"'{galaxy_id}'" if id_type == "Source ID" else int(galaxy_id)
    id_where = f"g.{sql_id_type} = {sql_id_value}"
    where_clauses.append(id_where)

if use_ra:
    where_clauses.append(f"ra BETWEEN {ra_min} AND {ra_max}")
if use_dec:
    where_clauses.append(f"dec BETWEEN {dec_min} AND {dec_max}")

sql_builder = [
    "SELECT g.*",
    "FROM galaxies g",
]

if where_clauses:
    sep = f"\n  AND "
    sql_builder.append(f"WHERE {sep.join(where_clauses)}")

# Add suffixes
sql_builder += [
    "GROUP BY g.id",
    f"LIMIT {limit}"
]

st.code("\n".join(sql_builder))

fetch = st.button("Fetch Data")
if fetch:
    st.session_state.preview_galaxies_result = fetch_galaxy_data("\n".join(sql_builder))
    clear_all_cache()

if not st.session_state.preview_galaxies_result:
    st.write("No data to preview")
else:
    st.subheader("Query Result")
    galaxy = st.session_state.preview_galaxies_result[0]
    uid, source_id, ra, dec, gal_prob, bin_id, status, fails = galaxy
    st.write(f"##### Galaxy #{uid}: b{bin_id}/{source_id}")
    st.write(f"Coordinates: ({ra}, {dec})")
    st.write(f"Galaxy Probability: {gal_prob}")
    st.write(f"Status: {status} (Fails: {fails})")

    if status == "Fetched" or status == "Transformed":
        fits_data: GalaxyFitsData = fits_interface.load_fits(source_id, bin_id)

        columns = st.columns(4)
        for i, band in enumerate(FITS_BANDS):
            with columns[i]:
                band_fits_data: BandFitsBuilder = fits_data.get_band_data(band)
                if not band_fits_data:
                    st.write(f"No FITS data for band {band}")
                    continue

                st.write(f"Band {band}:")
                fig, ax = plt.subplots()
                ax.imshow(band_fits_data.build(), cmap="gray")
                plt.axis("off")
                st.pyplot(fig)
