import requests
import streamlit as st

from config import BACKEND_BASE_URL

st.set_page_config(
    page_title="Pipeline Status",
)


@st.cache_data
def get_pipelines():
    response = requests.get(f"{BACKEND_BASE_URL}/pipelines/all")
    if response.status_code != 200:
        return None
    return response.json()["pipelines"]


@st.cache_data
def get_pipeline_status(container_id):
    container_status_response = requests.get(f"{BACKEND_BASE_URL}/pipelines/status/{container_id}")
    if container_status_response.status_code != 200:
        return None
    return container_status_response.json()["status"]


def clear_all_cache():
    get_pipelines.clear()
    get_pipeline_status.clear()


# State variables
if "create_pipeline_response" not in st.session_state:
    st.session_state.create_pipeline_response = None

# Website starts
st.title("Pipeline Management")
with st.sidebar:
    st.button(label="Clear Cache & Refresh", on_click=clear_all_cache)

# Pipeline creation
st.subheader("Create New Pipeline")
pipeline_type = st.selectbox("Pipeline Type", ["fetch", "radon"])
create_pipeline_button = st.button(label="Create Pipeline")
if create_pipeline_button:
    response = requests.post(f"{BACKEND_BASE_URL}/pipelines/{pipeline_type}")
    st.session_state.create_pipeline_response = response.json()
    clear_all_cache()

if st.session_state.create_pipeline_response is not None:
    st.write(
        f"Successfully created new '{st.session_state.create_pipeline_response['new_pipeline']}' pipeline at ports {st.session_state.create_pipeline_response['ports']}")

# Pipeline status
st.header("Pipeline Status")
pipelines = get_pipelines()
if not pipelines:
    st.write("No active pipelines")
    st.stop()

for pipeline_type, containers in pipelines.items():
    st.subheader(f"Pipeline: {pipeline_type.split('-')[1].title()} ({len(containers)} containers)")
    for i, container in enumerate(containers):
        with st.expander(f"Container: {pipeline_type}-{i}"):
            st.write("#### Container Information:")
            st.write(f"**Container ID:** {container['id']}")
            st.write(f"**Container Name:** {container['name']}")
            st.write(f"**Internal Hostname:** {container['hostname']}")
            st.write(f"**Internal Port:** {container['port']}")
            st.write(f"**Status:** {container['status']}")

            # Get container status
            container_json = get_pipeline_status(container['id'])
            if container_json is None:
                st.write(f"Failed to get container status")
            else:
                st.write("#### Container Status:")
                if not container_json:
                    st.write("No status available yet")
                else:
                    st.write(f"**Iteration:** #{container_json['iteration']}")

                    processed = container_json['galaxies']
                    st.write(f"**Processed galaxies:** ({len(processed)})")
                    st.code(', '.join(str(a) for a in processed))

                    successes = container_json['successes']
                    st.write(f"**Successful galaxies:** ({len(successes)})")
                    st.code(', '.join(str(a) for a in successes))

                    fails = container_json['fails']
                    st.write(f"**Failed galaxies:** ({len(fails)})")
                    st.code(', '.join(str(a) for a in fails))

                    st.write(f"Success rate: {len(successes) / len(processed) * 100:.2f}%")

            # Shutdown button
            shutdown_button = st.button(label="Shutdown Container", key=container['id'])
            if shutdown_button:
                response = requests.delete(f"{BACKEND_BASE_URL}/pipelines/{pipeline_type.split('-')[1]}",
                                           json={"container_id": container['id']})
                if response.status_code != 200:
                    st.write(f"Failed to shutdown container: {response.json()['error']}")
                    with st.expander("Traceback"):
                        st.write(response.json()["details"])
                else:
                    st.write(f"Successfully sent shutdown command to container")
                clear_all_cache()
