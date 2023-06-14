import requests
import streamlit as st

st.set_page_config(
    page_title="Pipeline Status",
)


@st.cache_data
def get_pipelines():
    response = requests.get(f"http://localhost:5000/pipelines/all")
    if response.status_code != 200:
        return None
    return response.json()["pipelines"]


@st.cache_data
def get_pipeline_status(pipeline_id):
    container_status_response = requests.get(f"http://localhost:5000/pipelines/status/{container['id']}")
    if container_status_response.status_code != 200:
        return None
    return container_status_response.json()["status"]


def clear_all_cache():
    get_pipelines.clear()
    get_pipeline_status.clear()


# Website starts
st.title("Active Pipelines")
with st.sidebar:
    st.button(label="Clear Cache & Refresh", on_click=clear_all_cache)

pipelines = get_pipelines()
for pipeline_type, containers in pipelines.items():
    st.subheader(f"Pipeline: {pipeline_type.split('-')[1].title()} ({len(containers)} containers)")
    for i, container in enumerate(containers):
        with st.expander(f"Container: {pipeline_type}-{i}"):
            st.write("#### Container Information:")
            st.write(f"**Container ID:** {container['id']}")
            st.write(f"**Internal Port:** {container['port']}")
            st.write(f"**Status:** {container['status']}")

            # Get container status
            container_json = get_pipeline_status(container['id'])
            if container_json is not None:
                st.write("#### Container Status:")
                if container_json:
                    st.write(f"**Iteration:** #{container_json['iteration']}")

                    processed = container_json['galaxies']
                    st.write(f"**Processed galaxies:** ({len(processed)})")
                    st.code(', '.join(processed))

                    successes = container_json['successes']
                    st.write(f"**Successful galaxies:** ({len(successes)})")
                    st.code(', '.join(successes))

                    fails = container_json['fails']
                    st.write(f"**Failed galaxies:** ({len(fails)})")
                    st.code(', '.join(fails))

                    st.write(f"Success rate: {len(successes) / len(processed) * 100:.2f}%")
                else:
                    st.write("No status available yet")
            else:
                st.write(f"Failed to get container status")
