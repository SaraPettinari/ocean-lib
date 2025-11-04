import streamlit as st
import pandas as pd
import yaml
import os

st.title("Config Generator with Drag & Drop CSV Upload and Attribute Selection")

# Folder to save uploaded files
UPLOAD_DIR = "uploaded_csvs"
os.makedirs(UPLOAD_DIR, exist_ok=True)

uploaded_files = st.file_uploader(
    "Upload multiple CSV files (drag & drop supported)",
    accept_multiple_files=True,
    type=["csv"]
)

def infer_type(val):
    if pd.isna(val):
        return "String"
    if isinstance(val, int) or (isinstance(val, float) and val.is_integer()):
        return "Integer"
    try:
        pd.to_datetime(val)
        return "Datetime"
    except:
        pass
    return "String"

entities_config = {}  # Will hold all entities

# Step 1: Handle uploaded files
if uploaded_files and len(uploaded_files) > 0:
    st.success(f"{len(uploaded_files)} files uploaded")

    # Detect event file
    event_files = [f for f in uploaded_files if "event" in f.name.lower()]
    if event_files:
        event_file = max(event_files, key=lambda f: f.size)
    else:
        event_file = max(uploaded_files, key=lambda f: f.size)

    st.markdown(f"**Detected event CSV:** `{event_file.name}`")

    # Save event file
    event_path = os.path.join(UPLOAD_DIR, event_file.name)
    with open(event_path, "wb") as f:
        f.write(event_file.getbuffer())

    # Read event CSV
    event_df = pd.read_csv(event_file)
    st.markdown("**Preview of event CSV:**")
    st.dataframe(event_df.head(10))  # show first 10 rows
    event_attrs = list(event_df.columns)

    # Let user select/discard attributes and edit types
    event_attr_types = {}
    event_attr_selected = {}
    st.write("Select and adjust event attributes:")
    for attr in event_attrs:
        sample_vals = event_df[attr].dropna()
        sample_val = sample_vals.iloc[0] if len(sample_vals) > 0 else None
        inferred_type = infer_type(sample_val)

        col1, col2 = st.columns([1, 2])
        with col1:
            keep = st.checkbox(f"Keep {attr}", value=True, key=f"keep_event_{attr}")
        with col2:
            dtype = st.selectbox(
                f"Type for {attr}",
                ["String", "Integer", "Datetime"],
                index=["String", "Integer", "Datetime"].index(inferred_type),
                key=f"type_event_{attr}"
            )
        event_attr_selected[attr] = keep
        event_attr_types[attr] = dtype

    event_attrs_final = [a for a, keep in event_attr_selected.items() if keep]
    event_attr_types_final = {a: t for a, t in event_attr_types.items() if event_attr_selected[a]}

    # Step 2: Handle entity files from uploads
    uploaded_entity_files = [f for f in uploaded_files if f != event_file]
    for ef in uploaded_entity_files:
        ef_path = os.path.join(UPLOAD_DIR, ef.name)
        with open(ef_path, "wb") as f:
            f.write(ef.getbuffer())

        ent_df = pd.read_csv(ef)
        st.markdown(f"**Preview of entity `{ef.name}`:**")
        st.dataframe(ent_df.head(10))
        ent_attrs = list(ent_df.columns)
        ent_attr_types = {}
        ent_attr_selected = {}
        st.write(f"Select and adjust attributes for entity `{ef.name}`:")
        for attr in ent_attrs:
            sample_vals = ent_df[attr].dropna()
            sample_val = sample_vals.iloc[0] if len(sample_vals) > 0 else None
            inferred_type = infer_type(sample_val)

            col1, col2 = st.columns([1, 2])
            with col1:
                keep = st.checkbox(f"Keep {attr}", value=True, key=f"keep_{ef.name}_{attr}")
            with col2:
                dtype = st.selectbox(
                    f"Type for {attr}",
                    ["String", "Integer", "Datetime"],
                    index=["String", "Integer", "Datetime"].index(inferred_type),
                    key=f"type_{ef.name}_{attr}"
                )
            ent_attr_selected[attr] = keep
            ent_attr_types[attr] = dtype

        ent_attrs_final = [a for a, keep in ent_attr_selected.items() if keep]
        ent_attr_types_final = {a: t for a, t in ent_attr_types.items() if ent_attr_selected[a]}

        ent_type = ef.name.rsplit(".", 1)[0]
        entities_config[ent_type] = {
            "type": ent_type,
            "path": ef_path,
            "attr": ent_attrs_final,
            "attr_types": ent_attr_types_final
        }

# Step 3: Allow user to add additional entity files manually
st.markdown("### Add additional entity files manually")
num_manual = st.number_input("Number of additional entities to add", min_value=0, value=0)
for i in range(num_manual):
    st.subheader(f"Manual entity {i+1}")
    manual_file = st.file_uploader(f"Upload entity CSV {i+1}", type=["csv"], key=f"manual_{i}")
    if manual_file:
        manual_path = os.path.join(UPLOAD_DIR, manual_file.name)
        with open(manual_path, "wb") as f:
            f.write(manual_file.getbuffer())

        ent_df = pd.read_csv(manual_file)
        ent_attrs = list(ent_df.columns)
        ent_attr_types = {}
        ent_attr_selected = {}
        st.write(f"Select and adjust attributes for entity `{manual_file.name}`:")
        for attr in ent_attrs:
            sample_vals = ent_df[attr].dropna()
            sample_val = sample_vals.iloc[0] if len(sample_vals) > 0 else None
            inferred_type = infer_type(sample_val)

            col1, col2 = st.columns([1, 2])
            with col1:
                keep = st.checkbox(f"Keep {attr}", value=True, key=f"keep_manual_{i}_{attr}")
            with col2:
                dtype = st.selectbox(
                    f"Type for {attr}",
                    ["String", "Integer", "Datetime"],
                    index=["String", "Integer", "Datetime"].index(inferred_type),
                    key=f"type_manual_{i}_{attr}"
                )
            ent_attr_selected[attr] = keep
            ent_attr_types[attr] = dtype

        ent_attrs_final = [a for a, keep in ent_attr_selected.items() if keep]
        ent_attr_types_final = {a: t for a, t in ent_attr_types.items() if ent_attr_selected[a]}

        ent_type = manual_file.name.rsplit(".", 1)[0]
        entities_config[ent_type] = {
            "type": ent_type,
            "path": manual_path,
            "attr": ent_attrs_final,
            "attr_types": ent_attr_types_final
        }

# Step 4: Event/entity IDs
st.markdown("### Define ID attributes")
event_id = st.text_input("Event unique ID attribute", value="id")
event_activity = st.text_input("Event activity attribute", value="eventName")
event_timestamp = st.text_input("Event timestamp attribute", value="eventDatetime")
entity_id = st.text_input("Entity unique ID attribute", value="wyId")

# Step 5: Generate config if event file exists
if uploaded_files and len(uploaded_files) > 0:
    config = {
        "event_id": event_id,
        "event_activity": event_activity,
        "event_timestamp": event_timestamp,
        "entity_id": entity_id,
        "events": {
            "path": event_path,
            "attr": event_attrs_final,
            "attr_types": event_attr_types_final,
        },
        "entities": entities_config
    }

    yaml_str = yaml.dump(config, sort_keys=False)
    st.header("Generated Config YAML")
    st.code(yaml_str, language="yaml")

    st.download_button(
        label="Download config.yaml",
        data=yaml_str,
        file_name="config.yaml",
        mime="text/yaml"
    )
