import streamlit as st
from ocean_lib import pipeline, AggrSpecification, AggrStep
from neo4j import GraphDatabase

# ---- Neo4j connection ----
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12341234"))

def get_entity_types():
    query = "MATCH (n:Entity) RETURN DISTINCT labels(n) AS entity_labels"
    with driver.session() as session:
        result = session.run(query)
        return sorted(set(label for record in result for label in record["entity_labels"]))

# ---- UI ----
st.title("Aggregation Pipeline Builder")

num_steps = st.number_input("Number of steps", min_value=1, value=1)

steps = []
entity_types = get_entity_types()

for i in range(num_steps):
    st.subheader(f"Step {i+1}")
    aggr_type = st.selectbox(f"Aggregation Type {i+1}", ["ENTITIES", "EVENTS"])
    ent_type = None
    if aggr_type == "ENTITIES":
        ent_type = st.selectbox(f"Entity Type {i+1}", entity_types)
    group_by = st.text_input(f"Group By (comma-separated) {i+1}")
    where = st.text_input(f"Where condition {i+1}", "")
    
    steps.append(
        AggrStep(
            aggr_type=aggr_type,
            ent_type=ent_type,
            group_by=[x.strip() for x in group_by.split(",") if x.strip()],
            where=where or None,
            attr_aggrs=[]
        )
    )

if st.button("Build Aggregation Spec"):
    aggr_spec = AggrSpecification(steps)
    st.success("Aggregation specification created!")
    st.json([s.__dict__ for s in steps])
