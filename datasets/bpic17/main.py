from ocean_lib import pipeline, AggrSpecification, AggrStep

#https://link.springer.com/article/10.1007/s13740-021-00122-1/figures/25

@pipeline(first_load=False)
def build_aggr_spec(log, ekg):
    aggr_basic = [
        #AggrStep(aggr_type="ENTITIES", ent_type="Application", group_by=[ekg.type_tag], where='type = "New credit"', attr_aggrs=[]),
        #AggrStep(aggr_type="ENTITIES", ent_type="Application", group_by=[ekg.type_tag], where='type <> "New credit"', attr_aggrs=[]),
        AggrStep(aggr_type="ENTITIES", ent_type="Application", group_by=[ekg.type_tag], where=None, attr_aggrs=[]),
        AggrStep(aggr_type="ENTITIES", ent_type="Offer", group_by=[ekg.type_tag], where=None, attr_aggrs=[]),
        AggrStep(aggr_type="ENTITIES", ent_type="Workflow", group_by=[ekg.type_tag], where=None, attr_aggrs=[]),
        AggrStep(aggr_type="ENTITIES", ent_type="Resource", group_by=[ekg.type_tag], where=None, attr_aggrs=[]),
        AggrStep(aggr_type="EVENTS", ent_type= None, group_by=[log.event_activity,'lifecycle'], where=None, attr_aggrs=[])]
    
    return AggrSpecification(aggr_basic)

