from ocean_lib import pipeline, AggrSpecification, AggrStep


@pipeline(first_load=False)
def build_aggr_spec(log, ekg):
    aggr_basic = [
        AggrStep(aggr_type="ENTITIES", ent_type="Invoice", group_by=[ekg.type_tag], where='value >= 5000', attr_aggrs=[]),
        AggrStep(aggr_type="ENTITIES", ent_type="Invoice", group_by=[ekg.type_tag], where='value < 5000', attr_aggrs=[]),
        AggrStep(aggr_type="EVENTS", ent_type= None, group_by=[log.event_activity], where=None, attr_aggrs=[])]
    
    return AggrSpecification(aggr_basic)

