from ocean_lib import pipeline, AggrSpecification, AggrStep, AttrAggr, AggregationFunction  

@pipeline(first_load=False)
def build_aggr_spec(log, ekg):
    
    step1 = AggrStep(aggr_type="ENTITIES", ent_type= "customers", group_by=["id"], where=None, attr_aggrs=[AttrAggr(name='id', function=AggregationFunction.MULTISET)])
    step2 = AggrStep(aggr_type="ENTITIES", ent_type= "orders", group_by=[ekg.type_tag], where='price >= 1000', attr_aggrs=[AttrAggr(name='price', function=AggregationFunction.AVG)])
    step3 = AggrStep(aggr_type="ENTITIES", ent_type= "employees", group_by=['role'], where=None, attr_aggrs=None)
    step4 = AggrStep(aggr_type="EVENTS", ent_type= None, where=f'{log.event_timestamp} >= datetime("2024-03-31T00:00:00")', group_by=[log.event_activity, "customers", "orders"], attr_aggrs=[]) # I want to check all the orders after Easter
    step5 = AggrStep(aggr_type="EVENTS", ent_type= None, where=f'{log.event_timestamp} < datetime("2024-03-31T00:00:00")', group_by=[log.event_activity, "customers", "orders"], attr_aggrs=[]) # I want to check all the orders before Easter

    return AggrSpecification(steps=[step1, step2, step3,step4,step5])


