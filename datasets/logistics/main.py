from ocean_lib import pipeline, AggrSpecification, AggrStep, AttrAggr, AggregationFunction  

@pipeline(first_load=False)
def build_aggr_spec(log, ekg):
    
    step1 = AggrStep(aggr_type="ENTITIES", ent_type= "Container", group_by=["Status"], where=None, attr_aggrs=[AttrAggr(name='id', function=AggregationFunction.MULTISET), AttrAggr(name='Weight', function=AggregationFunction.AVG), 
                                                                                                               AttrAggr(name='Amount_of_Handling_Units', function=AggregationFunction.MINMAX)])
    step2 = AggrStep(aggr_type="EVENTS", ent_type= None, where=None, group_by=[log.event_activity, "Customer_Order"], attr_aggrs=[AttrAggr(name=f'{log.event_timestamp}', function=AggregationFunction.MINMAX)]) 
    step3 = AggrStep(aggr_type="ENTITIES", ent_type= "Vehicle", where='Departure_Date > datetime("2023-12-31T23:59:59")', group_by=[ekg.type_tag], attr_aggrs=[]) # I want to check all the orders after Easter

    return AggrSpecification(steps=[step1, step2,step3])
