from ocean_lib import pipeline, AggrSpecification, AggrStep, AttrAggr, AggregationFunction  


@pipeline(first_load=True)
def build_aggr_spec(log, ekg):
    '''
    This aggregation defines separate classes for high-value and low-value orders (i.e., >= 1000 and < 1000)
    and aggregates events accordingly to support customer-centric process analysis and order-value centric insights.
    '''
    steps = [
    AggrStep(aggr_type="ENTITIES", ent_type= "customers", group_by=["id"], where=None, attr_aggrs=[]),
    AggrStep(aggr_type="ENTITIES", ent_type= "orders", group_by=[ekg.type_tag], where='price >= 1000', attr_aggrs=[AttrAggr(name='price', function=AggregationFunction.AVG)]),
    AggrStep(aggr_type="ENTITIES", ent_type= "orders", group_by=[ekg.type_tag], where='price < 1000', attr_aggrs=[AttrAggr(name='price', function=AggregationFunction.AVG)]),
    AggrStep(aggr_type="EVENTS", ent_type= None, group_by=[log.event_activity], where=None, attr_aggrs=[]),
    # AggrStep(aggr_type="ENTITIES", ent_type= "employees", group_by=[ekg.type_tag], where=None, attr_aggrs=[]),
    # AggrStep(aggr_type="ENTITIES", ent_type= "items", group_by=[ekg.type_tag], where=None, attr_aggrs=[]),
    ]
    return AggrSpecification(steps)


