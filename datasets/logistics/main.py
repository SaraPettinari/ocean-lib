from ocean_lib import pipeline, AggrSpecification, AggrStep, AttrAggr, AggregationFunction  

@pipeline(first_load=True)
def build_aggr_spec(log, ekg):
    '''
    This aggregation specification is designed to abstract container context by operational status full/empty.
    Then it segments customer orders into high-volume (>=1000) and low-volume (<1000) classes based on the Amount_of_Goods attribute.
    Finally, it aggregates events by activity and splits the analysis into two time windows (before and after March 31, 2024 - Easter period).
    '''
    steps = [
    AggrStep(aggr_type="ENTITIES", ent_type= "Container", group_by=["Status"], where=None, attr_aggrs=[AttrAggr(name='Weight', function=AggregationFunction.AVG), 
                                                                                                               AttrAggr(name='Amount_of_Handling_Units', function=AggregationFunction.MINMAX)]),
    AggrStep(aggr_type="ENTITIES", ent_type= "Customer_Order", group_by=[ekg.type_tag], where='Amount_of_Goods >= 1000', attr_aggrs=[AttrAggr(name='Amount_of_Goods', function=AggregationFunction.MINMAX)]),
    AggrStep(aggr_type="ENTITIES", ent_type= "Customer_Order", group_by=[ekg.type_tag], where='Amount_of_Goods < 1000', attr_aggrs=[AttrAggr(name='Amount_of_Goods', function=AggregationFunction.MINMAX)]),
    AggrStep(aggr_type="EVENTS", ent_type= None, group_by=[log.event_activity], where=f'{log.event_timestamp} <= datetime("2024-03-31T00:00:00")', attr_aggrs=[AttrAggr(name=f'{log.event_timestamp}', function=AggregationFunction.MINMAX)]) ,
    AggrStep(aggr_type="EVENTS", ent_type= None, group_by=[log.event_activity], where=f'{log.event_timestamp} > datetime("2024-03-31T00:00:00")', attr_aggrs=[AttrAggr(name=f'{log.event_timestamp}', function=AggregationFunction.MINMAX)]) ,
    ]
    
    return AggrSpecification(steps)
