from ocean_lib import pipeline, AggrSpecification, AggrStep, AttrAggr, AggregationFunction  

@pipeline(first_load=True)
def build_aggr_spec(log, ekg):
    '''
    This aggregation groups materials by storage location and purchase requisitions by
    purchasing group, and aggregates events by activity to support procurement-oriented
    process analysis.
    '''
    steps= [
    AggrStep(aggr_type="ENTITIES", ent_type= "material", group_by=['Storage_Location_EKPO_LGORT'], where=None, 
             attr_aggrs=[AttrAggr(name='Quantity_EKPO_MENGE', function=AggregationFunction.AVG), AttrAggr(name='Net_Price_EKPO_NETPR', function=AggregationFunction.SUM)]),
    AggrStep(aggr_type="ENTITIES", ent_type= "purchase_requisition", group_by=['Purchasing_Group_EBAN_EKGRP'], where=None, attr_aggrs=[]),
    AggrStep(aggr_type="EVENTS", ent_type= None, group_by=[log.event_activity], where=None, attr_aggrs=[])
    ]
    return AggrSpecification(steps)
