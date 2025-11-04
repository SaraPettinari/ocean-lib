from ocean_lib import pipeline, AggrSpecification, AggrStep, AttrAggr, AggregationFunction  

@pipeline(first_load=False)
def build_aggr_spec(log, ekg):
    
    step1 = AggrStep(aggr_type="ENTITIES", ent_type= "material", group_by=[ekg.type_tag], where='Delivery_Date_EKPO_BEDAT < dateTime("2023-10-24T09:30:27.930832")', attr_aggrs=[AttrAggr(name='Quantity_EKPO_MENGE', function=AggregationFunction.AVG), AttrAggr(name='Net_Price_EKPO_NETPR', function=AggregationFunction.SUM)])
    step2 = AggrStep(aggr_type="ENTITIES", ent_type= "material", group_by=[ekg.type_tag], where='Delivery_Date_EKPO_BEDAT >= dateTime("2023-10-24T09:30:27.930832")', attr_aggrs=[AttrAggr(name='Quantity_EKPO_MENGE', function=AggregationFunction.AVG), AttrAggr(name='Net_Price_EKPO_NETPR', function=AggregationFunction.SUM)])
    step3 = AggrStep(aggr_type="ENTITIES", ent_type= "purchase_requisition", group_by=['Purchasing_Group_EBAN_EKGRP'], where=None, attr_aggrs=None)
    step4 = AggrStep(aggr_type="EVENTS", ent_type= None, where=None, group_by=[log.event_activity, "material", "purchase_requisition"], attr_aggrs=[])

    return AggrSpecification(steps=[step1, step2, step3,step4])
