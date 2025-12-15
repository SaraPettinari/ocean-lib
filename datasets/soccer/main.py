from ocean_lib import pipeline, AggrSpecification, AggrStep, AttrAggr, AggregationFunction  

@pipeline(first_load=False)
def build_aggr_spec(log, ekg):
    
    # running example:
    aggr_running = [AggrStep(aggr_type="ENTITIES", ent_type= "playerId", group_by=["role"], where=None, attr_aggrs=[]),
                    AggrStep(aggr_type="ENTITIES", ent_type= "teamId", group_by=["wyId"], where=None, attr_aggrs=[]),
                    AggrStep(aggr_type="EVENTS", ent_type= None, where=None, group_by=[log.event_activity], 
                                    attr_aggrs=[AttrAggr(name=log.event_timestamp, function=AggregationFunction.MINMAX),
                                                AttrAggr(name="matchId", function=AggregationFunction.MULTISET)])]
    
    return AggrSpecification(aggr_running)

