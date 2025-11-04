from ocean_lib import pipeline, AggrSpecification, AggrStep, AttrAggr, AggregationFunction  

@pipeline(first_load=False)
def build_aggr_spec(log, ekg):
    # example 
    aggr_example = [AggrStep(aggr_type="ENTITIES", ent_type= "teamId", group_by=["country"], where=None, attr_aggrs=[AttrAggr(name='city', function=AggregationFunction.MULTISET)]),
                    AggrStep(aggr_type="ENTITIES", ent_type= "playerId", group_by=["role"], where='birthYear > 1990', attr_aggrs=[]),
                    AggrStep(aggr_type="EVENTS", ent_type= None, where=None, group_by=[log.event_activity, "teamId","playerId"], 
                                    attr_aggrs=[AttrAggr(name=log.event_timestamp, function=AggregationFunction.MINMAX),
                                                AttrAggr(name="matchId", function=AggregationFunction.MULTISET)])]
    # running example:
    aggr_running = [AggrStep(aggr_type="ENTITIES", ent_type= "playerId", group_by=["role"], where=None, attr_aggrs=[]),
                    AggrStep(aggr_type="EVENTS", ent_type= None, where=None, group_by=[log.event_activity, "teamId","playerId"], 
                                    attr_aggrs=[AttrAggr(name=log.event_timestamp, function=AggregationFunction.MINMAX),
                                                AttrAggr(name="matchId", function=AggregationFunction.MULTISET)])]
    
    # 4-step:
    aggr_example_2 =[
    AggrStep(aggr_type="ENTITIES", ent_type= "playerId", group_by=["role"], where='birthYear > 1990', attr_aggrs=[]),
    AggrStep(aggr_type="ENTITIES", ent_type= "pos_orig", group_by=["zone"], where=None, attr_aggrs=[]),
    AggrStep(aggr_type="EVENTS", ent_type= None, where='matchId = "2575959"', group_by=[log.event_activity, "teamId","playerId"], 
                     attr_aggrs=[AttrAggr(name=log.event_timestamp, function=AggregationFunction.MINMAX),
                                 AttrAggr(name="teamId", function=AggregationFunction.MULTISET)]),
    AggrStep(aggr_type="EVENTS", ent_type= None, where='matchId <> "2575959"', group_by=[log.event_activity, "teamId","playerId"], 
                     attr_aggrs=[AttrAggr(name=log.event_timestamp, function=AggregationFunction.MINMAX),
                                 AttrAggr(name="teamId", function=AggregationFunction.MULTISET)]) ]
    
    aggr_basic = [AggrStep(aggr_type="ENTITIES", ent_type= "playerId", group_by=["role"], where=None, attr_aggrs=[]),
                    AggrStep(aggr_type="EVENTS", ent_type= None, where=None, group_by=[log.event_activity, "playerId"], 
                                    attr_aggrs=[])]
    
    aggr_basic0 = [
        AggrStep(aggr_type="ENTITIES", ent_type= "playerId", group_by=["role"], where=None, attr_aggrs=[]),
        AggrStep(aggr_type="EVENTS", ent_type= None, group_by=[log.event_activity, "teamId","playerId"], where='matchId = "2575959"', attr_aggrs=[AttrAggr(name=log.event_timestamp, function=AggregationFunction.MINMAX)])]
    
    return AggrSpecification(aggr_basic0)

