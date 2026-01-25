from ocean_lib import pipeline, AggrSpecification, AggrStep, AttrAggr, AggregationFunction  

@pipeline(first_load=False)
def build_aggr_spec(log, ekg):

    # baseline example:
    '''
    This aggregation aims to reproduce a baseline analysis by reconstructing an OC-DFG.
    '''
    baseline = [AggrStep(aggr_type="ENTITIES", ent_type= "playerId", group_by=[ekg.type_tag], where=None, attr_aggrs=[]),
                AggrStep(aggr_type="ENTITIES", ent_type= "teamId", group_by=[ekg.type_tag], where=None, attr_aggrs=[]),
                AggrStep(aggr_type="EVENTS", ent_type= None, where=None, group_by=[log.event_activity], 
                                            attr_aggrs=None)]
        
    # running example (matchId = 2575959):
    '''
    This aggregation aims to analyze player and team activities during a specific soccer match, to analyze teams strategies.
    '''
    aggr_running = [AggrStep(aggr_type="ENTITIES", ent_type= "playerId", group_by=["role"], where=None, attr_aggrs=[]),
                    AggrStep(aggr_type="ENTITIES", ent_type= "teamId", group_by=["wyId"], where=None, attr_aggrs=[]),
                    AggrStep(aggr_type="EVENTS", ent_type= None, where=None, group_by=[log.event_activity], 
                                    attr_aggrs=[AttrAggr(name=log.event_timestamp, function=AggregationFunction.MINMAX),
                                                AttrAggr(name="matchId", function=AggregationFunction.MULTISET)])]
    
    # full dataset example:
    # we want to aggregate by teamId and position 
    # so that we can see all events for each team and position 
    # and anlyze which areas are more active in terms of events
    '''
    This aggregation aims to provide a comprehensive overview of team events positions across all matches,
    facilitating the analysis of event distributions for each team (i.e., anlyze which areas are more active in terms of events).
    '''
    aggr_full = [
        AggrStep(aggr_type="ENTITIES", ent_type="teamId", group_by=["wyId"], where=None, attr_aggrs=[]),
        AggrStep(aggr_type="ENTITIES", ent_type="pos_orig", group_by=["wyId"], where=None, attr_aggrs=[]),
        AggrStep(aggr_type="EVENTS", ent_type=None, group_by=[log.event_activity], where=None, 
                 attr_aggrs=[AttrAggr(name=log.event_timestamp, function=AggregationFunction.MINMAX),
                             AttrAggr(name="playerId", function=AggregationFunction.MULTISET)])
    ]
                
    return AggrSpecification(aggr_running)

