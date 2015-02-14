from ordereddict import OrderedDict

# These essentially come from the java classes defined here. It is cheesey and should probably come from a configuration file.
# https://github.com/linkedin/helix/blob/master/helix-core/src/main/java/com/linkedin/helix/tools/StateModelConfigGenerator.java

LEADER_STANDBY_STATE_DEF = OrderedDict()
LEADER_STANDBY_STATE_DEF["id"] = "LeaderStandby"
MAP_FIELDS = OrderedDict()
LEADER_STANDBY_STATE_DEF["mapFields"] = MAP_FIELDS
MAP_FIELDS["DROPPED.meta"] = { "count" : "-1" }
MAP_FIELDS["LEADER.meta"] = { "count" : "1" }
LEADER_NEXT = OrderedDict()
MAP_FIELDS["LEADER.next"] = LEADER_NEXT
LEADER_NEXT["DROPPED"] = "STANDBY"
LEADER_NEXT["STANDBY"] = "STANDBY"
LEADER_NEXT["OFFLINE"] = "STANDBY"
MAP_FIELDS["OFFLINE.meta"] = { "count" : "-1" }
OFFLINE_NEXT = OrderedDict()
MAP_FIELDS["OFFLINE.next"] = OFFLINE_NEXT
OFFLINE_NEXT["LEADER"] = "STANDBY"
OFFLINE_NEXT["DROPPED"] = "DROPPED"
OFFLINE_NEXT["STANDBY"] = "STANDBY"
MAP_FIELDS["STANDBY.meta"] = { "count" : "R" }
STANDBY_NEXT = OrderedDict()
MAP_FIELDS["STANDBY.next"] = STANDBY_NEXT
STANDBY_NEXT["LEADER"] = "LEADER"
STANDBY_NEXT["DROPPED"] = "OFFLINE"
STANDBY_NEXT["OFFLINE"] = "OFFLINE"
LIST_FIELDS = OrderedDict()
LEADER_STANDBY_STATE_DEF["listFields"] = LIST_FIELDS
LIST_FIELDS["STATE_PRIORITY_LIST"] = [ "LEADER", "STANDBY", "OFFLINE", "DROPPED" ]
LIST_FIELDS["STATE_TRANSITION_PRIORITYLIST"] = [ "LEADER-STANDBY", "STANDBY-LEADER", "OFFLINE-STANDBY", "STANDBY-OFFLINE", "OFFLINE-DROPPED" ]
SIMPLE_FIELDS = OrderedDict()
LEADER_STANDBY_STATE_DEF["simpleFields"] = SIMPLE_FIELDS
SIMPLE_FIELDS["INITIAL_STATE"] = "OFFLINE"

STATE_DEF_MAP = {
    "LeaderStandby": LEADER_STANDBY_STATE_DEF
}
