%%%----------------------------------------------------------------------
%%% File        : kafka_constants.hrl
%%% Author      : Aleksey Morarash <aleksey.morarash@gmail.com>
%%% Description : constant value definitions for Apache Kafka protocol.
%%% Created     : 02 Apr 2016
%%%----------------------------------------------------------------------

-ifndef(_KAFKA_CONSTANTS).
-define(_KAFKA_CONSTANTS, true).

%% API Keys
-define(Produce, 0).
-define(Fetch, 1).
-define(Offsets, 2).
-define(Metadata, 3).
-define(LeaderAndIsr, 4).
-define(StopReplica, 5).
-define(UpdateMetadata, 6).
-define(ControlledShutdown, 7).
-define(OffsetCommit, 8).
-define(OffsetFetch, 9).
-define(GroupCoordinator, 10).
-define(JoinGroup, 11).
-define(Heartbeat, 12).
-define(LeaveGroup, 13).
-define(SyncGroup, 14).
-define(DescribeGroups, 15).
-define(ListGroups, 16).

%% Compression levels
-define(None, 0).
-define(GZIP, 1).
-define(Snappy, 2).

%% Values for RequiredAcks field
-define(AfterClusterCommit, -1).
-define(NoAcknowledgement, 0). %% for fully asynchronous produce request
-define(AfterLocalWrite, 1).

%% ----------------------------------------------------------------------
%% Error codes

%% Retriable: no. The server experienced an unexpected error when
%% processing the request
-define(UNKNOWN, -1).
%% Retriable: no. OFFSET_OUT_OF_RANGE1FalseThe requested offset is
%% not within the range of offsets maintained by the server.
-define(NONE, 0).
%% Retriable: no. The requested offset is outside the range of offsets
%% maintained by the server for the given topic/partition.
-define(OFFSET_OUT_OF_RANGE, 1).
%% Retriable: yes. The message contents does not match the message CRC
%% or the message is otherwise corrupt.
-define(CORRUPT_MESSAGE, 2).
%% Retriable: yes. This server does not host this topic-partition.
-define(UNKNOWN_TOPIC_OR_PARTITION, 3).
%% Retriable: yes. There is no leader for this topic-partition as we
%% are in the middle of a leadership election.
-define(LEADER_NOT_AVAILABLE, 5).
%% Retriable: yes. This server is not the leader for that topic-partition.
-define(NOT_LEADER_FOR_PARTITION, 6).
%% Retriable: yes. The request timed out.
-define(REQUEST_TIMED_OUT, 7).
%% Retriable: no. The broker is not available.
-define(BROKER_NOT_AVAILABLE, 8).
%% Retriable: no. The replica is not available for the requested
%% topic-partition
-define(REPLICA_NOT_AVAILABLE, 9).
%% Retriable: no. The request included a message larger than the max
%% message size the server will accept.
-define(MESSAGE_TOO_LARGE, 10).
%% Retriable: no. The controller moved to another broker.
-define(STALE_CONTROLLER_EPOCH, 11).
%% Retriable: no. The metadata field of the offset request was too large.
-define(OFFSET_METADATA_TOO_LARGE, 12).
%% Retriable: yes. The server disconnected before a response was received.
-define(NETWORK_EXCEPTION, 13).
%% Retriable: yes. The coordinator is loading and hence can't process
%% requests for this group.
-define(GROUP_LOAD_IN_PROGRESS, 14).
%% Retriable: yes. The group coordinator is not available.
-define(GROUP_COORDINATOR_NOT_AVAILABLE, 15).
%% Retriable: yes. This is not the correct coordinator for this group.
-define(NOT_COORDINATOR_FOR_GROUP, 16).
%% Retriable: no. The request attempted to perform an operation on an
%% invalid topic.
-define(INVALID_TOPIC_EXCEPTION, 17).
%% Retriable: no. The request included message batch larger than the
%% configured segment size on the server.
-define(RECORD_LIST_TOO_LARGE, 18).
%% Retriable: yes. Messages are rejected since there are fewer in-sync
%% replicas than required.
-define(NOT_ENOUGH_REPLICAS, 19).
%% Retriable: yes. Messages are written to the log, but to fewer in-sync
%% replicas than required.
-define(NOT_ENOUGH_REPLICAS_AFTER_APPEND, 20).
%% Retriable: no. Produce request specified an invalid value for required acks.
-define(INVALID_REQUIRED_ACKS, 21).
%% Retriable: no. Specified group generation id is not valid.
-define(ILLEGAL_GENERATION, 22).
%% Retriable: no. The group member's supported protocols are incompatible
%% with those of existing members.
-define(INCONSISTENT_GROUP_PROTOCOL, 23).
%% Retriable: no. The configured groupId is invalid
-define(INVALID_GROUP_ID, 24).
%% Retriable: no. The coordinator is not aware of this member.
-define(UNKNOWN_MEMBER_ID, 25).
%% Retriable: no. The session timeout is not within an acceptable range.
-define(INVALID_SESSION_TIMEOUT, 26).
%% Retriable: no. The group is rebalancing, so a rejoin is needed.
-define(REBALANCE_IN_PROGRESS, 27).
%% Retriable: no. The committing offset data size is not valid
-define(INVALID_COMMIT_OFFSET_SIZE, 28).
%% Retriable: no. Topic authorization failed.
-define(TOPIC_AUTHORIZATION_FAILED, 29).
%% Retriable: no. Group authorization failed.
-define(GROUP_AUTHORIZATION_FAILED, 30).
%% Retriable: no. Cluster authorization failed.
-define(CLUSTER_AUTHORIZATION_FAILED, 31).

-define(
   err2atom(ErrorCode),
   case ErrorCode of
       ?UNKNOWN -> unknown;
       ?NONE -> none;
       ?OFFSET_OUT_OF_RANGE -> offset_out_of_range;
       ?CORRUPT_MESSAGE -> corrupt_message;
       ?UNKNOWN_TOPIC_OR_PARTITION -> unknown_topic_or_partition;
       ?LEADER_NOT_AVAILABLE -> leader_not_available;
       ?NOT_LEADER_FOR_PARTITION -> not_leader_for_partition;
       ?REQUEST_TIMED_OUT -> request_timed_out;
       ?BROKER_NOT_AVAILABLE -> broker_not_available;
       ?REPLICA_NOT_AVAILABLE -> replica_not_available;
       ?MESSAGE_TOO_LARGE -> message_too_large;
       ?STALE_CONTROLLER_EPOCH -> stale_controller_epoch;
       ?OFFSET_METADATA_TOO_LARGE -> offset_metadata_too_large;
       ?NETWORK_EXCEPTION -> network_exception;
       ?GROUP_LOAD_IN_PROGRESS -> group_load_in_progress;
       ?GROUP_COORDINATOR_NOT_AVAILABLE -> group_coordinator_not_available;
       ?NOT_COORDINATOR_FOR_GROUP -> not_coordinator_for_group;
       ?INVALID_TOPIC_EXCEPTION -> invalid_topic_exception;
       ?RECORD_LIST_TOO_LARGE -> record_list_too_large;
       ?NOT_ENOUGH_REPLICAS -> not_enough_replicas;
       ?NOT_ENOUGH_REPLICAS_AFTER_APPEND -> not_enough_replicas_after_append;
       ?INVALID_REQUIRED_ACKS -> invalid_required_acks;
       ?ILLEGAL_GENERATION -> illegal_generation;
       ?INCONSISTENT_GROUP_PROTOCOL -> inconsistent_group_protocol;
       ?INVALID_GROUP_ID -> invalid_group_id;
       ?UNKNOWN_MEMBER_ID -> unknown_member_id;
       ?INVALID_SESSION_TIMEOUT -> invalid_session_timeout;
       ?REBALANCE_IN_PROGRESS -> rebalance_in_progress;
       ?INVALID_COMMIT_OFFSET_SIZE -> invalid_commit_offset_size;
       ?TOPIC_AUTHORIZATION_FAILED -> topic_authorization_failed;
       ?GROUP_AUTHORIZATION_FAILED -> group_authorization_failed;
       ?CLUSTER_AUTHORIZATION_FAILED -> cluster_authorization_failed;
       _ -> ErrorCode
   end).

-endif.
