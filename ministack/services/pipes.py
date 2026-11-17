# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
EventBridge Pipes service emulator.

REST/JSON protocol — /v1/pipes/* and /tags/* paths.

SDK surface:
  ListPipes, CreatePipe, DescribePipe, UpdatePipe, DeletePipe,
  StartPipe, StopPipe, ListTagsForResource, TagResource, UntagResource

Runtime (background poller + CloudFormation) scope is intentionally limited to:
- Source: DynamoDB Streams
- Target: SNS, Step Functions state machine, SQS queue, EventBridge event bus
"""

import copy
import json
import logging
import os
import threading
import time
from urllib.parse import unquote

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    _request_account_id,
    _request_region,
    get_account_id,
    get_region,
)

logger = logging.getLogger("pipes")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
CROSS_REGION_PIPE_ERROR = "Creating cross-region pipe is not permitted."

_pipes = AccountRegionScopedDict()       # pipe_name -> pipe record
_positions = AccountRegionScopedDict()   # pipe_arn -> next stream record index
_poller_started = False
_poller_lock = threading.Lock()


def get_state():
    return {
        "pipes": copy.deepcopy(_pipes),
        "positions": copy.deepcopy(_positions),
    }


def load_persisted_state(data) -> None:
    _restore_state(data)
    # Restored RUNNING pipes need the background poller — register_pipe is the
    # only other place that starts it, and it is not called on warm boot.
    if any(pipe.get("CurrentState") == "RUNNING" for pipe in _pipes.all_values()):
        _ensure_poller()


def _restore_state(data):
    if data:
        _restore_pipe_store(data.get("pipes", {}))
        _restore_position_store(data.get("positions", {}))


def _pipe_arn_scope(pipe_arn: str, default_account_id: str | None = None) -> tuple[str, str]:
    try:
        spec = parse_arn(pipe_arn)
    except ArnParseError:
        return default_account_id or get_account_id(), get_region()
    if spec.service != "pipes":
        return default_account_id or get_account_id(), get_region()
    return spec.account_id or default_account_id or get_account_id(), spec.region or get_region()


def _pipe_record_scope(pipe: dict, default_account_id: str | None = None) -> tuple[str, str]:
    return _pipe_arn_scope(pipe.get("Arn", ""), default_account_id)


def _restore_pipe_store(data) -> None:
    if isinstance(data, AccountRegionScopedDict):
        _pipes.update(data)
        return
    if isinstance(data, AccountScopedDict):
        for (account_id, name), pipe in data._data.items():
            restored_account_id, region = _pipe_record_scope(pipe, account_id)
            _pipes.set_scoped(restored_account_id, region, name, copy.deepcopy(pipe))
        return
    if isinstance(data, dict):
        for key, pipe in data.items():
            if isinstance(key, tuple) and len(key) == 3:
                account_id, region, name = key
            elif isinstance(key, tuple) and len(key) == 2:
                account_id, name = key
                account_id, region = _pipe_record_scope(pipe, account_id)
            else:
                name = key
                account_id, region = _pipe_record_scope(pipe)
            _pipes.set_scoped(account_id, region, name, copy.deepcopy(pipe))


def _restore_position_store(data) -> None:
    if isinstance(data, AccountRegionScopedDict):
        _positions.update(data)
        return
    if isinstance(data, AccountScopedDict):
        for (account_id, pipe_arn), position in data._data.items():
            restored_account_id, region = _pipe_arn_scope(pipe_arn, account_id)
            _positions.set_scoped(restored_account_id, region, pipe_arn, position)
        return
    if isinstance(data, dict):
        for key, position in data.items():
            if isinstance(key, tuple) and len(key) == 3:
                account_id, region, pipe_arn = key
            elif isinstance(key, tuple) and len(key) == 2:
                account_id, pipe_arn = key
                account_id, region = _pipe_arn_scope(pipe_arn, account_id)
            else:
                pipe_arn = key
                account_id, region = _pipe_arn_scope(pipe_arn)
            _positions.set_scoped(account_id, region, pipe_arn, position)


def _iter_all_pipes():
    for scoped_key, pipe in list(_pipes.all_items()):
        account_id, region, _name = scoped_key
        yield account_id, region, pipe




def reset():
    _pipes.clear()
    _positions.clear()


def check_same_region(*arns):
    """Raise ValueError when an ARN names a region other than the pipe's."""
    for component_arn in arns:
        try:
            component_region = parse_arn(component_arn).region
        except ArnParseError:
            continue
        if component_region and component_region != get_region():
            raise ValueError(CROSS_REGION_PIPE_ERROR)


def check_target_supported(target: str) -> None:
    """Pipes does not take an SNS FIFO topic as a target (measured on AWS)."""
    if target.startswith("arn:") and target.split(":")[2:3] == ["sns"] and target.endswith(".fifo"):
        raise ValueError("SNS FIFO topics are not supported as a pipe target.")


def register_pipe(
    *,
    name: str,
    source: str,
    target: str,
    role_arn: str = "",
    desired_state: str = "RUNNING",
    starting_position: str = "LATEST",
    tags: dict | None = None,
    description: str = "",
):
    pipe_region = get_region()
    # AWS rejects cross-region source/target ARNs before role validation.
    check_same_region(source, target)
    check_target_supported(target)

    arn = f"arn:aws:pipes:{pipe_region}:{get_account_id()}:pipe/{name}"
    state = "STOPPED" if str(desired_state).upper() == "STOPPED" else "RUNNING"
    start = str(starting_position or "LATEST").upper()
    if start not in ("LATEST", "TRIM_HORIZON"):
        start = "LATEST"

    now = int(time.time())
    _pipes[name] = {
        "Name": name,
        "Arn": arn,
        "RoleArn": role_arn,
        "Description": description or "",
        "Source": source,
        "Target": target,
        "DesiredState": state,
        "CurrentState": state,
        "StartingPosition": start,
        "Tags": tags or {},
        "CreationTime": now,
        "LastModifiedTime": now,
    }
    _positions[arn] = _initial_position(_pipes[name])

    _ensure_poller()
    return _pipes[name]


def delete_pipe(name: str):
    pipe = _pipes.pop(name, None)
    if pipe:
        _positions.pop(pipe["Arn"], None)


def _ensure_poller():
    global _poller_started
    with _poller_lock:
        if not _poller_started:
            t = threading.Thread(target=_poll_loop, daemon=True)
            t.start()
            _poller_started = True


def _poll_loop():
    while True:
        try:
            _poll_once()
        except Exception as e:
            logger.error("Pipes poller error: %s", e)
        time.sleep(1 if _pipes.has_any() else 5)


def _poll_once():
    from ministack.services import dynamodb as _ddb

    stream_records = getattr(_ddb, "_stream_records", None)
    if stream_records is None:
        return

    for pipe_account_id, pipe_region, pipe in _iter_all_pipes():
        account_token = _request_account_id.set(pipe_account_id)
        region_token = _request_region.set(pipe_region)
        try:
            _poll_pipe(_ddb, pipe, pipe_account_id)
        finally:
            _request_region.reset(region_token)
            _request_account_id.reset(account_token)


def _poll_pipe(_ddb, pipe: dict, pipe_account_id: str) -> None:
    if pipe.get("CurrentState") != "RUNNING":
        return
    if _arn_service(pipe.get("Source", "")) != "dynamodb":
        return

    source = _dynamodb_stream_source(pipe.get("Source", ""))
    if source is None:
        return
    source_spec, table_name = source
    if source_spec.account_id != pipe_account_id:
        return

    scope = {"account_id": pipe_account_id, "region": source_spec.region}
    # Positions are absolute stream positions: records expiring off the
    # front of the stream must not shift a pipe's read position.
    horizon = _ddb.stream_start_position(table_name, **scope)
    end = _ddb.stream_end_position(table_name, **scope)
    pos = max(int(_positions.get(pipe["Arn"], 0)), horizon)
    if pos >= end:
        return

    batch = _ddb.stream_records_since(table_name, pos, end - pos, **scope)
    # Records no filter matches are consumed, not retried: the position moves
    # past them whether or not anything is left to deliver.
    passed = [record for record in batch if _passes_filters(pipe, record)]
    if not passed or _deliver_batch(pipe, passed):
        _positions[pipe["Arn"]] = pos + len(batch)


def _passes_filters(pipe: dict, record) -> bool:
    """Whether the record matches one of SourceParameters.FilterCriteria.Filters, or
    the pipe has none. Patterns use EventBridge's syntax and matcher, so operators
    such as equals-ignore-case, exists and prefix behave as in rules. A pattern
    EventBridge would refuse matches nothing."""
    from ministack.services import eventbridge as _eb

    criteria = (pipe.get("SourceParameters") or {}).get("FilterCriteria") or {}
    patterns = [f.get("Pattern") for f in criteria.get("Filters") or []
                if isinstance(f, dict) and f.get("Pattern")]
    if not patterns:
        return True
    for pattern in patterns:
        alternatives = _eb._compiled_pattern(pattern) if isinstance(pattern, str) else None
        if alternatives is None:
            logger.warning("Pipes %s: filter pattern is not valid; it matches nothing: %s",
                           pipe.get("Name"), pattern)
            continue
        if any(_eb._matches_detail(record, alternative) for alternative in alternatives):
            return True
    return False


def _deliver_batch(pipe: dict, batch: list) -> bool:
    """True once every record in the batch has reached the target. A batch that
    did not is left on the stream: the position stays on it and the next poll
    retries it, until the records age out of the retention window."""
    target_arn = pipe.get("Target", "")
    target_service = _arn_service(target_arn)
    if target_service == "states":
        return _start_state_machine_from_records(target_arn, pipe, batch)
    if target_service == "sns":
        for rec in batch:
            _publish_record_to_sns(target_arn, pipe, rec)
        return True
    if target_service == "sqs":
        return _send_records_to_sqs(target_arn, pipe, batch)
    if target_service == "events":
        return _put_records_on_event_bus(target_arn, pipe, batch)
    logger.warning(
        "Pipes %s: holding %d record(s); MiniStack delivers to sns, sqs, states and events, "
        "not to %s", pipe.get("Name"), len(batch), target_service or target_arn)
    return False


def _target_parameters(pipe: dict, key: str) -> dict:
    params = (pipe.get("TargetParameters") or {}).get(key)
    return params if isinstance(params, dict) else {}


_MISSING = object()


def _json_path(document, path: str):
    """The value at a Pipes JSON path (``$.a.b[0]``) in ``document``, or ``_MISSING``.
    Dot notation and array indices: the syntax the Pipes docs list for dynamic
    parameters and input templates, without wildcards."""
    if path == "$":
        return document
    if not path.startswith("$."):
        return _MISSING
    value = document
    for part in path[2:].split("."):
        name, _, rest = part.partition("[")
        steps = [name] if name else []
        while rest:
            index, _, rest = rest.partition("]")
            steps.append(int(index) if index.isdigit() else index)
            rest = rest[1:] if rest.startswith("[") else rest
        for step in steps:
            if isinstance(step, int) and isinstance(value, list) and step < len(value):
                value = value[step]
            elif isinstance(step, str) and isinstance(value, dict) and step in value:
                value = value[step]
            else:
                return _MISSING
    return value


def _dynamic(value, record):
    """A target parameter: a literal, or a ``$.`` JSON path into the record
    (Pipes "dynamic path parameters"). A path the record lacks gives None."""
    if isinstance(value, str) and value.startswith("$."):
        found = _json_path(record, value)
        return None if found is _MISSING else found
    return value


def _send_records_to_sqs(queue_arn: str, pipe: dict, records: list) -> bool:
    """Send each record to the queue as one message, through SQS's own SendMessage,
    so the queue's delay, size limit and FIFO de-duplication apply.
    ``SqsQueueParameters`` (``MessageGroupId``, ``MessageDeduplicationId``) are
    passed for a FIFO queue. Stops at the first refusal; the batch is then held."""
    from ministack.services import sqs as _sqs

    queue = _sqs._queue_by_arn(queue_arn)
    if queue is None:
        logger.warning("Pipes %s: SQS queue %s not found", pipe.get("Name"), queue_arn)
        return False
    params = _target_parameters(pipe, "SqsQueueParameters")
    for record in records:
        request = {"QueueUrl": queue["url"], "MessageBody": json.dumps(record)}
        if queue.get("is_fifo"):
            for key in ("MessageGroupId", "MessageDeduplicationId"):
                value = _dynamic(params.get(key), record)
                if value is not None:
                    request[key] = str(value)
        try:
            _sqs._act_send_message(request, queue["url"])
        except _sqs._Err as exc:
            logger.warning("Pipes %s: SendMessage to %s refused: %s: %s",
                           pipe.get("Name"), queue_arn, exc.code, exc.message)
            return False
    return True


def _event_bus_entry(bus_arn: str, pipe: dict, record) -> dict:
    """The PutEvents entry for one record: the record is the event's ``detail``.

    ``EventBridgeEventBusParameters`` set ``Source``, ``DetailType`` (literals
    or JSON paths), ``Resources`` and ``Time`` (a JSON path). Without them,
    ``source`` is ``Pipe <name>`` and ``detail-type`` ``Event from <eventSource>``:
    not stated in the AWS docs and not captured from AWS; taken from events
    seen from real pipes."""
    params = _target_parameters(pipe, "EventBridgeEventBusParameters")
    event_source = record.get("eventSource", "") if isinstance(record, dict) else ""
    entry = {
        "EventBusName": bus_arn,
        "Source": _dynamic(params.get("Source"), record) or f"Pipe {pipe.get('Name', '')}",
        "DetailType": _dynamic(params.get("DetailType"), record) or f"Event from {event_source}",
        "Detail": json.dumps(record),
        "Resources": [r for r in (_dynamic(r, record) for r in params.get("Resources") or []) if r],
    }
    if params.get("Time"):
        time_value = _dynamic(params["Time"], record)
        if time_value is not None:
            entry["Time"] = time_value
    return entry


def _put_records_on_event_bus(bus_arn: str, pipe: dict, records: list) -> bool:
    """Put each record on the bus through EventBridge's own PutEvents, so the
    bus's rules match and dispatch it as they do any other event."""
    from ministack.services import eventbridge as _eb

    params = _target_parameters(pipe, "EventBridgeEventBusParameters")
    entries = [_event_bus_entry(bus_arn, pipe, record) for record in records]
    for start in range(0, len(entries), 10):  # PutEvents takes at most 10 entries
        request = {"Entries": entries[start:start + 10]}
        if params.get("EndpointId"):
            request["EndpointId"] = params["EndpointId"]
        status, _headers, body = _eb._put_events(request)
        result = json.loads(body) if status < 400 else {}
        if status >= 400 or result.get("FailedEntryCount"):
            logger.warning("Pipes %s: PutEvents on %s failed (%s): %s",
                           pipe.get("Name"), bus_arn, status, body)
            return False
    return True


def _start_state_machine_from_records(sm_arn: str, pipe: dict, records: list) -> bool:
    """Start one execution carrying the batch as the JSON array Pipes delivers.

    `_start_execution` answers the `(status, headers, body)` triple every
    MiniStack handler answers; anything from 400 up means no execution started.
    """
    from ministack.services import stepfunctions as _sfn

    status, _headers, body = _sfn._start_execution({
        "stateMachineArn": sm_arn,
        "input": json.dumps(records),
    })
    if status >= 400:
        logger.warning("Pipes %s: StartExecution on %s failed (%s): %s",
                       pipe.get("Name"), sm_arn, status, body)
        return False
    return True


def _publish_record_to_sns(topic_arn: str, pipe: dict, record: dict):
    from ministack.services import sns as _sns

    try:
        result = _sns.publish_internal(topic_arn, json.dumps(record), f"Pipes {pipe.get('Name', '')}")
    except _sns.SnsPublishError as exc:
        logger.warning("Pipes %s: SNS publish to %s refused: %s", pipe.get("Name"), topic_arn, exc)
        return
    if result is None:
        logger.warning("Pipes %s: SNS topic not found %s", pipe.get("Name"), topic_arn)


def _arn_service(arn: str) -> str:
    """Classify a target ARN for dispatch; invalid stored targets are ignored."""
    try:
        return parse_arn(arn).service
    except ArnParseError:
        return ""


def _table_name_from_stream_arn(stream_arn: str) -> str:
    """Return a DynamoDB table name for Pipes runtime dispatch, or empty string."""
    source = _dynamodb_stream_source(stream_arn)
    return "" if source is None else source[1]


def _dynamodb_stream_source(stream_arn: str):
    """Return the parsed source ARN and table name for a DynamoDB stream."""
    try:
        spec = parse_arn(stream_arn)
    except ArnParseError:
        return None
    if spec.service != "dynamodb":
        return None
    parts = spec.resource.split("/")
    if (
        len(parts) < 4
        or parts[0] != "table"
        or parts[2] != "stream"
        or not parts[1]
        or not parts[3]
    ):
        return None
    return spec, parts[1]


def _pipe_account_id(pipe: dict) -> str:
    try:
        spec = parse_arn(pipe.get("Arn", ""))
    except ArnParseError:
        return get_account_id()
    if spec.service != "pipes" or not spec.account_id:
        return get_account_id()
    return spec.account_id


def _initial_position(pipe: dict) -> int:
    from ministack.services import dynamodb as _ddb

    source = _dynamodb_stream_source(pipe.get("Source", ""))
    if source is None:
        return 0
    source_spec, table_name = source
    pipe_account_id = _pipe_account_id(pipe)
    if source_spec.account_id != pipe_account_id:
        return 0

    stream_records = getattr(_ddb, "_stream_records", None)
    if stream_records is None:
        return 0
    scope = {"account_id": pipe_account_id, "region": source_spec.region}
    if pipe.get("StartingPosition") == "TRIM_HORIZON":
        return _ddb.stream_start_position(table_name, **scope)
    return _ddb.stream_end_position(table_name, **scope)


# ---------------------------------------------------------------------------
# REST/JSON request handler (endpointPrefix "pipes", protocol rest-json).
#
# Op / method / requestUri (botocore pipes/2015-10-07/service-2.json):
#   ListPipes             GET    /v1/pipes
#   CreatePipe            POST   /v1/pipes/{Name}
#   DescribePipe          GET    /v1/pipes/{Name}
#   UpdatePipe            PUT    /v1/pipes/{Name}
#   DeletePipe            DELETE /v1/pipes/{Name}
#   StartPipe             POST   /v1/pipes/{Name}/start
#   StopPipe              POST   /v1/pipes/{Name}/stop
#   ListTagsForResource   GET    /tags/{resourceArn}
#   TagResource           POST   /tags/{resourceArn}
#   UntagResource         DELETE /tags/{resourceArn}
#
# JSON-protocol timestamps are int epoch seconds (Timestamp shape).
# ---------------------------------------------------------------------------

_VALID_REQUESTED_STATE = ("RUNNING", "STOPPED")


def _json_resp(status, body):
    return status, {"Content-Type": "application/json"}, json.dumps(body).encode()


def _error(status, code, message):
    return (
        status,
        {"Content-Type": "application/json", "x-amzn-errortype": code},
        json.dumps({"__type": code, "message": message}).encode(),
    )


def _not_found(name):
    # Matches the real AWS NotFoundException message for DescribePipe/DeletePipe
    # etc. (member: "message").
    return _error(404, "NotFoundException", f"Pipe {name} does not exist.")


def _single(v):
    return v[0] if isinstance(v, list) else v


def _lifecycle_response(pipe):
    """Shape shared by CreatePipe/UpdatePipe/DeletePipe/StartPipe/StopPipe."""
    return {
        "Arn": pipe.get("Arn", ""),
        "Name": pipe.get("Name", ""),
        "DesiredState": pipe.get("DesiredState", "RUNNING"),
        "CurrentState": pipe.get("CurrentState", "RUNNING"),
        "CreationTime": int(pipe.get("CreationTime", 0)),
        "LastModifiedTime": int(pipe.get("LastModifiedTime", pipe.get("CreationTime", 0))),
    }


def _summary(pipe):
    """ListPipes Pipe summary member shape."""
    out = {
        "Name": pipe.get("Name", ""),
        "Arn": pipe.get("Arn", ""),
        "DesiredState": pipe.get("DesiredState", "RUNNING"),
        "CurrentState": pipe.get("CurrentState", "RUNNING"),
        "StateReason": pipe.get("StateReason", ""),
        "CreationTime": int(pipe.get("CreationTime", 0)),
        "LastModifiedTime": int(pipe.get("LastModifiedTime", pipe.get("CreationTime", 0))),
        "Source": pipe.get("Source", ""),
        "Target": pipe.get("Target", ""),
    }
    if pipe.get("Enrichment"):
        out["Enrichment"] = pipe["Enrichment"]
    return out


_OPTIONAL_PIPE_FIELDS = (
    "SourceParameters", "Enrichment", "EnrichmentParameters", "TargetParameters",
    "LogConfiguration", "KmsKeyIdentifier",
)


def _describe_response(pipe):
    out = {
        "Arn": pipe.get("Arn", ""),
        "Name": pipe.get("Name", ""),
        "Description": pipe.get("Description", ""),
        "DesiredState": pipe.get("DesiredState", "RUNNING"),
        "CurrentState": pipe.get("CurrentState", "RUNNING"),
        "StateReason": pipe.get("StateReason", ""),
        "Source": pipe.get("Source", ""),
        "Target": pipe.get("Target", ""),
        "RoleArn": pipe.get("RoleArn", ""),
        "Tags": pipe.get("Tags", {}) or {},
        "CreationTime": int(pipe.get("CreationTime", 0)),
        "LastModifiedTime": int(pipe.get("LastModifiedTime", pipe.get("CreationTime", 0))),
    }
    for key in _OPTIONAL_PIPE_FIELDS:
        if pipe.get(key):
            out[key] = pipe[key]
    return out


# Source fields only CreatePipe takes (absent from the UpdatePipeSource* shapes).
_CREATE_ONLY_SOURCE_FIELDS = {
    "KinesisStreamParameters": ("StartingPosition", "StartingPositionTimestamp"),
    "DynamoDBStreamParameters": ("StartingPosition",),
    "ActiveMQBrokerParameters": ("QueueName",),
    "RabbitMQBrokerParameters": ("QueueName", "VirtualHost"),
    "ManagedStreamingKafkaParameters": ("ConsumerGroupID", "StartingPosition", "TopicName"),
    "SelfManagedKafkaParameters": (
        "AdditionalBootstrapServers", "ConsumerGroupID", "StartingPosition", "TopicName"),
}


def _apply_optional_fields(pipe, body):
    for key in ("Enrichment", "KmsKeyIdentifier"):
        if key in body:
            if body[key]:
                pipe[key] = body[key]
            else:
                pipe.pop(key, None)
    if "LogConfiguration" in body:
        if body["LogConfiguration"]:
            pipe["LogConfiguration"] = copy.deepcopy(body["LogConfiguration"])
        else:
            pipe.pop("LogConfiguration", None)
    for key in ("SourceParameters", "EnrichmentParameters", "TargetParameters"):
        sent = body.get(key)
        if not isinstance(sent, dict):
            continue
        merged = copy.deepcopy(pipe.get(key) or {})
        for sub, value in sent.items():
            kept = {}
            if key == "SourceParameters" and isinstance(merged.get(sub), dict):
                kept = {f: merged[sub][f] for f in _CREATE_ONLY_SOURCE_FIELDS.get(sub, ())
                        if f in merged[sub]}
            merged[sub] = copy.deepcopy(value)
            if kept and isinstance(merged[sub], dict):
                merged[sub].update(kept)
        pipe[key] = merged


def _find_pipe_by_arn(arn):
    for pipe in _pipes.values():
        if pipe.get("Arn") == arn:
            return pipe
    return None


def _create_pipe(name, body):
    if name in _pipes:
        return _error(409, "ConflictException", f"Pipe {name} already exists.")
    source = body.get("Source", "")
    target = body.get("Target", "")
    role_arn = body.get("RoleArn", "")
    desired = str(body.get("DesiredState", "RUNNING")).upper()
    if desired not in _VALID_REQUESTED_STATE:
        return _error(
            400,
            "ValidationException",
            f"DesiredState must be one of {list(_VALID_REQUESTED_STATE)}.",
        )
    source_params = body.get("SourceParameters") or {}
    stream_params = (
        source_params.get("DynamoDBStreamParameters")
        or source_params.get("KinesisStreamParameters")
        or {}
    ) if isinstance(source_params, dict) else {}
    try:
        pipe = register_pipe(
            name=name,
            source=source,
            target=target,
            role_arn=role_arn,
            desired_state=desired,
            starting_position=stream_params.get("StartingPosition") or "LATEST",
            tags=body.get("Tags") or {},
            description=body.get("Description", "") or "",
        )
    except ValueError as e:
        return _error(400, "ValidationException", str(e))
    _apply_optional_fields(pipe, body)
    _pipes[name] = pipe
    return _json_resp(200, _lifecycle_response(pipe))


def _describe_pipe(name):
    pipe = _pipes.get(name)
    if pipe is None:
        return _not_found(name)
    return _json_resp(200, _describe_response(pipe))


def _update_pipe(name, body):
    pipe = _pipes.get(name)
    if pipe is None:
        return _not_found(name)
    try:
        check_target_supported(body.get("Target") or "")
    except ValueError as e:
        return _error(400, "ValidationException", str(e))
    if "Description" in body:
        pipe["Description"] = body.get("Description", "") or ""
    if "RoleArn" in body:
        pipe["RoleArn"] = body.get("RoleArn", "") or ""
    if "Target" in body and body.get("Target"):
        pipe["Target"] = body["Target"]
    if "DesiredState" in body:
        desired = str(body.get("DesiredState", "")).upper()
        if desired not in _VALID_REQUESTED_STATE:
            return _error(
                400,
                "ValidationException",
                f"DesiredState must be one of {list(_VALID_REQUESTED_STATE)}.",
            )
        pipe["DesiredState"] = desired
        pipe["CurrentState"] = desired
    _apply_optional_fields(pipe, body)
    pipe["LastModifiedTime"] = int(time.time())
    _pipes[name] = pipe
    return _json_resp(200, _lifecycle_response(pipe))


def _delete_pipe_op(name):
    pipe = _pipes.get(name)
    if pipe is None:
        return _not_found(name)
    resp = {
        "Arn": pipe.get("Arn", ""),
        "Name": pipe.get("Name", ""),
        "DesiredState": "DELETED",
        "CurrentState": "DELETING",
        "CreationTime": int(pipe.get("CreationTime", 0)),
        "LastModifiedTime": int(pipe.get("LastModifiedTime", pipe.get("CreationTime", 0))),
    }
    delete_pipe(name)
    return _json_resp(200, resp)


def _set_state(name, desired):
    pipe = _pipes.get(name)
    if pipe is None:
        return _not_found(name)
    pipe["DesiredState"] = desired
    pipe["CurrentState"] = desired
    pipe["LastModifiedTime"] = int(time.time())
    _pipes[name] = pipe
    return _json_resp(200, _lifecycle_response(pipe))


def _list_pipes(query):
    name_prefix = _single(query.get("NamePrefix"))
    desired_state = _single(query.get("DesiredState"))
    current_state = _single(query.get("CurrentState"))
    source_prefix = _single(query.get("SourcePrefix"))
    target_prefix = _single(query.get("TargetPrefix"))
    limit = _single(query.get("Limit"))

    pipes = sorted(_pipes.values(), key=lambda p: p.get("Name", ""))
    result = []
    for pipe in pipes:
        if name_prefix and not pipe.get("Name", "").startswith(name_prefix):
            continue
        if desired_state and pipe.get("DesiredState") != desired_state:
            continue
        if current_state and pipe.get("CurrentState") != current_state:
            continue
        if source_prefix and not pipe.get("Source", "").startswith(source_prefix):
            continue
        if target_prefix and not pipe.get("Target", "").startswith(target_prefix):
            continue
        result.append(_summary(pipe))

    next_token = None
    if limit:
        try:
            n = int(limit)
            if 0 < n < len(result):
                next_token = result[n]["Name"]
                result = result[:n]
        except (TypeError, ValueError):
            pass

    body = {"Pipes": result}
    if next_token is not None:
        body["NextToken"] = next_token
    return _json_resp(200, body)


def _list_tags(arn):
    pipe = _find_pipe_by_arn(arn)
    if pipe is None:
        return _not_found(arn)
    return _json_resp(200, {"tags": pipe.get("Tags", {}) or {}})


def _tag_resource(arn, body):
    pipe = _find_pipe_by_arn(arn)
    if pipe is None:
        return _not_found(arn)
    pipe.setdefault("Tags", {}).update(body.get("tags", {}) or {})
    return _json_resp(200, {})


def _untag_resource(arn, query):
    pipe = _find_pipe_by_arn(arn)
    if pipe is None:
        return _not_found(arn)
    keys = query.get("tagKeys", [])
    if not isinstance(keys, list):
        keys = [keys]
    tags = pipe.setdefault("Tags", {})
    for k in keys:
        tags.pop(k, None)
    return _json_resp(200, {})


async def handle_request(method, path, headers, body_bytes, query_params):
    try:
        body = json.loads(body_bytes) if body_bytes else {}
    except (json.JSONDecodeError, TypeError):
        body = {}
    if not isinstance(body, dict):
        body = {}

    # Pipe lifecycle sub-actions: /v1/pipes/{Name}/start | /stop
    if path.startswith("/v1/pipes/"):
        rest = path[len("/v1/pipes/"):]
        if rest.endswith("/start"):
            return _set_state(unquote(rest[: -len("/start")]), "RUNNING")
        if rest.endswith("/stop"):
            return _set_state(unquote(rest[: -len("/stop")]), "STOPPED")
        name = unquote(rest)
        if name:
            if method == "POST":
                return _create_pipe(name, body)
            if method == "GET":
                return _describe_pipe(name)
            if method == "PUT":
                return _update_pipe(name, body)
            if method == "DELETE":
                return _delete_pipe_op(name)

    if path == "/v1/pipes" and method == "GET":
        return _list_pipes(query_params)

    # Tag routes: /tags/{resourceArn+}
    if path.startswith("/tags/"):
        arn = unquote(path[len("/tags/"):])
        if method == "GET":
            return _list_tags(arn)
        if method == "POST":
            return _tag_resource(arn, body)
        if method == "DELETE":
            return _untag_resource(arn, query_params)

    return _error(400, "ValidationException", f"No route for {method} {path}")
