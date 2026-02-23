from claude_context.capture.observation import AggregatedObservation, Observation


def aggregate(observations: list[Observation]) -> list[AggregatedObservation]:
    """Merge a batch of observations by (service, caller, method, path) key."""
    groups: dict[tuple[str, str, str, str], AggregatedObservation] = {}

    for obs in observations:
        key = (obs.service_name, obs.caller, obs.method, obs.path_template)

        if key not in groups:
            groups[key] = AggregatedObservation(
                service_name=obs.service_name,
                caller=obs.caller,
                method=obs.method,
                path_template=obs.path_template,
                request_fields=set(obs.request_fields),
                request_headers=set(obs.request_headers),
                query_params=set(obs.query_params),
                response_codes={str(obs.status_code)},
                call_count=1,
                first_seen=obs.timestamp,
                last_seen=obs.timestamp,
            )
        else:
            agg = groups[key]
            agg.request_fields |= obs.request_fields
            agg.request_headers |= obs.request_headers
            agg.query_params |= obs.query_params
            agg.response_codes.add(str(obs.status_code))
            agg.call_count += 1
            if obs.timestamp < agg.first_seen:
                agg.first_seen = obs.timestamp
            if obs.timestamp > agg.last_seen:
                agg.last_seen = obs.timestamp

    return list(groups.values())
