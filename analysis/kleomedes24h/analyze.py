#!/usr/bin/env python3
import collections
import datetime as dt
import json
import math
import statistics
import urllib.parse
import urllib.request

GRAPHQL_URL = "https://data.pocket.network/"
REST_URL = "https://sauron-api.infra.pocket.network"
KLEOMEDES_DOMAIN = "kleomedes.network"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "kleomedes-analytics-research/0.1",
}


def request_json(url, payload=None, timeout=90):
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(url, data=data, headers=HEADERS)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def gql(query, variables=None):
    body = request_json(
        GRAPHQL_URL,
        payload={"query": query, "variables": variables or {}},
    )
    if body.get("errors"):
        raise RuntimeError(body["errors"])
    return body["data"]


def paginated_rest(path, collection):
    output = []
    next_key = ""
    while True:
        params = {"dehydrated": "false", "pagination.limit": "200"}
        if next_key:
            params["pagination.key"] = next_key
        url = f"{REST_URL}{path}?{urllib.parse.urlencode(params)}"
        body = request_json(url)
        output.extend(body.get(collection) or [])
        next_key = ((body.get("pagination") or {}).get("next_key") or "")
        if not next_key:
            return output


def registrable_domain(host):
    host = (host or "").lower().strip(".")
    labels = [part for part in host.split(".") if part]
    if not labels:
        return None
    if len(labels) <= 2:
        return host
    suffix = ".".join(labels[-2:])
    second_level = {"co.uk", "org.uk", "com.au", "net.au", "co.jp", "com.br"}
    return ".".join(labels[-3:]) if suffix in second_level else suffix


def endpoint_domain(url):
    if not url:
        return None
    normalized = url if "://" in url else f"https://{url}"
    return registrable_domain(urllib.parse.urlparse(normalized).hostname)


def parse_int(value):
    if value is None:
        return 0
    return int(str(value).split(".", 1)[0] or "0")


def to_pokt(upokt):
    return upokt / 1_000_000


def load_supplier_domains():
    result = {}
    offset = 0
    while True:
        page = gql(
            """
            query Suppliers($offset: Int!) {
              suppliers(first: 200, offset: $offset) {
                nodes {
                  ownerId
                  operatorId
                  serviceConfigs(first: 100) {
                    nodes { domains endpoints }
                  }
                }
              }
            }
            """,
            {"offset": offset},
        )["suppliers"]["nodes"]
        if not page:
            return result
        for supplier in page:
            domains = []
            endpoint_urls = []
            for config in ((supplier.get("serviceConfigs") or {}).get("nodes") or []):
                if not config:
                    continue
                domains.extend(config.get("domains") or [])
                for endpoint in config.get("endpoints") or []:
                    if isinstance(endpoint, dict):
                        endpoint_urls.append(endpoint.get("url"))
                    elif isinstance(endpoint, str):
                        endpoint_urls.append(endpoint)
            explicit = sorted({str(domain).lower() for domain in domains if domain})
            candidates = [endpoint_domain(url) for url in endpoint_urls]
            candidates = [candidate for candidate in candidates if candidate]
            result[supplier["operatorId"]] = (
                explicit[0]
                if explicit
                else collections.Counter(candidates).most_common(1)[0][0]
                if candidates
                else f"owner:{supplier.get('ownerId') or supplier['operatorId']}"
            )
        offset += len(page)


def main():
    supplier_domains = load_supplier_domains()

    services = {}
    for service in paginated_rest(
        "/pokt-network/poktroll/service/service", "service"
    ):
        service_id = service.get("id")
        if not service_id:
            continue
        services[service_id] = {
            "name": service.get("name") or service_id,
            "cu_per_relay": parse_int(
                service.get("compute_units_per_relay", service.get("computeUnitsPerRelay"))
            ),
        }

    configured_by_supplier = {}
    staked_supplier_count = collections.Counter()
    for supplier in paginated_rest(
        "/pokt-network/poktroll/supplier/supplier", "supplier"
    ):
        operator = supplier.get("operator_address")
        owner = supplier.get("owner_address")
        if not operator:
            continue
        service_ids = sorted(
            {
                item.get("service_id")
                for item in supplier.get("services") or []
                if item.get("service_id")
            }
        )
        configured_by_supplier[operator] = service_ids
        staked_supplier_count.update(service_ids)
        if operator not in supplier_domains:
            endpoint_urls = [
                endpoint.get("url")
                for item in supplier.get("services") or []
                for endpoint in item.get("endpoints") or []
                if isinstance(endpoint, dict)
            ]
            candidates = [endpoint_domain(url) for url in endpoint_urls]
            candidates = [candidate for candidate in candidates if candidate]
            supplier_domains[operator] = (
                collections.Counter(candidates).most_common(1)[0][0]
                if candidates
                else f"owner:{owner or operator}"
            )

    window_end = dt.datetime.now(dt.timezone.utc)
    window_start = window_end - dt.timedelta(hours=24)
    response = gql(
        """
        query Claims24h($start: Datetime!) {
          status: _metadata {
            targetHeight
            lastProcessedHeight
            lastProcessedTimestamp
            lastFinalizedVerifiedHeight
            indexerHealthy
          }
          claims: eventClaimSettleds(
            filter: { block: { timestamp: { greaterThanOrEqualTo: $start } } }
          ) {
            groupedAggregates(groupBy: [SUPPLIER_ID, SERVICE_ID]) {
              keys
              sum { claimedAmount numRelays }
            }
          }
        }
        """,
        {"start": window_start.isoformat().replace("+00:00", "Z")},
    )

    rows = []
    for aggregate in response["claims"]["groupedAggregates"] or []:
        keys = aggregate.get("keys") or []
        if len(keys) < 2:
            continue
        supplier, service_id = keys[:2]
        sums = aggregate.get("sum") or {}
        reward_upokt = parse_int(sums.get("claimedAmount"))
        relays = parse_int(sums.get("numRelays"))
        if reward_upokt <= 0 and relays <= 0:
            continue
        rows.append(
            {
                "supplier": supplier,
                "provider": supplier_domains.get(supplier, f"supplier:{supplier}"),
                "service_id": service_id,
                "reward_upokt": reward_upokt,
                "relays": relays,
                "compute_units": relays
                * (services.get(service_id) or {}).get("cu_per_relay", 0),
            }
        )

    provider_totals = collections.defaultdict(
        lambda: {
            "reward_upokt": 0,
            "relays": 0,
            "compute_units": 0,
            "suppliers": set(),
            "services": set(),
        }
    )
    service_totals = collections.defaultdict(
        lambda: {
            "reward_upokt": 0,
            "relays": 0,
            "compute_units": 0,
            "suppliers": set(),
            "providers": set(),
        }
    )
    provider_service = collections.defaultdict(
        lambda: {
            "reward_upokt": 0,
            "relays": 0,
            "compute_units": 0,
            "suppliers": set(),
        }
    )

    for row in rows:
        provider = provider_totals[row["provider"]]
        provider["reward_upokt"] += row["reward_upokt"]
        provider["relays"] += row["relays"]
        provider["compute_units"] += row["compute_units"]
        provider["suppliers"].add(row["supplier"])
        provider["services"].add(row["service_id"])

        service = service_totals[row["service_id"]]
        service["reward_upokt"] += row["reward_upokt"]
        service["relays"] += row["relays"]
        service["compute_units"] += row["compute_units"]
        service["suppliers"].add(row["supplier"])
        service["providers"].add(row["provider"])

        pair = provider_service[(row["provider"], row["service_id"])]
        pair["reward_upokt"] += row["reward_upokt"]
        pair["relays"] += row["relays"]
        pair["compute_units"] += row["compute_units"]
        pair["suppliers"].add(row["supplier"])

    network_reward = sum(value["reward_upokt"] for value in provider_totals.values())
    network_relays = sum(value["relays"] for value in provider_totals.values())
    reward_order = sorted(
        provider_totals,
        key=lambda provider: (-provider_totals[provider]["reward_upokt"], provider),
    )
    relay_order = sorted(
        provider_totals,
        key=lambda provider: (-provider_totals[provider]["relays"], provider),
    )
    kleo = provider_totals.get(
        KLEOMEDES_DOMAIN,
        {
            "reward_upokt": 0,
            "relays": 0,
            "compute_units": 0,
            "suppliers": set(),
            "services": set(),
        },
    )
    kleo_operators = sorted(
        operator
        for operator, provider in supplier_domains.items()
        if provider == KLEOMEDES_DOMAIN
    )
    kleo_configured = sorted(
        {
            service_id
            for operator in kleo_operators
            for service_id in configured_by_supplier.get(operator, [])
        }
    )

    providers = []
    for provider in reward_order:
        totals = provider_totals[provider]
        providers.append(
            {
                "provider": provider,
                "reward_pokt": to_pokt(totals["reward_upokt"]),
                "reward_share_pct": 100 * totals["reward_upokt"] / network_reward
                if network_reward
                else 0,
                "relays": totals["relays"],
                "compute_units": totals["compute_units"],
                "earning_suppliers": len(totals["suppliers"]),
                "earning_services": len(totals["services"]),
            }
        )

    kleo_services = []
    for service_id in sorted(
        kleo["services"],
        key=lambda current: -provider_service[(KLEOMEDES_DOMAIN, current)][
            "reward_upokt"
        ],
    ):
        own = provider_service[(KLEOMEDES_DOMAIN, service_id)]
        market = service_totals[service_id]
        ranking = sorted(
            market["providers"],
            key=lambda provider: (
                -provider_service[(provider, service_id)]["reward_upokt"],
                provider,
            ),
        )
        kleo_services.append(
            {
                "service_id": service_id,
                "service_name": (services.get(service_id) or {}).get("name", service_id),
                "reward_pokt": to_pokt(own["reward_upokt"]),
                "network_reward_pokt": to_pokt(market["reward_upokt"]),
                "market_share_pct": 100 * own["reward_upokt"] / market["reward_upokt"],
                "provider_rank": ranking.index(KLEOMEDES_DOMAIN) + 1,
                "active_providers": len(market["providers"]),
                "relays": own["relays"],
                "network_relays": market["relays"],
                "earning_suppliers": len(own["suppliers"]),
            }
        )

    opportunities = []
    for service_id, market in service_totals.items():
        if service_id in kleo_configured or market["reward_upokt"] <= 0:
            continue
        provider_count = len(market["providers"])
        earning_supplier_count = len(market["suppliers"])
        reward_pokt = to_pokt(market["reward_upokt"])
        per_provider = reward_pokt / max(provider_count, 1)
        per_earning_supplier = reward_pokt / max(earning_supplier_count, 1)
        score = (
            per_provider
            * math.log10(max(market["relays"], 10))
            / math.sqrt(max(earning_supplier_count, 1))
        )
        opportunities.append(
            {
                "service_id": service_id,
                "service_name": (services.get(service_id) or {}).get("name", service_id),
                "network_reward_pokt": reward_pokt,
                "network_relays": market["relays"],
                "network_compute_units": market["compute_units"],
                "active_providers": provider_count,
                "earning_suppliers": earning_supplier_count,
                "staked_suppliers": staked_supplier_count.get(service_id, 0),
                "reward_per_active_provider_pokt": per_provider,
                "reward_per_earning_supplier_pokt": per_earning_supplier,
                "screening_score": score,
            }
        )
    opportunities.sort(
        key=lambda item: (
            -item["screening_score"],
            -item["network_reward_pokt"],
            item["service_id"],
        )
    )

    provider_rewards = [item["reward_pokt"] for item in providers]
    report = {
        "generated_at": window_end.isoformat(),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "indexer": response["status"],
        "network": {
            "reward_pokt": to_pokt(network_reward),
            "relays": network_relays,
            "active_providers": len(provider_totals),
            "median_provider_reward_pokt": statistics.median(provider_rewards)
            if provider_rewards
            else 0,
        },
        "kleomedes": {
            "reward_pokt": to_pokt(kleo["reward_upokt"]),
            "reward_share_pct": 100 * kleo["reward_upokt"] / network_reward
            if network_reward
            else 0,
            "reward_rank": reward_order.index(KLEOMEDES_DOMAIN) + 1
            if KLEOMEDES_DOMAIN in reward_order
            else None,
            "relay_rank": relay_order.index(KLEOMEDES_DOMAIN) + 1
            if KLEOMEDES_DOMAIN in relay_order
            else None,
            "relays": kleo["relays"],
            "compute_units": kleo["compute_units"],
            "earning_suppliers": len(kleo["suppliers"]),
            "earning_services": len(kleo["services"]),
            "known_operators": kleo_operators,
            "configured_services": kleo_configured,
            "services": kleo_services,
        },
        "providers": providers,
        "opportunities_not_configured": opportunities[:30],
    }

    print("KLEOMEDES_ANALYSIS_BEGIN")
    print(json.dumps(report, indent=2, sort_keys=True))
    print("KLEOMEDES_ANALYSIS_END")


if __name__ == "__main__":
    main()
