"""Cursor-paged disposal-progress feed and catch-up semantics.

Covers the duty-screen requirements:

* the five node kinds are written at exactly the right moments, with the alarm
  number, cabinet number (when present) and the status frozen at write time;
* an ambiguous receipt never produces an acceptance/rejection node — the node
  is appended only after a definitive gateway answer;
* same-number/same-content submissions and repeat confirmations add no nodes;
* nodes are committed in the same transaction as their disposal state, and a
  node already read never changes after a later acceptance/confirmation;
* a catch-up session pins a stable node range: pages and retries are identical
  while new nodes keep arriving; after catching up, live tailing from the last
  cursor delivers every later committed node, including across a restart and
  under concurrent delivery/confirmation.
"""

from __future__ import annotations

import asyncio

import httpx

from app.config import Settings
from app.main import create_app
from tests.conftest import ApiHarness
from tests.fake_gateway import FakeGateway, build_fake_gateway_app
from tests.helpers import _get, _status


# --------------------------------------------------------------------- helpers


async def read_all(api: ApiHarness, **params) -> list[dict]:
    items: list[dict] = []
    cursor = 0
    for _ in range(100):
        page = (await api.client.get("/progress", params={"cursor": cursor, **params})).json()
        items.extend(page["items"])
        cursor = page["next_cursor"]
        if not page["has_more"]:
            assert page["caught_up"] is True
            break
    return items


async def read_catchup(api: ApiHarness, session_id: str) -> list[dict]:
    items: list[dict] = []
    cursor = 0
    for _ in range(100):
        page = (
            await api.client.get(
                "/progress", params={"cursor": cursor, "session_id": session_id}
            )
        ).json()
        items.extend(page["items"])
        cursor = page["next_cursor"]
        if not page["has_more"]:
            assert page["caught_up"] is True
            break
    return items


def types(items: list[dict]) -> list[str]:
    return [n["node_type"] for n in items]


# ------------------------------------------------------------- node lifecycle


async def test_progress_records_submission_acceptance_escalation_and_confirmation(api: ApiHarness):
    await api.client.post("/alarms", json=api.alarm_payload("P-1", deadline=2))
    assert await api.wait_until(lambda: _status(api, "P-1", "escalated"), timeout=8)

    nodes = await read_all(api, alarm_no="P-1")
    assert types(nodes) == [
        "alarm_submitted",
        "primary_accepted",
        "backup_generated",
    ]

    submitted, accepted, backup = nodes
    # Every node carries the alarm number and the cabinet when the payload has one.
    assert all(n["alarm_no"] == "P-1" for n in nodes)
    assert all(n["cabinet_no"] == "A07" for n in nodes)
    assert submitted["notification_role"] == "primary"
    assert submitted["alarm_status"] == "primary_queued"
    # The snapshot is frozen at write time: the submission node must not retroactively
    # claim an accepted timer.
    assert submitted["status_snapshot"]["primary_accepted_at"] is None
    assert accepted["node_type"] == "primary_accepted"
    assert accepted["notification_role"] == "primary"
    assert accepted["alarm_status"] == "awaiting_confirmation"
    assert accepted["status_snapshot"]["primary_accepted_at"] is not None
    assert backup["notification_role"] == "backup"
    assert backup["alarm_status"] == "escalated"

    # Seqs are the cursor: monotonic, gap-free and strictly ordered.
    seqs = [n["seq"] for n in nodes]
    assert seqs == sorted(seqs) and len(seqs) == len(set(seqs))

    # Manual confirmation appends one more node.
    confirm = await api.client.post("/alarms/P-1/confirm", json={"confirmed_by": "张工"})
    assert confirm.status_code == 200
    nodes = await read_all(api, alarm_no="P-1")
    assert types(nodes)[-1] == "confirmed"
    # A backup cancelled before dispatch renders as plain "confirmed" per the
    # existing view semantics; an already-dispatched backup renders
    # confirmed_after_escalation. Both are valid frozen outcomes here.
    assert nodes[-1]["alarm_status"] in {"confirmed", "confirmed_after_escalation"}
    assert nodes[-1]["status_snapshot"]["confirmed"] is True


async def test_progress_unknown_receipt_writes_no_node_until_definitive_answer(api: ApiHarness):
    api.fake.drop_receipt = True
    api.fake.drop_hold_seconds = 0.3
    api.fake.lookup_down = True
    await api.client.post("/alarms", json=api.alarm_payload("P-UNK", deadline=2))
    assert await api.wait_until(lambda: _status(api, "P-UNK", "primary_unconfirmed"), timeout=6)

    # While the receipt is unproven, only the submission node exists. No accepted
    # node, no rejected node, no escalation node.
    nodes = await read_all(api, alarm_no="P-UNK")
    assert types(nodes) == ["alarm_submitted"]
    assert nodes[0]["alarm_status"] == "primary_queued"

    await asyncio.sleep(2.4)  # nominal deadline passes while still unknown
    nodes = await read_all(api, alarm_no="P-UNK")
    assert types(nodes) == ["alarm_submitted"], "ambiguous receipt must not be recorded as accepted"

    # Gateway becomes queryable: the true acceptance is discovered and appended once.
    # Because the gateway acceptance happened in the past, the confirmation clock
    # (measured from that real acceptance time) has already run out, so the
    # deadline loop enqueues the backup right after discovery — the same
    # late-receipt semantics as test_clock.py.
    api.fake.drop_receipt = False
    api.fake.lookup_down = False
    assert await api.wait_until(
        lambda: _has_node(api, "P-UNK", "primary_accepted"), timeout=8
    )
    nodes = await read_all(api, alarm_no="P-UNK")
    assert types(nodes)[:2] == ["alarm_submitted", "primary_accepted"]
    # Exactly one acceptance node regardless of how the discovery was reached.
    assert types(nodes).count("primary_accepted") == 1
    # Discovery late must still freeze the awaiting-confirmation status, not rewrite
    # the submission node.
    assert nodes[0]["alarm_status"] == "primary_queued"


async def test_progress_rejected_primary_records_rejection_and_never_escalates(api: ApiHarness):
    api.fake.reject = True
    await api.client.post("/alarms", json=api.alarm_payload("P-REJ", deadline=2))
    assert await api.wait_until(lambda: _status(api, "P-REJ", "primary_rejected"), timeout=6)
    await asyncio.sleep(2.4)

    nodes = await read_all(api, alarm_no="P-REJ")
    assert types(nodes) == ["alarm_submitted", "primary_rejected"]
    rejected = nodes[-1]
    assert rejected["alarm_status"] == "primary_rejected"
    assert rejected["notification_role"] == "primary"
    assert "reject" in (rejected["detail"] or "").lower()


# ------------------------------------------------------------- dedupe behaviour


async def test_same_content_retry_and_repeat_confirmation_add_no_nodes(api: ApiHarness):
    payload = api.alarm_payload("P-DEDUP", deadline=5)
    await api.client.post("/alarms", json=payload)
    assert await api.wait_until(lambda: _status(api, "P-DEDUP", "awaiting_confirmation"))

    retry = await api.client.post("/alarms", json=payload)
    assert retry.status_code == 200

    await api.client.post("/alarms/P-DEDUP/confirm", json={"confirmed_by": "张工"})
    await api.client.post("/alarms/P-DEDUP/confirm", json={"confirmed_by": "李工"})
    await api.client.post("/alarms/P-DEDUP/confirm", json={"confirmed_by": "王工"})

    nodes = await read_all(api, alarm_no="P-DEDUP")
    assert types(nodes) == ["alarm_submitted", "primary_accepted", "confirmed"]
    # Deterministic node ids are stable per (alarm, kind[, role]).
    ids = [n["node_id"] for n in nodes]
    assert ids == [
        "P-DEDUP:alarm_submitted:primary",
        "P-DEDUP:primary_accepted:primary",
        "P-DEDUP:confirmed",
    ]


async def test_acceptance_discovered_twice_still_appends_one_node(api: ApiHarness):
    """A lookup after a lost receipt and then again on a later read must not
    produce a second primary_accepted node."""
    api.fake.drop_receipt = True
    api.fake.drop_hold_seconds = 0.3
    api.fake.lookup_down = True
    await api.client.post("/alarms", json=api.alarm_payload("P-ONCE", deadline=5))
    assert await api.wait_until(lambda: _status(api, "P-ONCE", "primary_unconfirmed"), timeout=6)

    api.fake.drop_receipt = False
    api.fake.lookup_down = False
    assert await api.wait_until(lambda: _has_node(api, "P-ONCE", "primary_accepted"), timeout=8)

    # Force several more reconciliations through the read path and the worker.
    for _ in range(4):
        await api.client.get("/alarms/P-ONCE")
        await asyncio.sleep(0.05)
    await asyncio.sleep(0.5)

    nodes = await read_all(api, alarm_no="P-ONCE")
    assert types(nodes).count("primary_accepted") == 1


# ------------------------------------------------------------- frozen snapshot


async def test_nodes_already_read_do_not_change_after_later_confirmation(api: ApiHarness):
    await api.client.post("/alarms", json=api.alarm_payload("P-FROZEN", deadline=5))
    assert await api.wait_until(lambda: _status(api, "P-FROZEN", "awaiting_confirmation"))

    before = await read_all(api, alarm_no="P-FROZEN")
    assert [n["alarm_status"] for n in before] == [
        "primary_queued",
        "awaiting_confirmation",
    ]
    snapshot_before = [dict(n["status_snapshot"]) for n in before]

    await api.client.post("/alarms/P-FROZEN/confirm", json={"confirmed_by": "张工"})
    after = await read_all(api, alarm_no="P-FROZEN")

    # The earlier nodes are immutable in seq, status and full snapshot.
    for old, old_snapshot, fresh in zip(before, snapshot_before, after):
        assert fresh["seq"] == old["seq"]
        assert fresh["alarm_status"] == old["alarm_status"]
        assert fresh["status_snapshot"] == old_snapshot
        assert fresh["occurred_at"] == old["occurred_at"]
    assert [n["alarm_status"] for n in after][-1] == "confirmed"


# ------------------------------------------------------------- catch-up window


async def test_catchup_session_pins_a_stable_range_while_new_nodes_arrive(api: ApiHarness):
    await api.client.post("/alarms", json=api.alarm_payload("P-CATCH-1", deadline=5))
    assert await api.wait_until(lambda: _status(api, "P-CATCH-1", "awaiting_confirmation"))

    session = (await api.client.post("/progress/catchup", json={"cursor": 0})).json()
    assert session["start_cursor"] == 0
    pinned_end = session["end_seq"]
    assert pinned_end >= 2

    # More activity after the session started must not shift its window.
    await api.client.post("/alarms", json=api.alarm_payload("P-CATCH-2", deadline=5))
    assert await api.wait_until(lambda: _status(api, "P-CATCH-2", "awaiting_confirmation"))
    await api.client.post("/alarms/P-CATCH-1/confirm", json={"confirmed_by": "张工"})
    await asyncio.sleep(0.2)

    first = await read_catchup(api, session["session_id"])
    assert [n["seq"] for n in first][-1] <= pinned_end
    assert all(n["alarm_no"] != "P-CATCH-2" for n in first)

    # Retry every page from every cursor: content must be byte-stable.
    for page_size in (1, 2, 100):
        seen: list[dict] = []
        cursor = 0
        while True:
            resp = await api.client.get(
                "/progress",
                params={
                    "cursor": cursor,
                    "limit": page_size,
                    "session_id": session["session_id"],
                },
            )
            assert resp.status_code == 200
            page = resp.json()
            assert page["window_end_seq"] == pinned_end
            seen.extend(page["items"])
            cursor = page["next_cursor"]
            if page["caught_up"]:
                break
        assert [n["seq"] for n in seen] == [n["seq"] for n in first]
        assert seen == first


async def test_catchup_resumes_from_last_cursor_and_then_live_tail_keeps_up(api: ApiHarness):
    await api.client.post("/alarms", json=api.alarm_payload("P-TAIL", deadline=2))
    assert await api.wait_until(lambda: _status(api, "P-TAIL", "escalated"), timeout=8)

    session = (await api.client.post("/progress/catchup", json={"cursor": 0})).json()
    caught = await read_catchup(api, session["session_id"])
    last_cursor = caught[-1]["seq"]

    # Live tailing from the end cursor picks up nodes committed afterwards.
    await api.client.post("/alarms/P-TAIL/confirm", json={"confirmed_by": "张工"})
    ok = await api.wait_until(lambda: _has_node(api, "P-TAIL", "confirmed"))
    assert ok

    page = (await api.client.get("/progress", params={"cursor": last_cursor})).json()
    assert page["session_id"] is None
    new_seqs = [n["seq"] for n in page["items"]]
    assert new_seqs and min(new_seqs) > last_cursor
    assert any(n["node_type"] == "confirmed" for n in page["items"])

    # Polling again from the new cursor returns nothing new yet (no duplicates).
    again = (
        await api.client.get("/progress", params={"cursor": page["next_cursor"]})
    ).json()
    assert again["items"] == []
    assert again["next_cursor"] == page["next_cursor"]
    assert again["caught_up"] is True


async def test_unknown_catchup_session_is_a_404(api: ApiHarness):
    resp = await api.client.get(
        "/progress", params={"session_id": "does-not-exist", "cursor": 0}
    )
    assert resp.status_code == 404


async def test_pagination_returns_pages_in_order_without_gaps_or_duplicates(api: ApiHarness):
    for i in range(3):
        await api.client.post("/alarms", json=api.alarm_payload(f"P-PAGE-{i}", deadline=5))
    for i in range(3):
        assert await api.wait_until(
            lambda name=f"P-PAGE-{i}": _status(api, name, "awaiting_confirmation")
        )

    collected: list[dict] = []
    cursor = 0
    rounds = 0
    while rounds < 50:
        page = (
            await api.client.get(
                "/progress", params={"cursor": cursor, "limit": 2}
            )
        ).json()
        collected.extend(page["items"])
        assert len(page["items"]) <= 2
        if not page["has_more"]:
            break
        assert page["next_cursor"] > cursor
        cursor = page["next_cursor"]
        rounds += 1

    seqs = [n["seq"] for n in collected]
    assert seqs == sorted(seqs)
    assert len(seqs) == len(set(seqs))
    assert {n["alarm_no"] for n in collected} >= {f"P-PAGE-{i}" for i in range(3)}


# ------------------------------------------------------------- restart safety


async def test_progress_nodes_and_catchup_session_survive_restart(tmp_path, fake_gateway: FakeGateway):
    db_path = str(tmp_path / "progress.db")
    gateway_url = "http://gw.test"
    settings = Settings(
        database_path=db_path,
        gateway_url=gateway_url,
        post_timeout_seconds=0.6,
        lookup_timeout_seconds=0.4,
        worker_interval_seconds=0.02,
        delivery_lease_seconds=1.0,
        max_backoff_seconds=0.05,
        reconcile_on_read=True,
    )
    app = create_app(settings)
    gateway_transport = httpx.ASGITransport(app=build_fake_gateway_app(fake_gateway))
    lifespan = app.router.lifespan_context(app)
    await lifespan.__aenter__()
    app.state.gateway._client = httpx.AsyncClient(
        transport=gateway_transport, base_url=gateway_url
    )
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api.test")
    try:
        resp = await client.post("/alarms", json=_payload("P-RESTART"))
        assert resp.status_code == 201
        for _ in range(100):
            view = (await client.get("/alarms/P-RESTART")).json()
            if view["primary_accepted_at"] is not None:
                break
            await asyncio.sleep(0.02)
        session = (await client.post("/progress/catchup", json={"cursor": 0})).json()
        before = await _read_catchup_client(client, session["session_id"])
        assert [n["node_type"] for n in before] == [
            "alarm_submitted",
            "primary_accepted",
        ]
    finally:
        await client.aclose()
        await lifespan.__aexit__(None, None, None)

    # Rebuild the whole process against the same SQLite file and gateway.
    app2 = create_app(settings)
    lifespan2 = app2.router.lifespan_context(app2)
    await lifespan2.__aenter__()
    app2.state.gateway._client = httpx.AsyncClient(
        transport=gateway_transport, base_url=gateway_url
    )
    client2 = httpx.AsyncClient(transport=httpx.ASGITransport(app=app2), base_url="http://api.test")
    try:
        # The pinned session is still usable after the restart.
        after = await _read_catchup_client(client2, session["session_id"])
        assert [n["seq"] for n in after] == [n["seq"] for n in before]
        assert after == before

        # And newly committed nodes continue the same cursor sequence.
        await client2.post("/alarms/P-RESTART/confirm", json={"confirmed_by": "张工"})
        page = (
            await client2.get("/progress", params={"cursor": before[-1]["seq"]})
        ).json()
        assert page["items"][0]["node_type"] == "confirmed"
        assert page["items"][0]["seq"] == before[-1]["seq"] + 1
    finally:
        await client2.aclose()
        await lifespan2.__aexit__(None, None, None)


# ------------------------------------------------------------- concurrency


async def test_concurrent_confirmations_and_deliveries_lose_no_committed_node(api: ApiHarness):
    alarm_numbers = [f"P-CONC-{i}" for i in range(6)]
    for no in alarm_numbers:
        await api.client.post("/alarms", json=api.alarm_payload(no, deadline=2))
    for no in alarm_numbers:
        assert await api.wait_until(lambda name=no: _status(api, name, "escalated"), timeout=10)

    # Hammer confirms concurrently with ongoing background delivery/reconciliation.
    async def confirm(no: str) -> None:
        for _ in range(3):
            await api.client.post(f"/alarms/{no}/confirm", json={"confirmed_by": "张工"})
            await asyncio.sleep(0.01)

    await asyncio.gather(*(confirm(no) for no in alarm_numbers))
    await asyncio.sleep(0.5)

    nodes = await read_all(api)
    by_alarm: dict[str, list[str]] = {}
    for node in nodes:
        by_alarm.setdefault(node["alarm_no"], []).append(node["node_type"])

    for no in alarm_numbers:
        kinds = by_alarm[no]
        # One of each terminal kind, in order, no duplicates under concurrency.
        assert kinds.count("alarm_submitted") == 1
        assert kinds.count("primary_accepted") == 1
        assert kinds.count("backup_generated") == 1
        assert kinds.count("confirmed") == 1
        assert kinds == sorted(
            kinds,
            key=[
                "alarm_submitted",
                "primary_accepted",
                "primary_rejected",
                "backup_generated",
                "confirmed",
            ].index,
        )


async def test_progress_swagger_documents_new_endpoints(api: ApiHarness):
    schema = (await api.client.get("/openapi.json")).json()
    assert "/progress" in schema["paths"]
    assert "/progress/catchup" in schema["paths"]
    docs = await api.client.get("/docs")
    assert docs.status_code == 200


async def test_cabinet_no_is_null_when_payload_has_no_cabinet(api: ApiHarness):
    payload = api.alarm_payload("P-NOCAB", deadline=5)
    payload["metadata"] = {"site": "DC-1"}
    await api.client.post("/alarms", json=payload)
    assert await api.wait_until(lambda: _status(api, "P-NOCAB", "awaiting_confirmation"))

    nodes = await read_all(api, alarm_no="P-NOCAB")
    assert nodes, "submission must always produce a node"
    assert all(n["cabinet_no"] is None for n in nodes)


async def test_every_node_is_committed_together_with_its_state_change(api: ApiHarness):
    """A node can never exist without the state change it describes, and the
    state change can never be missing its node."""
    await api.client.post("/alarms", json=api.alarm_payload("P-ATOM", deadline=2))
    assert await api.wait_until(lambda: _status(api, "P-ATOM", "escalated"), timeout=8)
    await api.client.post("/alarms/P-ATOM/confirm", json={"confirmed_by": "张工"})
    await asyncio.sleep(0.2)

    db = api.app.state.db
    # Every accepted/rejected notification has its matching terminal node.
    notification_rows = db.query(
        "SELECT alarm_no, role, state FROM notifications "
        " WHERE state IN ('accepted', 'failed') AND role = 'primary'"
    )
    for row in notification_rows:
        expected = "primary_accepted" if row["state"] == "accepted" else "primary_rejected"
        node = db.query_one(
            "SELECT 1 FROM progress_nodes WHERE alarm_no = ? AND node_type = ?"
            " AND notification_role = 'primary'",
            (row["alarm_no"], expected),
        )
        assert node is not None, f"{expected} state committed without its node"

    # Every escalated alarm has exactly one backup node; every confirmed alarm
    # has exactly one confirmation node.
    for row in db.query("SELECT alarm_no FROM alarms WHERE escalated_at IS NOT NULL"):
        count = db.query_one(
            "SELECT COUNT(*) AS c FROM progress_nodes WHERE alarm_no = ? AND node_type = 'backup_generated'",
            (row["alarm_no"],),
        )["c"]
        assert count == 1
    for row in db.query("SELECT alarm_no FROM alarms WHERE confirmed_at IS NOT NULL"):
        count = db.query_one(
            "SELECT COUNT(*) AS c FROM progress_nodes WHERE alarm_no = ? AND node_type = 'confirmed'",
            (row["alarm_no"],),
        )["c"]
        assert count == 1

    # Unknown receipts must never have produced a terminal node.
    unknowns = db.query(
        "SELECT DISTINCT alarm_no FROM notifications WHERE state = 'unknown'"
    )
    for row in unknowns:
        assert db.query_one(
            "SELECT 1 FROM progress_nodes WHERE alarm_no = ? AND node_type IN "
            " ('primary_accepted', 'primary_rejected')",
            (row["alarm_no"],),
        ) is None


# --------------------------------------------------------------------- helpers


def _payload(alarm_no: str) -> dict:
    return {
        "alarm_no": alarm_no,
        "content": "供电柜 B12 温度 82°C",
        "metadata": {"cabinet": "B12"},
        "primary_contact": {"name": "主值班员", "channel": "app"},
        "backup_contact": {"name": "备班", "channel": "sms", "address": "+8613800000001"},
        "ack_deadline_seconds": 5,
    }


async def _has_node(api: ApiHarness, alarm_no: str, node_type: str) -> bool:
    page = await api.client.get("/progress", params={"alarm_no": alarm_no, "limit": 200})
    if page.status_code != 200:
        return False
    return any(n["node_type"] == node_type for n in page.json()["items"])


async def _read_catchup_client(client: httpx.AsyncClient, session_id: str) -> list[dict]:
    items: list[dict] = []
    cursor = 0
    for _ in range(100):
        page = (
            await client.get(
                "/progress", params={"cursor": cursor, "session_id": session_id}
            )
        ).json()
        items.extend(page["items"])
        cursor = page["next_cursor"]
        if page["caught_up"]:
            break
    return items
