from datetime import datetime, timedelta, timezone

import pandas as pd

from gnome_pipelines.cfb.qb_status.autoapply import NEWS_CREDIBILITY, credibility_for, decide
from gnome_pipelines.cfb.qb_status.evidence import Evidence, evidence_frame
from gnome_pipelines.cfb.qb_status.x_scan import UNLISTED_CREDIBILITY, Registry, registry_frame

T0 = datetime(2026, 10, 22, 18, 0, tzinfo=timezone.utc)
CRED = {"BeatOne": 1.0, "BeatTwo": 1.0, "SiteLow": 0.5}
MODEL = "claude-haiku-5-5"


def cred(source_type, org):
    return CRED.get(org, 0.0) if source_type == "x_post" else NEWS_CREDIBILITY if source_type == "news" else 0.0


def claim(org, status="out", hedge="confirmed", conf=0.9, extractor=MODEL, source_type="x_post", seen=T0,
          player="9000001", game=77, n=None):
    return Evidence(evidence_id=f"{org}-{status}-{n or seen.isoformat()}", source_type=source_type,
                    source_id=f"{org}-{n}", source_org=org, first_seen_at=seen, team="Testville State", status=status,
                    season=2026, team_id=501, player_id=player, game_id=game, hedge=hedge, extractor=extractor,
                    extraction_confidence=conf, review_state="pending")


def official(status, seen, slot="Initial"):
    return Evidence(evidence_id=f"off-{status}-{seen.isoformat()}", source_type="official_report", source_id="cell",
                    source_org="conference", first_seen_at=seen, team="Testville State", status=status, season=2026,
                    team_id=501, player_id="9000001", game_id=77, report_slot=slot, review_state="auto")


def states(new, existing=()):
    out = decide(new, evidence_frame(list(existing)), cred, T0)
    return [(e.review_state, e.reviewer) for e in out]


def test_a_plain_claim_read_by_the_model_from_a_credible_reporter_applies():
    assert states([claim("BeatOne")]) == [("auto", "auto:credible")]
    assert states([claim("BeatOne", hedge="per_sources")]) == [("auto", "auto:credible")]


def test_hedged_rule_read_or_less_credible_claims_wait():
    assert states([claim("BeatOne", hedge="expected")]) == [("pending", None)]
    assert states([claim("BeatOne", conf=0.6, extractor="rules", hedge="unclear")]) == [("pending", None)]
    assert states([claim("BeatOne", conf=0.7)]) == [("pending", None)]
    assert states([claim("SiteLow")]) == [("pending", None)]
    assert states([claim("Some Paper", source_type="news")]) == [("pending", None)]


def test_a_claim_matching_the_latest_official_report_applies():
    rule = claim("SiteLow", conf=0.6, extractor="rules", hedge="unclear")
    assert states([rule], [official("out", T0 - timedelta(hours=3))]) == [("auto", "auto:official")]
    later = [official("out", T0 - timedelta(hours=9)), official("questionable", T0 - timedelta(hours=3), "Update 1")]
    assert states([claim("SiteLow", conf=0.6, extractor="rules", hedge="unclear")], later) == [("pending", None)]


def test_a_second_independent_credible_source_applies_the_claim():
    first = claim("BeatOne", conf=0.6, extractor="rules", hedge="unclear", seen=T0 - timedelta(hours=5))
    second = claim("BeatTwo", conf=0.6, extractor="rules", hedge="unclear")
    assert states([second], [first]) == [("auto", "auto:agree")]


def test_the_same_account_twice_or_an_old_claim_is_not_agreement():
    again = claim("BeatOne", conf=0.6, extractor="rules", hedge="unclear", n=2)
    first = claim("BeatOne", conf=0.6, extractor="rules", hedge="unclear", seen=T0 - timedelta(hours=5), n=1)
    assert states([again], [first]) == [("pending", None)]
    stale = claim("BeatOne", conf=0.6, extractor="rules", hedge="unclear", seen=T0 - timedelta(hours=60))
    assert states([claim("BeatTwo", conf=0.6, extractor="rules", hedge="unclear")], [stale]) == [("pending", None)]


def test_agreement_needs_the_same_player_status_and_game():
    first = claim("BeatOne", conf=0.6, extractor="rules", hedge="unclear", seen=T0 - timedelta(hours=1))
    assert states([claim("BeatTwo", status="doubtful", conf=0.6, extractor="rules", hedge="unclear")],
                  [first]) == [("pending", None)]
    assert states([claim("BeatTwo", game=78, conf=0.6, extractor="rules", hedge="unclear")], [first]) == [
        ("pending", None)]


def test_agreement_inside_one_batch_applies_the_later_claim():
    a = claim("BeatOne", conf=0.6, extractor="rules", hedge="unclear", seen=T0 - timedelta(minutes=2))
    b = claim("BeatTwo", conf=0.6, extractor="rules", hedge="unclear")
    assert states([a, b]) == [("pending", None), ("auto", "auto:agree")]


def test_less_credible_sources_need_more_of_them():
    news = [claim(f"Paper {i}", source_type="news", conf=0.6, extractor="rules", hedge="unclear", n=i,
                  seen=T0 - timedelta(hours=i)) for i in (1, 2)]
    assert states([claim("SiteLow", conf=0.6, extractor="rules", hedge="unclear")], news) == [("pending", None)]
    assert states([claim("BeatOne", conf=0.6, extractor="rules", hedge="unclear")], news) == [("auto", "auto:agree")]


def test_speculation_read_by_the_model_never_counts():
    first = claim("BeatOne", hedge="unclear", seen=T0 - timedelta(hours=1))
    assert states([claim("BeatTwo", hedge="unclear")], [first]) == [("pending", None)]


def test_x_credibility_comes_from_the_registry_and_unlisted_accounts_count_little():
    rows = pd.DataFrame([{"platform": "x", "handle": "BeatOne", "teams": "Testville State", "kind": "beat",
                          "confidence": "normal"},
                         {"platform": "x", "handle": "SiteLow", "teams": "Testville State", "kind": "outlet",
                          "confidence": "low"}])
    c = credibility_for(Registry(registry_frame(rows, vetted_by="user", now=T0)))
    assert (c("x_post", "BeatOne"), c("x_post", "SiteLow"), c("x_post", "RandomFan")) == (1.0, 0.5,
                                                                                         UNLISTED_CREDIBILITY)
    assert c("news", "Some Paper") == NEWS_CREDIBILITY and c("market_alarm", "kalshi") == 0.0


def test_two_outlets_saying_the_incumbent_starts_apply():
    incumbent = {501: "9000001"}.get
    new = [claim("On3", status="expected_starter", hedge="expected", source_type="news", n=1),
           claim("247Sports", status="expected_starter", hedge="per_sources", source_type="news", n=2)]
    out = decide(new, evidence_frame([]), cred, T0, incumbent=incumbent)
    assert [(e.review_state, e.reviewer) for e in out] == [("pending", None), ("auto", "auto:incumbent")]


def test_two_outlets_naming_a_different_starter_still_wait():
    incumbent = {501: "9000002"}.get
    new = [claim("On3", status="expected_starter", hedge="expected", source_type="news", n=1),
           claim("247Sports", status="expected_starter", hedge="per_sources", source_type="news", n=2)]
    out = decide(new, evidence_frame([]), cred, T0, incumbent=incumbent)
    assert [e.review_state for e in out] == ["pending", "pending"]


def test_one_outlet_on_the_incumbent_is_not_enough():
    out = decide([claim("On3", status="expected_starter", hedge="expected", source_type="news")], evidence_frame([]),
                 cred, T0, incumbent={501: "9000001"}.get)
    assert out[0].review_state == "pending"


def test_when_the_starter_is_up_in_the_air_two_outlets_on_any_candidate_apply():
    new = [claim("Mercury News", status="expected_starter", hedge="expected", source_type="news", n=1),
           claim("RotoWire", status="expected_starter", hedge="expected", source_type="news", n=2)]
    out = decide(new, evidence_frame([]), cred, T0, incumbent={501: "9000002"}.get, unsettled=lambda tid: tid == 501)
    assert [(e.review_state, e.reviewer) for e in out] == [("pending", None), ("auto", "auto:unsettled")]
