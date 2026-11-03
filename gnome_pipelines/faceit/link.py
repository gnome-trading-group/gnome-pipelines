"""
FACEIT series <-> HLTV series, one to one, and the FACEIT rows the e12 model must drop.

Many organised series (ESEA, CCT qualifiers) are on both sites; counting both would count one result twice. Candidates
come from two routes, since a side's quarterly HLTV team misses roster moves and loans:
  pair     both sides map to HLTV teams and the same pair met on HLTV within 2 days
  players  at least 3 linked players on each FACEIT side are in the matching HLTV lineup within 2 days
Each candidate is scored on agreement of the maps played (name and score, either orientation), lineup overlap and the
gap between start times; pairs are then taken greedily, best first. quality: exact = every map name and score agrees;
maps = names agree, a score differs; partial = some maps agree and the starts are within 6 hours (FACEIT keeps
restarted maps HLTV drops). Anything weaker is not kept: a same-pair series a day or two apart with no agreeing map is
as likely to be a different series.
"""
from __future__ import annotations

from collections import defaultdict

import numpy as np
import pandas as pd

WINDOW = pd.Timedelta(days=2)
LINK_COLUMNS = ["faceit_match_id", "hltv_match_id", "swapped", "quality", "maps_same_score", "lineup_overlap", "hours_apart"]


def _maps(df: pd.DataFrame) -> dict:
    return {mid: [(m, a, b) for m, a, b in zip(g.map_name, g.team_a_score, g.team_b_score)] for mid, g in df.groupby("match_id")}


def series_links(fm: pd.DataFrame, fh: pd.DataFrame, hltv: pd.DataFrame, taken_hltv: set | None = None) -> pd.DataFrame:
    """fm: faceit_matches rows; fh: their faceit_match_history rows; hltv: cs2_match_history rows around the same days.
    HLTV series in taken_hltv are already linked to another FACEIT series and are not offered again."""
    taken_hltv = taken_hltv or set()
    fm = fm.assign(t=pd.to_datetime(fm.started_at.fillna(fm.finished_at), unit="s"))
    h = hltv[~hltv.match_id.isin(taken_hltv)].copy()
    h["t"] = pd.to_datetime(h.match_time, utc=True).dt.tz_convert(None)
    f_maps, h_maps = _maps(fh), _maps(h)
    hs = h.drop_duplicates("match_id").set_index("match_id")
    f_side = {mid: tuple({int(x) for ids in g[c] for x in ids if x is not None and not pd.isna(x)}
                         for c in ("team_a_player_ids", "team_b_player_ids")) for mid, g in fh.groupby("match_id")}
    h_side = {mid: ({int(x) for x in r.team_a_lineup_ids}, {int(x) for x in r.team_b_lineup_ids}) for mid, r in hs.iterrows()}

    cand = set()
    by_pair = defaultdict(list)
    for mid, r in hs.iterrows():
        by_pair[frozenset((r.team_a_id, r.team_b_id))].append((r.t, mid))
    for r in fm.dropna(subset=["faction1_hltv_team", "faction2_hltv_team"]).itertuples():
        for t, hid in by_pair.get(frozenset((int(r.faction1_hltv_team), int(r.faction2_hltv_team))), []):
            if abs(t - r.t) <= WINDOW:
                cand.add((r.match_id, hid))
    by_player = defaultdict(list)
    for hid, (a, b) in h_side.items():
        for p in a | b:
            by_player[p].append(hid)
    ft = fm.set_index("match_id").t
    for mid, (a, b) in f_side.items():
        if len(a) < 3 or len(b) < 3 or mid not in ft.index:
            continue
        for hid in {hid for p in a | b for hid in by_player.get(p, ())}:
            if abs(hs.t[hid] - ft[mid]) > WINDOW:
                continue
            ha, hb = h_side[hid]
            if (len(a & ha) >= 3 and len(b & hb) >= 3) or (len(a & hb) >= 3 and len(b & ha) >= 3):
                cand.add((mid, hid))
    if not cand:
        return pd.DataFrame(columns=LINK_COLUMNS)

    def score(mid, hid) -> dict:
        fmaps, hmaps = f_maps.get(mid, []), h_maps.get(hid, [])
        best = None
        for swap in (False, True):
            hm = [(m, b, a) if swap else (m, a, b) for m, a, b in hmaps]
            names = sorted(m for m, _, _ in fmaps) == sorted(m for m, _, _ in hm)
            exact = names and sorted(fmaps) == sorted(hm)
            same = len(set(fmaps) & set(hm))
            fa, fb = f_side.get(mid, (set(), set()))
            ha, hb = h_side[hid]
            overlap = len(fa & (hb if swap else ha)) + len(fb & (ha if swap else hb))
            key = (exact, names, same, overlap)
            if best is None or key > best[0]:
                best = (key, swap)
        (exact, names, same, overlap), swap = best
        return {"faceit_match_id": mid, "hltv_match_id": hid, "swapped": swap, "exact": exact, "map_names_agree": names,
                "maps_same_score": same, "lineup_overlap": overlap, "hours_apart": abs((hs.t[hid] - ft[mid]).total_seconds()) / 3600}

    c = pd.DataFrame([score(m, hid) for m, hid in cand]).sort_values(
        ["exact", "map_names_agree", "maps_same_score", "lineup_overlap", "hours_apart"], ascending=[False, False, False, False, True])
    taken_f, taken_h, keep = set(), set(), []
    for r in c.itertuples():
        if r.faceit_match_id in taken_f or r.hltv_match_id in taken_h:
            continue
        taken_f.add(r.faceit_match_id)
        taken_h.add(r.hltv_match_id)
        keep.append(r.Index)
    links = c.loc[keep]
    links = links.assign(quality=np.where(links.exact, "exact", np.where(links.map_names_agree, "maps", np.where(
        (links.maps_same_score > 0) & (links.hours_apart <= 6), "partial", "weak"))))
    return links[links.quality != "weak"][LINK_COLUMNS].reset_index(drop=True)


def exclusions(fm: pd.DataFrame, fh: pd.DataFrame, linked: set) -> pd.DataFrame:
    """The FACEIT rows the model drops, map_name "" for a whole series: implausible map scores, a winner that
    disagrees with the score, the wrong number of maps for the best-of, series without stats (forfeits), and series
    that duplicate a linked HLTV series."""
    hi = fh[["team_a_score", "team_b_score"]].max(axis=1)
    lo = fh[["team_a_score", "team_b_score"]].min(axis=1)
    regulation = (hi == 13) & (lo <= 11)
    overtime = (hi > 13) & (hi - lo).between(1, 4) & (lo >= 12)
    parts = [fh[~(regulation | overtime)][["match_id", "map_name"]].assign(reason="implausible map score"),
             fh[(fh.team_a_score > fh.team_b_score) != (fh.team_a_won == 1)][["match_id", "map_name"]].assign(
                 reason="winner disagrees with score")]
    g = fh.groupby("match_id")
    series = pd.DataFrame({"bo": g.bo_type.first(), "maps": g.size(), "a": g.team_a_won.sum()})
    series["w"] = np.maximum(series.a, series.maps - series.a)
    parts.append(pd.DataFrame({"match_id": series.index[series.w != series.bo // 2 + 1], "map_name": "",
                               "reason": "wrong map count for best-of"}))
    parts.append(pd.DataFrame({"match_id": fm.match_id[~fm.has_stats.astype(bool)], "map_name": "", "reason": "no stats (forfeit)"}))
    parts.append(pd.DataFrame({"match_id": sorted(set(fm.match_id) & linked), "map_name": "", "reason": "duplicate of an HLTV series"}))
    out = pd.concat(parts, ignore_index=True)
    out["map_name"] = out.map_name.fillna("")
    return out.drop_duplicates().reset_index(drop=True)
