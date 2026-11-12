import gzip
from datetime import datetime, timezone

from gnome_pipelines.rt.films import (MISSES_TO_CLOSE, FilmState, changed, kalshi, merge, polymarket,
                                      rt_slug_from_search)
from gnome_pipelines.rt.match import child_sitemaps, decode, entries, film_id, is_review_of
from gnome_pipelines.rt.outlets import article_sitemaps, recent, url_date
from gnome_pipelines.rt.page import facts
from gnome_pipelines.rt.tomatometer import counts

T0 = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)


def test_review_urls_match_their_film_and_nothing_else():
    assert is_review_of("https://variety.com/2026/film/reviews/how-to-rob-a-bank-review-1236/", "How to Rob a Bank",
                        r"/film/reviews/")
    assert not is_review_of("https://variety.com/2026/film/news/how-to-rob-a-bank-box-office-1236/", "How to Rob a Bank")
    assert not is_review_of("https://example.com/how-to-rob-a-bank-season-2-review/", "How to Rob a Bank")
    # A one-word title has to sit next to "review" or start the slug; what that lets through ("michael-b-jordan-...")
    # Claude's is_review rejects, as in the research run.
    assert is_review_of("https://www.ign.com/articles/michael-review", "Michael")
    assert not is_review_of("https://www.ign.com/articles/sinners-michael-b-jordan-review", "Michael")
    assert film_id('"How to Rob a Bank"') == film_id("How to Rob a Bank") == "how-to-rob-a-bank"


def test_sitemaps_rss_and_gzipped_sitemaps_list_their_urls_and_times():
    sitemap = """<urlset><url><loc>https://a.com/x-review/</loc><news:news><news:publication_date>2026-11-11T11:58:00Z
    </news:publication_date></news:news></url><url><loc>https://a.com/y</loc><lastmod>2026-11-10</lastmod></url></urlset>"""
    rss = """<rss><channel><item><title>t</title><link>https://b.com/z-review/</link>
    <pubDate>Wed, 11 Nov 2026 11:59:00 GMT</pubDate></item></channel></rss>"""
    got = entries(decode(gzip.compress(sitemap.encode()))) + entries(rss)
    assert [u for u, _ in got] == ["https://a.com/x-review/", "https://a.com/y", "https://b.com/z-review/"]
    assert got[0][1] == datetime(2026, 11, 11, 11, 58, tzinfo=timezone.utc)
    assert got[2][1] == datetime(2026, 11, 11, 11, 59, tzinfo=timezone.utc)


def test_an_index_s_recent_children_are_its_current_period():
    index = """<sitemapindex><sitemap><loc>https://a.com/sitemap-2026-11.xml</loc></sitemap>
    <sitemap><loc>https://a.com/sitemap-2026-09.xml</loc></sitemap>
    <sitemap><loc>https://a.com/posts-recent.xml</loc><lastmod>2026-11-11T08:00:00Z</lastmod></sitemap>
    <sitemap><loc>https://a.com/posts-old.xml</loc><lastmod>2025-01-01</lastmod></sitemap></sitemapindex>"""
    assert recent(child_sitemaps(index), T0) == ["https://a.com/sitemap-2026-11.xml", "https://a.com/posts-recent.xml"]
    assert url_date("https://www.thewrap.com/sitemap.xml?yyyy=2026&mm=11&dd=11") == (datetime(2026, 11, 11), False)


def test_an_index_s_tag_author_video_and_database_sitemaps_are_left_out():
    children = ["https://nerdist.com/article-sitemap32.xml", "https://nerdist.com/post_tag-sitemap11.xml",
                "https://nerdist.com/video-sitemap-3.xml", "https://nerdist.com/category-sitemap.xml",
                "https://screenrant.com/sitemap-2026-11-part1-articles.xml", "https://screenrant.com/sitemap-db-movie-part9.xml",
                "https://screenrant.com/sitemap-authors-part1.xml", "https://mashable.com/sitemap-entertainment-0.xml",
                "https://mashable.com/sitemap-video-files-31.xml", "https://www.denofgeek.com/page-sitemap.xml",
                "https://www.austinchronicle.com/newspack_collection_section-sitemap.xml",
                "https://www.thewrap.com/sitemap.xml?yyyy=2026&mm=11&dd=11"]
    assert [u for u, _ in article_sitemaps([(u, None) for u in children])] == [
        "https://nerdist.com/article-sitemap32.xml", "https://screenrant.com/sitemap-2026-11-part1-articles.xml",
        "https://mashable.com/sitemap-entertainment-0.xml", "https://www.thewrap.com/sitemap.xml?yyyy=2026&mm=11&dd=11"]


def test_films_from_all_three_venues_merge_by_title():
    k = kalshi([{"event_ticker": "KXRT-HOWT", "sub_title": "How to Rob a Bank", "markets": [
        {"floor_strike": 55, "close_time": "2026-11-16T15:00:00Z"}, {"floor_strike": 45, "close_time": "2026-11-16T15:00:00Z"}]}])
    pm = polymarket([{"slug": "how-to-rob-a-bank-rt", "title": '"How to Rob a Bank" Rotten Tomatoes Score?',
                      "endDate": "2026-11-16T15:00:00Z", "active": True, "closed": False},
                     {"slug": "doomsday-vs-dune", "title": "Will Avengers: Doomsday have a higher RT score than Dune?"}],
                    "polymarket")
    us = polymarket([{"slug": "t-howt", "title": "How to Rob a Bank Rotten Tomatoes Score?", "endDate": "2026-11-17T04:59:00Z"},
                     {"slug": "t-ebe", "title": "Ebenezer Rotten Tomatoes Score?", "endDate": "2026-11-17T04:59:00Z"}],
                    "polymarket_us")
    films = merge(k, pm, us)
    howt = films["how-to-rob-a-bank"]
    assert (howt.kalshi_event, howt.kalshi_strikes, howt.polymarket_event, howt.polymarket_us_event) == (
        "KXRT-HOWT", (45, 55), "how-to-rob-a-bank-rt", "t-howt")
    assert howt.close == datetime(2026, 11, 16, 15, tzinfo=timezone.utc) and set(films) == {"how-to-rob-a-bank", "ebenezer"}


def test_film_states_are_recorded_when_they_change_and_once_when_they_close():
    a, misses = FilmState("a", "A", T0, kalshi_event="KXRT-A"), {}
    known, updates = changed({}, {"a": a}, lambda f: "a_2026", misses)
    assert [u.rt_slug for u in updates] == ["a_2026"]
    known, updates = changed(known, {"a": a}, lambda f: "never asked again", misses)
    assert updates == [] and known["a"].rt_slug == "a_2026"
    for _ in range(MISSES_TO_CLOSE - 1):
        known, updates = changed(known, {}, lambda f: None, misses)
        assert updates == [] and known["a"].open, "one search leaving a film out doesn't close it"
    known, updates = changed(known, {}, lambda f: None, misses)
    assert [(u.film_id, u.open) for u in updates] == [("a", False)]
    assert changed(known, {}, lambda f: None, misses)[1] == []


def test_a_film_missing_from_one_pass_and_back_the_next_starts_its_count_again():
    a, misses = FilmState("a", "A", T0, kalshi_event="KXRT-A"), {}
    known, _ = changed({}, {"a": a}, lambda f: "a_2026", misses)
    for listed in ({}, {"a": a}, {}, {"a": a}, {}):
        known, updates = changed(known, listed, lambda f: None, misses)
        assert updates == [] and known["a"].open


def test_the_rt_slug_is_the_exact_title_released_nearest_the_close():
    page = """<search-page-media-row release-year="2026"><a href="https://www.rottentomatoes.com/m/how_to_rob_a_bank_2026">
    </a><a slot="title">How to Rob a Bank</a></search-page-media-row>
    <search-page-media-row release-year="2007"><a href="https://www.rottentomatoes.com/m/how_to_rob_a_bank"></a>
    <a slot="title">How to Rob a Bank</a></search-page-media-row>
    <search-page-media-row release-year="2026"><a href="https://www.rottentomatoes.com/m/how_to_fix"></a>
    <a slot="title">How to Fix a Drug Scandal</a></search-page-media-row>"""
    assert rt_slug_from_search(page, "How to Rob a Bank", T0) == "how_to_rob_a_bank_2026"
    assert rt_slug_from_search(page, "How to Rob a Bank", datetime(2015, 1, 1, tzinfo=timezone.utc)) is None


def test_review_pages_give_their_time_headline_and_rating():
    page = ('<meta property="article:published_time" content="2026-11-11T13:00:00Z"/>'
            '<meta property="og:title" content="&#039;How to Rob a Bank&#039; Review"/>'
            '<script>{"reviewRating":{"@type":"Rating","ratingValue":"7","bestRating":"10"}}</script>'
            "<p>A <b>breezy</b> heist.</p><p>Fun.</p>")
    f = facts(page, "ign")
    assert f.published_at == datetime(2026, 11, 11, 13, tzinfo=timezone.utc)
    assert f.headline == "'How to Rob a Bank' Review" and f.text == "A breezy heist. Fun."
    assert (f.rating_kind, f.rating_value) == ("score", 0.7)
    assert facts("<p>Grade: B+</p>", "indiewire").rating_grade == "B+"


def test_the_tomatometer_counts_come_from_the_page_json():
    page = ('"criticsScore":{"averageRating":"4.30","likedCount":45,"notLikedCount":140,"scorePercent":"24%"},'
            '"audienceScore":{"likedCount":9}')
    assert counts(page) == counts(page) and (counts(page).liked, counts(page).not_liked, counts(page).score) == (45, 140, 24.0)
    assert counts("<html>Please verify you are a human</html>") is None
