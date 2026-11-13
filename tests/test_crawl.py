"""crawl.py 오프라인 테스트 (네트워크 없이 실행)

실행: python -m unittest discover -s tests
"""
import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scraper"))
import crawl  # noqa: E402


class ParseDateTest(unittest.TestCase):
    def test_rfc2822(self):
        self.assertEqual(crawl.parse_date("Sun, 11 Oct 2026 16:54:48 GMT"),
                         int(datetime(2026, 10, 11, 16, 54, 48, tzinfo=crawl.timezone.utc).timestamp()))

    def test_date_only_is_kst_midnight(self):
        expected = int(datetime(2026, 10, 14, tzinfo=crawl.KST).timestamp())
        for s in ("2026-10-14", "2026.10.14", "2026/10/14", "26.10.14"):
            self.assertEqual(crawl.parse_date(s), expected, s)

    def test_invalid(self):
        self.assertIsNone(crawl.parse_date(""))
        self.assertIsNone(crawl.parse_date("2026-13-40"))
        self.assertIsNone(crawl.parse_date("어제"))


class ClassifyTest(unittest.TestCase):
    def test_short_keyword_word_boundary(self):
        # 'ost' 가 'ghost' 안에서 걸리면 안 됨
        self.assertNotEqual(crawl.classify_category("Digimon Ghost Game episode")[0], "sns")

    def test_set_code(self):
        self.assertEqual(crawl.classify_category("BT18 박스 개봉")[0], "card")
        self.assertNotEqual(crawl.classify_category("next week")[0], "card")

    def test_new_project_trigger(self):
        self.assertEqual(crawl.classify_category("디지몬 신작 애니 제작 결정"), ("game", "신작/공식속보"))


class SpamTest(unittest.TestCase):
    def test_gambling(self):
        self.assertTrue(crawl.is_spam({"title": "디지몬 카지노 이벤트", "link": "https://x.test/"}))

    def test_wikimon_betamon_is_not_spam(self):
        self.assertFalse(crawl.is_spam({"title": "[위키몬 신규] Betamon", "source": "Wikimon",
                                        "link": "https://wikimon.net/Betamon", "source_badge": "위키몬"}))

    def test_untrusted_news_source(self):
        self.assertTrue(crawl.is_spam({"title": "디지몬", "source": "아무블로그", "source_badge": "뉴스",
                                       "link": "https://news.google.com/x"}))


class MakeItemTest(unittest.TestCase):
    def test_shape(self):
        item = crawl.make_item(title="t", link="https://a.test/1", source="s", badge="b",
                               category=("card", "카드게임(TCG)"), raw_date="2026-10-14")
        self.assertEqual(list(item), ["id", "title", "snippet", "source", "source_badge", "category",
                                      "category_name", "link", "image", "timestamp", "created_at"])
        self.assertEqual(item["id"], crawl.get_hash_id("https://a.test/1"))
        self.assertEqual(item["created_at"], "2026-10-14 00:00")

    def test_id_key_and_unknown_date(self):
        item = crawl.make_item(title="t", link="https://a.test/", id_key="k", source="s", badge="b",
                               category=("general", "일반/뉴스"))
        self.assertEqual(item["id"], crawl.get_hash_id("k"))
        self.assertTrue(item["keep_first_seen"])


class SourcesTest(unittest.TestCase):
    def test_registry(self):
        names = [n for n, _ in crawl.SOURCES]
        self.assertEqual(len(names), len(set(names)), "소스 이름이 겹침")
        for name, fn in crawl.SOURCES:
            self.assertTrue(callable(fn), name)

    def test_crawl_all_isolates_failures_and_keeps_order(self):
        def ok():
            return [{"id": "1"}, {"id": "2"}]

        def logs_error():
            crawl.log_error("HTTP 403")
            return []

        def crashes():
            raise RuntimeError("boom")

        items, status = crawl.crawl_all([("ok", ok), ("err", logs_error), ("crash", crashes), ("ok2", ok)], workers=3)
        self.assertEqual([x["id"] for x in items], ["1", "2", "1", "2"])
        by = {s["name"]: s for s in status}
        self.assertEqual([s["name"] for s in status], ["ok", "err", "crash", "ok2"])
        self.assertEqual((by["ok"]["count"], by["ok"]["errors"]), (2, 0))
        self.assertEqual((by["err"]["count"], by["err"]["errors"]), (0, 1))
        self.assertIn("403", by["err"]["last_error"])
        self.assertEqual(by["crash"]["errors"], 1)


class MergeTest(unittest.TestCase):
    NOW = int(datetime(2026, 11, 13, tzinfo=crawl.KST).timestamp())

    def _item(self, id_, ts, **kw):
        base = {"id": id_, "title": "디지몬 소식", "source": "digimon.net", "source_badge": "디지몬웹",
                "link": f"https://digimon.net/{id_}", "category": "game", "category_name": "게임/공식애니",
                "timestamp": ts, "created_at": crawl.format_display_date(ts)}
        base.update(kw)
        return base

    def test_keep_first_seen(self):
        old = self._item("a", self.NOW - 86400)
        new = self._item("a", self.NOW, keep_first_seen=True)
        out = crawl.merge_feeds([old], [new], now_ts=self.NOW)
        self.assertEqual(out[0]["timestamp"], self.NOW - 86400)
        self.assertNotIn("keep_first_seen", out[0])

    def test_retention(self):
        old = self._item("old", self.NOW - (crawl.RETENTION_DAYS + 1) * 86400)
        fresh = self._item("fresh", self.NOW - 3600)
        out = crawl.merge_feeds([old, fresh], [], now_ts=self.NOW)
        self.assertEqual([x["id"] for x in out], ["fresh"])


if __name__ == "__main__":
    unittest.main()
