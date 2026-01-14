import unittest

from brak_scrape import (
    _extract_datagrid_id,
    _extract_total_results,
    _extract_updateDataResult_source,
    _extract_viewstate_any,
    _is_jsf_partial,
    _parse_cards,
    _parse_partial_response,
)


class BrakScrapeTests(unittest.TestCase):
    def test_parse_partial_response(self) -> None:
        xml = """
        <partial-response>
          <changes>
            <update id="foo">bar</update>
            <update id="javax.faces.ViewState">state123</update>
          </changes>
        </partial-response>
        """
        updates, viewstate = _parse_partial_response(xml)
        self.assertTrue(_is_jsf_partial(xml))
        self.assertEqual(updates["foo"], "bar")
        self.assertEqual(viewstate, "state123")

    def test_extract_viewstate_from_html(self) -> None:
        html = '<input type="hidden" name="jakarta.faces.ViewState" value="abc123" />'
        self.assertEqual(_extract_viewstate_any(html), "abc123")

    def test_extract_total_results(self) -> None:
        html = "<div>Anzahl der Treffer: 1.234</div>"
        self.assertEqual(_extract_total_results(html), 1234)

    def test_extract_datagrid_and_update_source(self) -> None:
        html = (
            'PrimeFaces.cw("DataGrid","widget",{id:"resultForm:dataGrid"});'
            'updateDataResult = function(){return PrimeFaces.ab({s:"resultForm:j_idt271"});}'
        )
        self.assertEqual(_extract_datagrid_id(html), "resultForm:dataGrid")
        self.assertEqual(_extract_updateDataResult_source(html), "resultForm:j_idt271")

    def test_parse_cards(self) -> None:
        html = """
        <div class="resultCard">
          <div class="resultCardHeader">Dr. Max Mustermann</div>
          <ul>
            <li>Rechtsanwalt</li>
            <li>Kanzlei Muster</li>
            <li>Hauptstr. 1</li>
            <li>10115 Berlin</li>
          </ul>
        </div>
        """
        cards = _parse_cards(html, bar_label="Berlin")
        self.assertEqual(len(cards), 1)
        card = cards[0]
        self.assertEqual(card["bar"], "Berlin")
        self.assertEqual(card["name"], "Dr. Max Mustermann")
        self.assertEqual(card["professional_title"], "Rechtsanwalt")
        self.assertEqual(card["office"], "Kanzlei Muster")
        self.assertEqual(card["street"], "Hauptstr. 1")
        self.assertEqual(card["zip"], "10115")
        self.assertEqual(card["city"], "Berlin")


if __name__ == "__main__":
    unittest.main()
