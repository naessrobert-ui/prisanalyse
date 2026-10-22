import unittest
from newsweb_monitor.report_analysis import outlook_excerpt

class OutlookTests(unittest.TestCase):
    def test_outlook_section_retained_with_page(self):
        text="[SIDE 1]\nHistorical EBIT 400\n\n[SIDE 2]\nOutlook\nWe expect revenue growth of 8% next year.\nMargins should improve.\n"
        result=outlook_excerpt(text)
        self.assertIn("Outlook",result)
        self.assertIn("Margins should improve",result)
    def test_norwegian_guidance(self):
        self.assertIn("Forventer",outlook_excerpt("[SIDE 4]\nForventer bedre marginer i 2027"))
    def test_no_guidance_returns_empty(self):
        self.assertEqual(outlook_excerpt("[SIDE 1]\nIncome statement\nRevenue 123"),"")
