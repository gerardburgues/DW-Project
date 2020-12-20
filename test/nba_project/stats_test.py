import unittest
import nba_api.stats.endpoints.commonplayerinfo as common_player_info_api


class TestStats(unittest.TestCase):

    def test_is_le_bron(self):
        le_bron_player_id = 2544
        data = common_player_info_api.CommonPlayerInfo(player_id=le_bron_player_id)
        name = data.get_normalized_dict()['CommonPlayerInfo'][0]['DISPLAY_FIRST_LAST']
        self.assertEqual(name, 'LeBron James')
