from typing import Dict

class TikTokAPIError(Exception):
  response: Dict[str, any]

  def __init__(self, response: Dict[str, any]):
    self.response = response
    super().__init__(f"TikTok API Error: {response['code']} | {response['message']}")

class TikTokPaginationError(Exception):
  def __init__(self, response: Dict[str, any], message: str):
    self.response = response
    page_info = None
    if 'page_info' in response:
      page_info = response['page_info']
    elif 'data' in response and 'page_info' in response['data']:
      page_info = response['data']['page_info']
    super().__init__(f'TikTok Pagination Error: {message} (received {page_info})')

class TikTokUsageError(Exception):
  pass

class TikTokMissingAdvertiserError(TikTokUsageError):
  def __init__(self):
    super().__init__(f"TikTok Usage Error: The advertiser_id has not been set.")
