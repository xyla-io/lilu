import json
import requests

from .context import EntityGranularity
from .error import TikTokAPIError, TikTokPaginationError
from typing import Optional, Dict, List, Callable
from furl import furl

def handle_response_error_and_page(f: Callable[..., Dict[str, any]]) -> Callable[..., Dict[str, any]]:
  def wrapper(*args, params: Dict[str, any], **kwargs):
    response = f(
      *args,
      params=params,
      **kwargs
    )
    if response['code'] != 0:
      raise TikTokAPIError(response=response)
    page_info = response['page_info'] if 'page_info' in response else response['data']['page_info'] if 'data' in response and 'page_info' in response['data'] else None
    if page_info is not None and page_info['page'] < page_info['total_page']:
      if 'data' not in response or 'list' not in response['data']:
        raise TikTokPaginationError(
          response=response,
          message='pagination is not supported for responses with no data.list property'
        )
      next_page_response = wrapper(
        *args,
        params={
          **params,
          'page': page_info['page'] + 1
        },
        **kwargs,
      )
      response['data']['list'].extend(next_page_response['data']['list'])
      if page_info['page'] == 1:
        assert len(response['data']['list']) == page_info['total_number']
      del page_info['page']
    return response
  return wrapper

class TikTokAPI:
  access_token: str
  client_secret: str
  app_id: str
  advertiser_id: Optional[str]
  sandbox_environment: bool=False

  def __init__(self, access_token: str, client_secret: str, app_id: str, advertiser_id: Optional[str]=None):
    self.access_token = access_token
    self.client_secret = client_secret
    self.app_id = app_id
    self.advertiser_id = advertiser_id
  
  @property
  def api_base_url(self) -> str:
    return 'https://sandbox-ads.tiktok.com/open_api' if self.sandbox_environment else 'https://ads.tiktok.com/open_api'
  
  @handle_response_error_and_page
  def get(self, endpoint: str, params: Dict[str, any]) -> any:
    url = f'{self.api_base_url}/{endpoint}'
    headers = {'Access-Token': self.access_token}
    query_params = {
      k: json.dumps(v) if isinstance(v, list) or isinstance(v, dict) else v
      for k, v in params.items()
    }
    response = requests.get(url, params=query_params, headers=headers)
    return response.json()
  
  def get_advertiser_list(self):
    return self.get(
      endpoint='oauth2/advertiser/get/',
      params={
        'access_token': self.access_token,
        'app_id': self.app_id,
        'secret': self.client_secret
      }
    )
  
  def get_advertiser_info(self, advertiser_ids: Optional[List[str]]=None, fields: Optional[List[str]]=None):
    assert advertiser_ids is not None or self.advertiser_id is not None
    if advertiser_ids is None:
      advertiser_ids = [self.advertiser_id]

    response = self.get(
      endpoint='2/advertiser/info/',
      params={
        'advertiser_ids': advertiser_ids,
        **({'fields': fields} if fields is not None else {})
      }
    )
    return response['data']
  
  def get_entities(self, granularity: str, ids: Optional[List[str]]=None, advertiser_id: Optional[str]=None, deleted_only: bool=False) -> List[Dict[str, any]]:
    granularity = EntityGranularity(granularity)
    if ids is not None and len(ids) == 0:
      return []

    advertiser_id = advertiser_id if advertiser_id is not None else self.advertiser_id
    assert advertiser_id is not None
    response = self.get(
      endpoint=f'2/{granularity.value}/get/',
      params={
        'advertiser_id': self.advertiser_id,
        'page_size': 1000,
        'filtering': {
          **({f'{granularity.value}_ids': ids} if ids is not None else {}),
          **({'primary_status': 'STATUS_DELETE'} if deleted_only else {}),
        }
      }
    )
    return response['data']['list']