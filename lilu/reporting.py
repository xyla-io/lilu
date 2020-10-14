import pandas as pd

from .api import TikTokAPI
from .error import TikTokMissingAdvertiserError
from .context import TimeGranularity, EntityGranularity
from datetime import datetime
from typing import List, Optional, Callable

def require_advertiser_id(f: Callable[..., any]) -> Callable[..., any]:
  def wrapper(self, *args, **kwargs):
    if self.api.advertiser_id is None:
      raise TikTokMissingAdvertiserError()
    return f(self, *args, **kwargs)  
  return wrapper

class TikTokReporter:
  api: TikTokAPI

  def __init__(self, api: TikTokAPI):
    self.api = api
  
  def formatted_date(self, date: datetime) -> str:
    return date.strftime('%Y-%m-%d')
  
  @require_advertiser_id
  def get_entity_report(self, granularity: str, ids: Optional[List[str]]=None, columns: Optional[List[str]]=None, deleted_only: bool=False) -> pd.DataFrame:
    if ids is not None and len(ids) == 0:
      return pd.DataFrame() if columns is None else pd.DataFrame(columns=columns)

    entity_granularity = EntityGranularity(granularity)
    response = self.api.get_entities(
      granularity=entity_granularity.value,
      ids=None if ids is not None and len(ids) > 100 else ids,
      deleted_only=deleted_only
    )
    df = pd.DataFrame(response)
    if df.empty:
      return pd.DataFrame() if columns is None else pd.DataFrame(columns=columns)
      
    if ids is not None and len(ids) > 100:
      id_column = f'{entity_granularity.prefix}id'
      df.drop(df.index[~df[id_column].isin(ids)], inplace=True)
      df.reset_index(drop=True, inplace=True)
      
    df = df.add_prefix(entity_granularity.prefix)
    if columns is not None:
      selected_columns = list(filter(lambda c: c in df.columns, columns))
      df = df[selected_columns]

    return df

  @require_advertiser_id
  def get_performance_report(self, time_granularity: str, start: datetime, end: datetime, entity_granularity: str, entity_ids: Optional[List[str]]=None, columns: Optional[List[str]]=None, deleted_only: bool=False):
    entity_granularity = EntityGranularity(entity_granularity)
    time_granularity = TimeGranularity(time_granularity)
    if columns is None:
      columns = entity_granularity.performance_columns
    if entity_ids is not None and len(entity_ids) == 0:
      return pd.DataFrame(columns=columns)
    
    fields = list(filter(lambda c: c is not None, map(entity_granularity.performance_to_api_column, columns)))
    response = self.api.get(
      endpoint=f'2/reports/{entity_granularity.value}/get/',
      params={
        'advertiser_id': self.api.advertiser_id,
        'start_date': self.formatted_date(start),
        'end_date': self.formatted_date(end),
        'time_granularity': time_granularity.api_value,
        'fields': fields,
        'page_size': 1000,
        'group_by': ['STAT_GROUP_BY_FIELD_STAT_TIME', 'STAT_GROUP_BY_FIELD_ID'],
        'filtering': {
          **({f'{entity_granularity.value}_ids': entity_ids} if entity_ids is not None else {}),
          **({'primary_status': 'STATUS_DELETE'} if deleted_only else {}),
        }
      }
    )
    df = pd.DataFrame(response['data']['list'])
    field_map = {
      **{
        f: entity_granularity.api_to_performance_column(f)
        for f in df.columns
        if entity_granularity.api_to_performance_column(f) in columns
      },
      **({time_granularity.api_column: f'{entity_granularity.prefix}{time_granularity.api_column}'} if time_granularity.api_column in df.columns else {}),
    }
    df = df[list(field_map.keys())]
    df.rename(columns=field_map, inplace=True)

    return df
  
  def add_entity_info(self, report: pd.DataFrame, report_entity_granularity: str, added_entity_granularity: Optional[str]=None, columns: Optional[List[str]]=None, deleted_only: bool=False) -> pd.DataFrame:
    if report.empty:
      return pd.DataFrame(columns=columns + list(report.columns)) if columns is not None else report.copy()

    if added_entity_granularity is None:
      added_entity_granularity = report_entity_granularity

    assert f'{report_entity_granularity}_{added_entity_granularity}_id' in report.columns

    entity_ids = [str(i) for i in report[f'{report_entity_granularity}_{added_entity_granularity}_id'].unique()]
    entity_report = self.get_entity_report(
      granularity=added_entity_granularity,
      ids=entity_ids,
      columns=columns,
      deleted_only=deleted_only
    )
    overlapping_columns = set(entity_report.columns).intersection(set(report.columns)) - {f'{added_entity_granularity}_{added_entity_granularity}_id'}
    entity_report.drop(columns=overlapping_columns, inplace=True)

    merged_report = report.merge(
      right=entity_report,
      how='left',
      left_on=f'{report_entity_granularity}_{added_entity_granularity}_id',
      right_on=f'{added_entity_granularity}_{added_entity_granularity}_id',
      suffixes=('', '')
    )
    return merged_report

  def add_performance_metrics(self, entity_report: pd.DataFrame, entity_granularity: str, time_granularity: str, start: datetime, end: datetime, columns: Optional[List[str]]=None, deleted_only: bool=False):
    raise NotImplementedError()