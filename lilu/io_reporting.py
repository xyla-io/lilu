import pandas as pd
from io_channel import IOChannelGranularity, IOChannelProperty, IOTimeGranularity, IOEntityGranularity, IOTimeMetric, IOEntityMetric, IOEntityAttribute, IOReportOption

from .context import TimeGranularity, EntityGranularity
from .api import TikTokAPI
from .reporting import TikTokReporter
from datetime import datetime, timedelta
from io_channel import IOChannelSourceReporter
from typing import List, Dict, Optional
from math import ceil

class IOTikTokReporter(IOChannelSourceReporter):
  @classmethod
  def _get_map_identifier(cls) -> str:
    return f'lilu/{cls.__name__}'

  @property
  def start_date(self) -> Optional[datetime]:
    date = self.get_from_filters('start_date')
    return datetime.strptime(date, '%Y-%m-%d') if date else None
  
  @property
  def end_date(self) -> Optional[datetime]:
    date = self.get_from_filters('end_date')
    return datetime.strptime(date, '%Y-%m-%d') if date else None

  @property
  def time_granularity(self) -> Optional[IOTimeGranularity]:
    raw_granularity = self.get_from_options(IOReportOption.time_granularity)
    return IOTimeGranularity(raw_granularity) if raw_granularity else None

  def io_time_granularity_to_api(self, granularity: IOTimeGranularity) -> Optional[str]:
    if granularity in [IOTimeGranularity.hourly, IOTimeGranularity.daily]:
      return TimeGranularity(granularity.value).api_value
    else:
      return None

  def io_time_metric_to_api(self, metric: IOTimeMetric, granularity: Optional[IOTimeGranularity]=None) -> Optional[str]:
    if metric is IOTimeMetric.time:
      return 'stat_datetime'
    else:
      return None

  def io_entity_granularity_to_api(self, granularity: IOEntityGranularity) -> Optional[str]:
    if granularity is IOEntityGranularity.account:
      return 'advertiser'
    else:
      return super().io_entity_granularity_to_api(granularity)

  def io_entity_attribute_to_api(self, attribute: IOEntityAttribute, granularity: IOEntityGranularity) -> Optional[str]:
    if granularity:
      api_granularity = self.io_entity_granularity_to_api(granularity)
      if api_granularity:
        api_prefix = f'{api_granularity}_' if granularity is IOEntityGranularity.account else EntityGranularity(api_granularity).prefix
        if attribute in [IOEntityAttribute.id, IOEntityAttribute.name]:
          return f'{api_prefix * 2}{attribute.value}'
        else:
          column = super().io_entity_attribute_to_api(
            attribute=attribute,
            granularity=granularity
          )
          return f'{api_prefix}{column}' if column else None

    return super().io_entity_attribute_to_api(
      attribute=attribute,
      granularity=granularity
    )

  def fetch_entity_report(self, granularity: IOEntityGranularity, reporter: TikTokReporter) -> pd.DataFrame:
    api_entity_columns = self.filtered_api_entity_attributes(granularity=granularity)

    if granularity is IOEntityGranularity.account:
      advertiser_info = reporter.api.get_advertiser_info()
      advertiser_report = pd.DataFrame(advertiser_info)
      advertiser_report.id = advertiser_report.id.astype(str)
      advertiser_prefix = f'{self.io_entity_granularity_to_api(granularity)}_'
      advertiser_report.rename(
        columns={
          'id': f'{advertiser_prefix}id',
          'name': f'{advertiser_prefix}name',
        },
        inplace=True
      )
      advertiser_report = advertiser_report.add_prefix(advertiser_prefix)
      return advertiser_report

    entity_granularity = EntityGranularity(self.io_entity_granularity_to_api(granularity))
    api_entity_identifiers = [
      self.io_to_api(g.identifier_property.value, g.value)
      for g in [granularity] + granularity.higher
    ]
    api_report_identifiers = [
      f'{entity_granularity.prefix}{"_".join(i.split("_")[1:])}' 
      for i in api_entity_identifiers
    ]
    api_entity_columns.extend([
      i
      for i in api_report_identifiers
      if i not in api_entity_columns
    ])

    report = pd.DataFrame()
    # TODO: Extract entity IDs from filters
    entity_ids = None
    if api_entity_columns or entity_ids is None:
      attribute_report = reporter.get_entity_report(
        granularity=entity_granularity.value,
        ids=entity_ids,
        columns=api_entity_columns
      )
      if not attribute_report.empty:
        report = report.append(attribute_report)

    api_metric_columns = self.filtered_api_entity_metrics(granularity=granularity)
    # TODO: Support metric columns using the add_performance_metrics() method, or using only the get_performance_report() method if name and ID are the only attribute columns
    if api_metric_columns:
      # api_time_granularity = self.io_to_api(self.time_granularity.value if self.time_granularity else IOTimeGranularity.daily.value)
      raise NotImplementedError()

    if not report.empty:
      report.rename(
        columns=dict(zip(api_report_identifiers, api_entity_identifiers)),
        inplace=True
      )
      report[api_entity_identifiers] = report[api_entity_identifiers].astype(str)
      report.reset_index(drop=True, inplace=True)
    return report

  def run(self, credentials: Dict[str, any], api: Optional[TikTokAPI]=None) -> Dict[str, any]:
    if api is None:
      api = TikTokAPI(**credentials)
    reporter = TikTokReporter(api=api)
    report = pd.DataFrame()
    
    for granularity in self.filtered_io_entity_granularities:
      api_report = self.fetch_entity_report(granularity=granularity, reporter=reporter)
      io_report = self.api_report_to_io(
        api_report=api_report,
        granularities=[
          granularity,
          *([self.time_granularity] if self.time_granularity else []),
        ])
      self.fill_api_ancestor_identifiers_in_io(
        api_report=api_report,
        io_report=io_report,
        granularities=[
          granularity,
          *([self.time_granularity] if self.time_granularity else []),
        ]
      )
      report = report.append(io_report)

    report = self.finalized_io_report(report)
    return report
