import pytest

from ..api import TikTokAPI
from ..reporting import TikTokReporter
from ..io_reporting import IOTikTokReporter
from datetime import datetime

@pytest.fixture
def credentials():
  return {
    'access_token': 'ACCESS_TOKEN',
    'client_secret': 'CLIENT_SECRET',
    'app_id': 'APP_ID',
    'advertiser_id': 'ADVERTISER_ID'
  }

@pytest.fixture
def api(credentials):
  return TikTokAPI(**credentials)

@pytest.fixture
def reporter(api):
  return TikTokReporter(api=api)

@pytest.fixture
def io_reporter():
  return IOTikTokReporter(
    columns=[
      'account.id',
      'account.name',
      'campaign.id',
      'campaign.name',
      'adgroup.id',
      'adgroup.name',
    ]
  )

def test_advertiser_list(api):
  response = api.get_advertiser_list()
  import pdb; pdb.set_trace()
  assert response is not None

def test_advertiser_info(api):
  response = api.get_advertiser_info()
  import pdb; pdb.set_trace()
  assert response is not None

def test_campaign_reporting(reporter):
  response = reporter.get_campaign_data(
    start=datetime.strptime('2020-04-20', '%Y-%m-%d'),
    end=datetime.strptime('2020-04-29', '%Y-%m-%d'),
    time_granularity='daily'
  )
  import pdb; pdb.set_trace()
  assert response is not None

def test_entity_reporting(reporter):
  df = reporter.get_entity_report(
    granularity='ad',
  )
  merged = reporter.add_entity_info(
    report=df,
    report_entity_granularity='ad',
    added_entity_granularity='campaign'
  )
  import pdb; pdb.set_trace()
  assert df is not None and merged is not None

def test_get_entities(api):
  granularity = 'adgroup'
  entities = api.get_entities(granularity=granularity)
  import pdb; pdb.set_trace()
  assert entities is not None

def test_performance_reporting(reporter):
  df = reporter.get_performance_report(
    time_granularity='daily',
    start=datetime.strptime('2020-05-01', '%Y-%m-%d'),
    end=datetime.strptime('2020-05-03', '%Y-%m-%d'),
    entity_granularity='adgroup',
  )
  df = reporter.add_entity_info(
    report=df,
    report_entity_granularity='adgroup'
  )
  import pdb; pdb.set_trace()
  assert df is not None

def test_io_reporting(io_reporter, credentials):
  TikTokAPI.sandbox_environment = True
  df = io_reporter.run(credentials=credentials)
  assert df is not None
