from enum import Enum
from typing import Optional, List, Dict

class TimeGranularity(Enum):
  hourly = 'hourly'
  daily = 'daily'

  @property
  def api_value(self) -> str:
    if self is TimeGranularity.hourly:
      return 'STAT_TIME_GRANULARITY_HOURLY'
    elif self is TimeGranularity.daily:
      return 'STAT_TIME_GRANULARITY_DAILY'

  @property
  def api_column(self) -> str:
    return 'stat_datetime'

class EntityGranularity(Enum):
  campaign = 'campaign'
  adgroup = 'adgroup'
  ad = 'ad'

  @property
  def prefix(self) -> str:
    return f'{self.value}_'

  def performance_to_api_column(self, performance_column: str) -> Optional[str]:
    api_column = performance_column.split(self.prefix, maxsplit=1)[1]
    if api_column in [
      'active_cost',
      'active_rate',
    ]:
      api_column = None
    return api_column

  def api_to_performance_column(self, api_column: str) -> Optional[str]:
    for performance_column in self.performance_columns:
      if self.performance_to_api_column(performance_column) == api_column:
        return performance_column
    return None

  @property
  def entity_columns(self) -> List[str]:
    if self is EntityGranularity.campaign:
      return [
        'campaign_advertiser_id',
        'campaign_campaign_id',
        'campaign_campaign_name',
        'campaign_budget_mode',
        'campaign_budget',
        'campaign_objective_type',
        'campaign_create_time',
        'campaign_modify_time',
        'campaign_status',
        'campaign_opt_status',
        'campaign_objective',
      ]
    elif self is EntityGranularity.adgroup:
      return [
        'adgroup_advertiser_id',
        'adgroup_adgroup_id',
        'adgroup_adgroup_name',
        'adgroup_campaign_id',
        'adgroup_campaign_name',
        'adgroup_placement_type',
        'adgroup_placement',
        'adgroup_enable_inventory_filter',
        'adgroup_landing_page_url',
        'adgroup_display_name',
        'adgroup_app_id',
        'adgroup_app_download_url',
        'adgroup_open_url',
        'adgroup_app_name',
        'adgroup_app_type',
        'adgroup_package',
        'adgroup_category',
        'adgroup_keywords',
        'adgroup_avatar_icon',
        'adgroup_is_comment_disable',
        'adgroup_android_osv',
        'adgroup_ios_osv',
        'adgroup_audience',
        'adgroup_excluded_audience',
        'adgroup_gender',
        'adgroup_location',
        'adgroup_age',
        'adgroup_languages',
        'adgroup_connection_type',
        'adgroup_operation_system',
        'adgroup_device_price',
        'adgroup_interest_category',
        'adgroup_budget',
        'adgroup_budget_mode',
        'adgroup_pacing',
        'adgroup_frequency',
        'adgroup_frequency_schedule',
        'adgroup_schedule_type',
        'adgroup_schedule_start_time',
        'adgroup_schedule_end_time',
        'adgroup_dayparting',
        'adgroup_billing_event',
        'adgroup_bid',
        'adgroup_conversion_id',
        'adgroup_skip_learning_phase',
        'adgroup_conversion_bid',
        'adgroup_impression_tracking_url',
        'adgroup_click_tracking_url',
        'adgroup_video_view_tracking_url',
        'adgroup_create_time',
        'adgroup_modify_time',
        'adgroup_creative_material_mode',
        'adgroup_optimize_goal',
        'adgroup_external_action',
        'adgroup_deep_external_action',
        'adgroup_deep_bid_type',
        'adgroup_status',
        'adgroup_pixel_id',
        'adgroup_profile_image',
        'adgroup_deep_cpabid',
        'adgroup_opt_status',
        'adgroup_bid_type',
        'adgroup_statistic_type',
      ]
    elif self is EntityGranularity.ad:
      return [
        'ad_advertiser_id',
        'ad_ad_id',
        'ad_ad_name',
        'ad_campaign_name',
        'ad_adgroup_id',
        'ad_adgroup_name',
        'ad_campaign_id',
        'ad_status',
        'ad_opt_status',
        'ad_call_to_action',
        'ad_video_id',
        'ad_image_ids',
        'ad_create_time',
        'ad_modify_time',
        'ad_is_aco',
        'ad_image_mode',
        'ad_profile_image',
        'ad_click_tracking_url',
        'ad_display_name',
        'ad_impression_tracking_url',
        'ad_video_view_tracking_url',
        'ad_landing_page_url',
        'ad_open_url',
        'ad_app_name',
      ]

  @property
  def performance_columns(self) -> List[str]:
    if self is EntityGranularity.campaign:
      return [
        'campaign_campaign_id',
        'campaign_campaign_name',
        'campaign_active_register',
        'campaign_skip',
        'campaign_active_register_rate',
        'campaign_active_rate',
        'campaign_active_pay_amount',
        'campaign_active_pay_avg_amount',
        'campaign_dy_comment',
        'campaign_active_pay_cost',
        'campaign_conversion_rate',
        'campaign_active_pay_show',
        'campaign_ecpm',
        'campaign_active_register_click_cost',
        'campaign_active_register_show_cost',
        'campaign_active_register_click',
        'campaign_conversion_cost',
        'campaign_active_click_cost',
        'campaign_stat_cost',
        'campaign_active_pay_click_cost',
        'campaign_active_register_cost',
        'campaign_active_pay_click',
        'campaign_dy_like',
        'campaign_active_pay_rate',
        'campaign_click_cost',
        'campaign_active_show',
        'campaign_active_click',
        'campaign_active',
        'campaign_convert_cnt',
        'campaign_show_cnt',
        'campaign_dy_share',
        'campaign_activate_cost',
        'campaign_ctr',
        'campaign_active_pay',
        'campaign_active_cost',
        'campaign_active_register_show',
        'campaign_active_pay_show_cost',
        'campaign_activate_rate',
        'campaign_time_attr_convert_cnt',
        'campaign_click_cnt',
        'campaign_dy_home_visited',
        'campaign_active_show_cost',
      ]
    elif self is EntityGranularity.adgroup:
      return [
        # 'adgroup_campaign_id',
        # 'adgroup_campaign_name',
        'adgroup_adgroup_id',
        'adgroup_adgroup_name',
        'adgroup_active_register',
        'adgroup_skip',
        'adgroup_active_register_rate',
        'adgroup_active_rate',
        'adgroup_active_pay_amount',
        'adgroup_active_pay_avg_amount',
        'adgroup_dy_comment',
        'adgroup_active_pay_cost',
        'adgroup_conversion_rate',
        'adgroup_active_pay_show',
        'adgroup_ecpm',
        'adgroup_active_register_click_cost',
        'adgroup_active_register_show_cost',
        'adgroup_dy_like',
        'adgroup_conversion_cost',
        'adgroup_active_click_cost',
        'adgroup_stat_cost',
        'adgroup_active_pay_click_cost',
        'adgroup_active_register_cost',
        'adgroup_active_pay_click',
        'adgroup_active_register_click',
        'adgroup_active_pay_rate',
        'adgroup_click_cost',
        'adgroup_active_show',
        'adgroup_active_click',
        'adgroup_active',
        'adgroup_convert_cnt',
        'adgroup_show_cnt',
        'adgroup_dy_share',
        'adgroup_activate_cost',
        'adgroup_ctr',
        'adgroup_active_pay',
        'adgroup_active_cost',
        'adgroup_active_register_show',
        'adgroup_active_pay_show_cost',
        'adgroup_activate_rate',
        'adgroup_time_attr_convert_cnt',
        'adgroup_click_cnt',
        'adgroup_dy_home_visited',
        'adgroup_active_show_cost',
      ]
    elif self is EntityGranularity.ad:
      return [
        # 'ad_campaign_id',
        # 'ad_campaign_name',
        # 'ad_adgroup_id',
        # 'ad_adgroup_name',
        'ad_ad_id',
        'ad_ad_name',
        'ad_active_register',
        'ad_skip',
        'ad_active_register_rate',
        'ad_active_rate',
        'ad_active_pay_amount',
        'ad_active_pay_avg_amount',
        'ad_dy_comment',
        'ad_active_pay_cost',
        'ad_active',
        'ad_conversion_rate',
        'ad_active_pay_show',
        'ad_ecpm',
        'ad_active_register_click_cost',
        'ad_active_register_show_cost',
        'ad_dy_like',
        'ad_conversion_cost',
        'ad_active_click_cost',
        'ad_stat_cost',
        'ad_active_pay_click_cost',
        'ad_active_register_cost',
        'ad_active_pay_click',
        'ad_active_register_click',
        'ad_active_pay_rate',
        'ad_ad_text',
        'ad_click_cost',
        'ad_active_show',
        'ad_active_click',
        'ad_convert_cnt',
        'ad_show_cnt',
        'ad_dy_share',
        'ad_activate_cost',
        'ad_ctr',
        'ad_active_pay',
        'ad_active_cost',
        'ad_active_register_show',
        'ad_active_pay_show_cost',
        'ad_activate_rate',
        'ad_time_attr_convert_cnt',
        'ad_click_cnt',
        'ad_dy_home_visited',
        'ad_active_show_cost',
      ]