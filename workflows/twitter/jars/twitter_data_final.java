// ORM class for table 'twitter_data_final'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Tue Oct 10 05:57:41 UTC 2017
// For connector: org.apache.sqoop.manager.MySQLManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class twitter_data_final extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  protected ResultSet __cur_result_set;
  private String accountId;
  public String get_accountId() {
    return accountId;
  }
  public void set_accountId(String accountId) {
    this.accountId = accountId;
  }
  public twitter_data_final with_accountId(String accountId) {
    this.accountId = accountId;
    return this;
  }
  private String accountName;
  public String get_accountName() {
    return accountName;
  }
  public void set_accountName(String accountName) {
    this.accountName = accountName;
  }
  public twitter_data_final with_accountName(String accountName) {
    this.accountName = accountName;
    return this;
  }
  private String consumerKey;
  public String get_consumerKey() {
    return consumerKey;
  }
  public void set_consumerKey(String consumerKey) {
    this.consumerKey = consumerKey;
  }
  public twitter_data_final with_consumerKey(String consumerKey) {
    this.consumerKey = consumerKey;
    return this;
  }
  private String campaignId;
  public String get_campaignId() {
    return campaignId;
  }
  public void set_campaignId(String campaignId) {
    this.campaignId = campaignId;
  }
  public twitter_data_final with_campaignId(String campaignId) {
    this.campaignId = campaignId;
    return this;
  }
  private String campaign_name;
  public String get_campaign_name() {
    return campaign_name;
  }
  public void set_campaign_name(String campaign_name) {
    this.campaign_name = campaign_name;
  }
  public twitter_data_final with_campaign_name(String campaign_name) {
    this.campaign_name = campaign_name;
    return this;
  }
  private String day_date;
  public String get_day_date() {
    return day_date;
  }
  public void set_day_date(String day_date) {
    this.day_date = day_date;
  }
  public twitter_data_final with_day_date(String day_date) {
    this.day_date = day_date;
    return this;
  }
  private String start_date;
  public String get_start_date() {
    return start_date;
  }
  public void set_start_date(String start_date) {
    this.start_date = start_date;
  }
  public twitter_data_final with_start_date(String start_date) {
    this.start_date = start_date;
    return this;
  }
  private String end_date;
  public String get_end_date() {
    return end_date;
  }
  public void set_end_date(String end_date) {
    this.end_date = end_date;
  }
  public twitter_data_final with_end_date(String end_date) {
    this.end_date = end_date;
    return this;
  }
  private java.math.BigDecimal daily_budget;
  public java.math.BigDecimal get_daily_budget() {
    return daily_budget;
  }
  public void set_daily_budget(java.math.BigDecimal daily_budget) {
    this.daily_budget = daily_budget;
  }
  public twitter_data_final with_daily_budget(java.math.BigDecimal daily_budget) {
    this.daily_budget = daily_budget;
    return this;
  }
  private java.math.BigDecimal total_budget;
  public java.math.BigDecimal get_total_budget() {
    return total_budget;
  }
  public void set_total_budget(java.math.BigDecimal total_budget) {
    this.total_budget = total_budget;
  }
  public twitter_data_final with_total_budget(java.math.BigDecimal total_budget) {
    this.total_budget = total_budget;
    return this;
  }
  private String currency;
  public String get_currency() {
    return currency;
  }
  public void set_currency(String currency) {
    this.currency = currency;
  }
  public twitter_data_final with_currency(String currency) {
    this.currency = currency;
    return this;
  }
  private java.math.BigDecimal spend;
  public java.math.BigDecimal get_spend() {
    return spend;
  }
  public void set_spend(java.math.BigDecimal spend) {
    this.spend = spend;
  }
  public twitter_data_final with_spend(java.math.BigDecimal spend) {
    this.spend = spend;
    return this;
  }
  private Integer impressions;
  public Integer get_impressions() {
    return impressions;
  }
  public void set_impressions(Integer impressions) {
    this.impressions = impressions;
  }
  public twitter_data_final with_impressions(Integer impressions) {
    this.impressions = impressions;
    return this;
  }
  private Integer engagements;
  public Integer get_engagements() {
    return engagements;
  }
  public void set_engagements(Integer engagements) {
    this.engagements = engagements;
  }
  public twitter_data_final with_engagements(Integer engagements) {
    this.engagements = engagements;
    return this;
  }
  private Integer billed_engagements;
  public Integer get_billed_engagements() {
    return billed_engagements;
  }
  public void set_billed_engagements(Integer billed_engagements) {
    this.billed_engagements = billed_engagements;
  }
  public twitter_data_final with_billed_engagements(Integer billed_engagements) {
    this.billed_engagements = billed_engagements;
    return this;
  }
  private Integer retweets;
  public Integer get_retweets() {
    return retweets;
  }
  public void set_retweets(Integer retweets) {
    this.retweets = retweets;
  }
  public twitter_data_final with_retweets(Integer retweets) {
    this.retweets = retweets;
    return this;
  }
  private Integer replies;
  public Integer get_replies() {
    return replies;
  }
  public void set_replies(Integer replies) {
    this.replies = replies;
  }
  public twitter_data_final with_replies(Integer replies) {
    this.replies = replies;
    return this;
  }
  private Integer follows;
  public Integer get_follows() {
    return follows;
  }
  public void set_follows(Integer follows) {
    this.follows = follows;
  }
  public twitter_data_final with_follows(Integer follows) {
    this.follows = follows;
    return this;
  }
  private Integer clicks;
  public Integer get_clicks() {
    return clicks;
  }
  public void set_clicks(Integer clicks) {
    this.clicks = clicks;
  }
  public twitter_data_final with_clicks(Integer clicks) {
    this.clicks = clicks;
    return this;
  }
  private Integer media_engagements;
  public Integer get_media_engagements() {
    return media_engagements;
  }
  public void set_media_engagements(Integer media_engagements) {
    this.media_engagements = media_engagements;
  }
  public twitter_data_final with_media_engagements(Integer media_engagements) {
    this.media_engagements = media_engagements;
    return this;
  }
  private Integer likes;
  public Integer get_likes() {
    return likes;
  }
  public void set_likes(Integer likes) {
    this.likes = likes;
  }
  public twitter_data_final with_likes(Integer likes) {
    this.likes = likes;
    return this;
  }
  private Double engagement_rate;
  public Double get_engagement_rate() {
    return engagement_rate;
  }
  public void set_engagement_rate(Double engagement_rate) {
    this.engagement_rate = engagement_rate;
  }
  public twitter_data_final with_engagement_rate(Double engagement_rate) {
    this.engagement_rate = engagement_rate;
    return this;
  }
  private Integer link_clicks;
  public Integer get_link_clicks() {
    return link_clicks;
  }
  public void set_link_clicks(Integer link_clicks) {
    this.link_clicks = link_clicks;
  }
  public twitter_data_final with_link_clicks(Integer link_clicks) {
    this.link_clicks = link_clicks;
    return this;
  }
  private Integer app_clicks;
  public Integer get_app_clicks() {
    return app_clicks;
  }
  public void set_app_clicks(Integer app_clicks) {
    this.app_clicks = app_clicks;
  }
  public twitter_data_final with_app_clicks(Integer app_clicks) {
    this.app_clicks = app_clicks;
    return this;
  }
  private java.math.BigDecimal cost_per_engagement;
  public java.math.BigDecimal get_cost_per_engagement() {
    return cost_per_engagement;
  }
  public void set_cost_per_engagement(java.math.BigDecimal cost_per_engagement) {
    this.cost_per_engagement = cost_per_engagement;
  }
  public twitter_data_final with_cost_per_engagement(java.math.BigDecimal cost_per_engagement) {
    this.cost_per_engagement = cost_per_engagement;
    return this;
  }
  private java.math.BigDecimal cost_per_follow;
  public java.math.BigDecimal get_cost_per_follow() {
    return cost_per_follow;
  }
  public void set_cost_per_follow(java.math.BigDecimal cost_per_follow) {
    this.cost_per_follow = cost_per_follow;
  }
  public twitter_data_final with_cost_per_follow(java.math.BigDecimal cost_per_follow) {
    this.cost_per_follow = cost_per_follow;
    return this;
  }
  private java.math.BigDecimal cost_per_link_click;
  public java.math.BigDecimal get_cost_per_link_click() {
    return cost_per_link_click;
  }
  public void set_cost_per_link_click(java.math.BigDecimal cost_per_link_click) {
    this.cost_per_link_click = cost_per_link_click;
  }
  public twitter_data_final with_cost_per_link_click(java.math.BigDecimal cost_per_link_click) {
    this.cost_per_link_click = cost_per_link_click;
    return this;
  }
  private Integer card_engagements;
  public Integer get_card_engagements() {
    return card_engagements;
  }
  public void set_card_engagements(Integer card_engagements) {
    this.card_engagements = card_engagements;
  }
  public twitter_data_final with_card_engagements(Integer card_engagements) {
    this.card_engagements = card_engagements;
    return this;
  }
  private Integer tweets_send;
  public Integer get_tweets_send() {
    return tweets_send;
  }
  public void set_tweets_send(Integer tweets_send) {
    this.tweets_send = tweets_send;
  }
  public twitter_data_final with_tweets_send(Integer tweets_send) {
    this.tweets_send = tweets_send;
    return this;
  }
  private Integer qualified_impressions;
  public Integer get_qualified_impressions() {
    return qualified_impressions;
  }
  public void set_qualified_impressions(Integer qualified_impressions) {
    this.qualified_impressions = qualified_impressions;
  }
  public twitter_data_final with_qualified_impressions(Integer qualified_impressions) {
    this.qualified_impressions = qualified_impressions;
    return this;
  }
  private Integer video_views_25;
  public Integer get_video_views_25() {
    return video_views_25;
  }
  public void set_video_views_25(Integer video_views_25) {
    this.video_views_25 = video_views_25;
  }
  public twitter_data_final with_video_views_25(Integer video_views_25) {
    this.video_views_25 = video_views_25;
    return this;
  }
  private Integer video_views_75;
  public Integer get_video_views_75() {
    return video_views_75;
  }
  public void set_video_views_75(Integer video_views_75) {
    this.video_views_75 = video_views_75;
  }
  public twitter_data_final with_video_views_75(Integer video_views_75) {
    this.video_views_75 = video_views_75;
    return this;
  }
  private Integer video_views_100;
  public Integer get_video_views_100() {
    return video_views_100;
  }
  public void set_video_views_100(Integer video_views_100) {
    this.video_views_100 = video_views_100;
  }
  public twitter_data_final with_video_views_100(Integer video_views_100) {
    this.video_views_100 = video_views_100;
    return this;
  }
  private Integer video_total_views;
  public Integer get_video_total_views() {
    return video_total_views;
  }
  public void set_video_total_views(Integer video_total_views) {
    this.video_total_views = video_total_views;
  }
  public twitter_data_final with_video_total_views(Integer video_total_views) {
    this.video_total_views = video_total_views;
    return this;
  }
  private Integer video_3s100pct_views;
  public Integer get_video_3s100pct_views() {
    return video_3s100pct_views;
  }
  public void set_video_3s100pct_views(Integer video_3s100pct_views) {
    this.video_3s100pct_views = video_3s100pct_views;
  }
  public twitter_data_final with_video_3s100pct_views(Integer video_3s100pct_views) {
    this.video_3s100pct_views = video_3s100pct_views;
    return this;
  }
  private Integer video_cta_clicks;
  public Integer get_video_cta_clicks() {
    return video_cta_clicks;
  }
  public void set_video_cta_clicks(Integer video_cta_clicks) {
    this.video_cta_clicks = video_cta_clicks;
  }
  public twitter_data_final with_video_cta_clicks(Integer video_cta_clicks) {
    this.video_cta_clicks = video_cta_clicks;
    return this;
  }
  private Integer video_content_starts;
  public Integer get_video_content_starts() {
    return video_content_starts;
  }
  public void set_video_content_starts(Integer video_content_starts) {
    this.video_content_starts = video_content_starts;
  }
  public twitter_data_final with_video_content_starts(Integer video_content_starts) {
    this.video_content_starts = video_content_starts;
    return this;
  }
  private Integer video_mrc_views;
  public Integer get_video_mrc_views() {
    return video_mrc_views;
  }
  public void set_video_mrc_views(Integer video_mrc_views) {
    this.video_mrc_views = video_mrc_views;
  }
  public twitter_data_final with_video_mrc_views(Integer video_mrc_views) {
    this.video_mrc_views = video_mrc_views;
    return this;
  }
  private Integer media_views;
  public Integer get_media_views() {
    return media_views;
  }
  public void set_media_views(Integer media_views) {
    this.media_views = media_views;
  }
  public twitter_data_final with_media_views(Integer media_views) {
    this.media_views = media_views;
    return this;
  }
  private String product;
  public String get_product() {
    return product;
  }
  public void set_product(String product) {
    this.product = product;
  }
  public twitter_data_final with_product(String product) {
    this.product = product;
    return this;
  }
  private String pos;
  public String get_pos() {
    return pos;
  }
  public void set_pos(String pos) {
    this.pos = pos;
  }
  public twitter_data_final with_pos(String pos) {
    this.pos = pos;
    return this;
  }
  private String mobile_os;
  public String get_mobile_os() {
    return mobile_os;
  }
  public void set_mobile_os(String mobile_os) {
    this.mobile_os = mobile_os;
  }
  public twitter_data_final with_mobile_os(String mobile_os) {
    this.mobile_os = mobile_os;
    return this;
  }
  private String language;
  public String get_language() {
    return language;
  }
  public void set_language(String language) {
    this.language = language;
  }
  public twitter_data_final with_language(String language) {
    this.language = language;
    return this;
  }
  private String sub_channel;
  public String get_sub_channel() {
    return sub_channel;
  }
  public void set_sub_channel(String sub_channel) {
    this.sub_channel = sub_channel;
  }
  public twitter_data_final with_sub_channel(String sub_channel) {
    this.sub_channel = sub_channel;
    return this;
  }
  private String channel;
  public String get_channel() {
    return channel;
  }
  public void set_channel(String channel) {
    this.channel = channel;
  }
  public twitter_data_final with_channel(String channel) {
    this.channel = channel;
    return this;
  }
  private String dim_channel_id;
  public String get_dim_channel_id() {
    return dim_channel_id;
  }
  public void set_dim_channel_id(String dim_channel_id) {
    this.dim_channel_id = dim_channel_id;
  }
  public twitter_data_final with_dim_channel_id(String dim_channel_id) {
    this.dim_channel_id = dim_channel_id;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof twitter_data_final)) {
      return false;
    }
    twitter_data_final that = (twitter_data_final) o;
    boolean equal = true;
    equal = equal && (this.accountId == null ? that.accountId == null : this.accountId.equals(that.accountId));
    equal = equal && (this.accountName == null ? that.accountName == null : this.accountName.equals(that.accountName));
    equal = equal && (this.consumerKey == null ? that.consumerKey == null : this.consumerKey.equals(that.consumerKey));
    equal = equal && (this.campaignId == null ? that.campaignId == null : this.campaignId.equals(that.campaignId));
    equal = equal && (this.campaign_name == null ? that.campaign_name == null : this.campaign_name.equals(that.campaign_name));
    equal = equal && (this.day_date == null ? that.day_date == null : this.day_date.equals(that.day_date));
    equal = equal && (this.start_date == null ? that.start_date == null : this.start_date.equals(that.start_date));
    equal = equal && (this.end_date == null ? that.end_date == null : this.end_date.equals(that.end_date));
    equal = equal && (this.daily_budget == null ? that.daily_budget == null : this.daily_budget.equals(that.daily_budget));
    equal = equal && (this.total_budget == null ? that.total_budget == null : this.total_budget.equals(that.total_budget));
    equal = equal && (this.currency == null ? that.currency == null : this.currency.equals(that.currency));
    equal = equal && (this.spend == null ? that.spend == null : this.spend.equals(that.spend));
    equal = equal && (this.impressions == null ? that.impressions == null : this.impressions.equals(that.impressions));
    equal = equal && (this.engagements == null ? that.engagements == null : this.engagements.equals(that.engagements));
    equal = equal && (this.billed_engagements == null ? that.billed_engagements == null : this.billed_engagements.equals(that.billed_engagements));
    equal = equal && (this.retweets == null ? that.retweets == null : this.retweets.equals(that.retweets));
    equal = equal && (this.replies == null ? that.replies == null : this.replies.equals(that.replies));
    equal = equal && (this.follows == null ? that.follows == null : this.follows.equals(that.follows));
    equal = equal && (this.clicks == null ? that.clicks == null : this.clicks.equals(that.clicks));
    equal = equal && (this.media_engagements == null ? that.media_engagements == null : this.media_engagements.equals(that.media_engagements));
    equal = equal && (this.likes == null ? that.likes == null : this.likes.equals(that.likes));
    equal = equal && (this.engagement_rate == null ? that.engagement_rate == null : this.engagement_rate.equals(that.engagement_rate));
    equal = equal && (this.link_clicks == null ? that.link_clicks == null : this.link_clicks.equals(that.link_clicks));
    equal = equal && (this.app_clicks == null ? that.app_clicks == null : this.app_clicks.equals(that.app_clicks));
    equal = equal && (this.cost_per_engagement == null ? that.cost_per_engagement == null : this.cost_per_engagement.equals(that.cost_per_engagement));
    equal = equal && (this.cost_per_follow == null ? that.cost_per_follow == null : this.cost_per_follow.equals(that.cost_per_follow));
    equal = equal && (this.cost_per_link_click == null ? that.cost_per_link_click == null : this.cost_per_link_click.equals(that.cost_per_link_click));
    equal = equal && (this.card_engagements == null ? that.card_engagements == null : this.card_engagements.equals(that.card_engagements));
    equal = equal && (this.tweets_send == null ? that.tweets_send == null : this.tweets_send.equals(that.tweets_send));
    equal = equal && (this.qualified_impressions == null ? that.qualified_impressions == null : this.qualified_impressions.equals(that.qualified_impressions));
    equal = equal && (this.video_views_25 == null ? that.video_views_25 == null : this.video_views_25.equals(that.video_views_25));
    equal = equal && (this.video_views_75 == null ? that.video_views_75 == null : this.video_views_75.equals(that.video_views_75));
    equal = equal && (this.video_views_100 == null ? that.video_views_100 == null : this.video_views_100.equals(that.video_views_100));
    equal = equal && (this.video_total_views == null ? that.video_total_views == null : this.video_total_views.equals(that.video_total_views));
    equal = equal && (this.video_3s100pct_views == null ? that.video_3s100pct_views == null : this.video_3s100pct_views.equals(that.video_3s100pct_views));
    equal = equal && (this.video_cta_clicks == null ? that.video_cta_clicks == null : this.video_cta_clicks.equals(that.video_cta_clicks));
    equal = equal && (this.video_content_starts == null ? that.video_content_starts == null : this.video_content_starts.equals(that.video_content_starts));
    equal = equal && (this.video_mrc_views == null ? that.video_mrc_views == null : this.video_mrc_views.equals(that.video_mrc_views));
    equal = equal && (this.media_views == null ? that.media_views == null : this.media_views.equals(that.media_views));
    equal = equal && (this.product == null ? that.product == null : this.product.equals(that.product));
    equal = equal && (this.pos == null ? that.pos == null : this.pos.equals(that.pos));
    equal = equal && (this.mobile_os == null ? that.mobile_os == null : this.mobile_os.equals(that.mobile_os));
    equal = equal && (this.language == null ? that.language == null : this.language.equals(that.language));
    equal = equal && (this.sub_channel == null ? that.sub_channel == null : this.sub_channel.equals(that.sub_channel));
    equal = equal && (this.channel == null ? that.channel == null : this.channel.equals(that.channel));
    equal = equal && (this.dim_channel_id == null ? that.dim_channel_id == null : this.dim_channel_id.equals(that.dim_channel_id));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof twitter_data_final)) {
      return false;
    }
    twitter_data_final that = (twitter_data_final) o;
    boolean equal = true;
    equal = equal && (this.accountId == null ? that.accountId == null : this.accountId.equals(that.accountId));
    equal = equal && (this.accountName == null ? that.accountName == null : this.accountName.equals(that.accountName));
    equal = equal && (this.consumerKey == null ? that.consumerKey == null : this.consumerKey.equals(that.consumerKey));
    equal = equal && (this.campaignId == null ? that.campaignId == null : this.campaignId.equals(that.campaignId));
    equal = equal && (this.campaign_name == null ? that.campaign_name == null : this.campaign_name.equals(that.campaign_name));
    equal = equal && (this.day_date == null ? that.day_date == null : this.day_date.equals(that.day_date));
    equal = equal && (this.start_date == null ? that.start_date == null : this.start_date.equals(that.start_date));
    equal = equal && (this.end_date == null ? that.end_date == null : this.end_date.equals(that.end_date));
    equal = equal && (this.daily_budget == null ? that.daily_budget == null : this.daily_budget.equals(that.daily_budget));
    equal = equal && (this.total_budget == null ? that.total_budget == null : this.total_budget.equals(that.total_budget));
    equal = equal && (this.currency == null ? that.currency == null : this.currency.equals(that.currency));
    equal = equal && (this.spend == null ? that.spend == null : this.spend.equals(that.spend));
    equal = equal && (this.impressions == null ? that.impressions == null : this.impressions.equals(that.impressions));
    equal = equal && (this.engagements == null ? that.engagements == null : this.engagements.equals(that.engagements));
    equal = equal && (this.billed_engagements == null ? that.billed_engagements == null : this.billed_engagements.equals(that.billed_engagements));
    equal = equal && (this.retweets == null ? that.retweets == null : this.retweets.equals(that.retweets));
    equal = equal && (this.replies == null ? that.replies == null : this.replies.equals(that.replies));
    equal = equal && (this.follows == null ? that.follows == null : this.follows.equals(that.follows));
    equal = equal && (this.clicks == null ? that.clicks == null : this.clicks.equals(that.clicks));
    equal = equal && (this.media_engagements == null ? that.media_engagements == null : this.media_engagements.equals(that.media_engagements));
    equal = equal && (this.likes == null ? that.likes == null : this.likes.equals(that.likes));
    equal = equal && (this.engagement_rate == null ? that.engagement_rate == null : this.engagement_rate.equals(that.engagement_rate));
    equal = equal && (this.link_clicks == null ? that.link_clicks == null : this.link_clicks.equals(that.link_clicks));
    equal = equal && (this.app_clicks == null ? that.app_clicks == null : this.app_clicks.equals(that.app_clicks));
    equal = equal && (this.cost_per_engagement == null ? that.cost_per_engagement == null : this.cost_per_engagement.equals(that.cost_per_engagement));
    equal = equal && (this.cost_per_follow == null ? that.cost_per_follow == null : this.cost_per_follow.equals(that.cost_per_follow));
    equal = equal && (this.cost_per_link_click == null ? that.cost_per_link_click == null : this.cost_per_link_click.equals(that.cost_per_link_click));
    equal = equal && (this.card_engagements == null ? that.card_engagements == null : this.card_engagements.equals(that.card_engagements));
    equal = equal && (this.tweets_send == null ? that.tweets_send == null : this.tweets_send.equals(that.tweets_send));
    equal = equal && (this.qualified_impressions == null ? that.qualified_impressions == null : this.qualified_impressions.equals(that.qualified_impressions));
    equal = equal && (this.video_views_25 == null ? that.video_views_25 == null : this.video_views_25.equals(that.video_views_25));
    equal = equal && (this.video_views_75 == null ? that.video_views_75 == null : this.video_views_75.equals(that.video_views_75));
    equal = equal && (this.video_views_100 == null ? that.video_views_100 == null : this.video_views_100.equals(that.video_views_100));
    equal = equal && (this.video_total_views == null ? that.video_total_views == null : this.video_total_views.equals(that.video_total_views));
    equal = equal && (this.video_3s100pct_views == null ? that.video_3s100pct_views == null : this.video_3s100pct_views.equals(that.video_3s100pct_views));
    equal = equal && (this.video_cta_clicks == null ? that.video_cta_clicks == null : this.video_cta_clicks.equals(that.video_cta_clicks));
    equal = equal && (this.video_content_starts == null ? that.video_content_starts == null : this.video_content_starts.equals(that.video_content_starts));
    equal = equal && (this.video_mrc_views == null ? that.video_mrc_views == null : this.video_mrc_views.equals(that.video_mrc_views));
    equal = equal && (this.media_views == null ? that.media_views == null : this.media_views.equals(that.media_views));
    equal = equal && (this.product == null ? that.product == null : this.product.equals(that.product));
    equal = equal && (this.pos == null ? that.pos == null : this.pos.equals(that.pos));
    equal = equal && (this.mobile_os == null ? that.mobile_os == null : this.mobile_os.equals(that.mobile_os));
    equal = equal && (this.language == null ? that.language == null : this.language.equals(that.language));
    equal = equal && (this.sub_channel == null ? that.sub_channel == null : this.sub_channel.equals(that.sub_channel));
    equal = equal && (this.channel == null ? that.channel == null : this.channel.equals(that.channel));
    equal = equal && (this.dim_channel_id == null ? that.dim_channel_id == null : this.dim_channel_id.equals(that.dim_channel_id));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.accountId = JdbcWritableBridge.readString(1, __dbResults);
    this.accountName = JdbcWritableBridge.readString(2, __dbResults);
    this.consumerKey = JdbcWritableBridge.readString(3, __dbResults);
    this.campaignId = JdbcWritableBridge.readString(4, __dbResults);
    this.campaign_name = JdbcWritableBridge.readString(5, __dbResults);
    this.day_date = JdbcWritableBridge.readString(6, __dbResults);
    this.start_date = JdbcWritableBridge.readString(7, __dbResults);
    this.end_date = JdbcWritableBridge.readString(8, __dbResults);
    this.daily_budget = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.total_budget = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.currency = JdbcWritableBridge.readString(11, __dbResults);
    this.spend = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.impressions = JdbcWritableBridge.readInteger(13, __dbResults);
    this.engagements = JdbcWritableBridge.readInteger(14, __dbResults);
    this.billed_engagements = JdbcWritableBridge.readInteger(15, __dbResults);
    this.retweets = JdbcWritableBridge.readInteger(16, __dbResults);
    this.replies = JdbcWritableBridge.readInteger(17, __dbResults);
    this.follows = JdbcWritableBridge.readInteger(18, __dbResults);
    this.clicks = JdbcWritableBridge.readInteger(19, __dbResults);
    this.media_engagements = JdbcWritableBridge.readInteger(20, __dbResults);
    this.likes = JdbcWritableBridge.readInteger(21, __dbResults);
    this.engagement_rate = JdbcWritableBridge.readDouble(22, __dbResults);
    this.link_clicks = JdbcWritableBridge.readInteger(23, __dbResults);
    this.app_clicks = JdbcWritableBridge.readInteger(24, __dbResults);
    this.cost_per_engagement = JdbcWritableBridge.readBigDecimal(25, __dbResults);
    this.cost_per_follow = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.cost_per_link_click = JdbcWritableBridge.readBigDecimal(27, __dbResults);
    this.card_engagements = JdbcWritableBridge.readInteger(28, __dbResults);
    this.tweets_send = JdbcWritableBridge.readInteger(29, __dbResults);
    this.qualified_impressions = JdbcWritableBridge.readInteger(30, __dbResults);
    this.video_views_25 = JdbcWritableBridge.readInteger(31, __dbResults);
    this.video_views_75 = JdbcWritableBridge.readInteger(32, __dbResults);
    this.video_views_100 = JdbcWritableBridge.readInteger(33, __dbResults);
    this.video_total_views = JdbcWritableBridge.readInteger(34, __dbResults);
    this.video_3s100pct_views = JdbcWritableBridge.readInteger(35, __dbResults);
    this.video_cta_clicks = JdbcWritableBridge.readInteger(36, __dbResults);
    this.video_content_starts = JdbcWritableBridge.readInteger(37, __dbResults);
    this.video_mrc_views = JdbcWritableBridge.readInteger(38, __dbResults);
    this.media_views = JdbcWritableBridge.readInteger(39, __dbResults);
    this.product = JdbcWritableBridge.readString(40, __dbResults);
    this.pos = JdbcWritableBridge.readString(41, __dbResults);
    this.mobile_os = JdbcWritableBridge.readString(42, __dbResults);
    this.language = JdbcWritableBridge.readString(43, __dbResults);
    this.sub_channel = JdbcWritableBridge.readString(44, __dbResults);
    this.channel = JdbcWritableBridge.readString(45, __dbResults);
    this.dim_channel_id = JdbcWritableBridge.readString(46, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.accountId = JdbcWritableBridge.readString(1, __dbResults);
    this.accountName = JdbcWritableBridge.readString(2, __dbResults);
    this.consumerKey = JdbcWritableBridge.readString(3, __dbResults);
    this.campaignId = JdbcWritableBridge.readString(4, __dbResults);
    this.campaign_name = JdbcWritableBridge.readString(5, __dbResults);
    this.day_date = JdbcWritableBridge.readString(6, __dbResults);
    this.start_date = JdbcWritableBridge.readString(7, __dbResults);
    this.end_date = JdbcWritableBridge.readString(8, __dbResults);
    this.daily_budget = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.total_budget = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.currency = JdbcWritableBridge.readString(11, __dbResults);
    this.spend = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.impressions = JdbcWritableBridge.readInteger(13, __dbResults);
    this.engagements = JdbcWritableBridge.readInteger(14, __dbResults);
    this.billed_engagements = JdbcWritableBridge.readInteger(15, __dbResults);
    this.retweets = JdbcWritableBridge.readInteger(16, __dbResults);
    this.replies = JdbcWritableBridge.readInteger(17, __dbResults);
    this.follows = JdbcWritableBridge.readInteger(18, __dbResults);
    this.clicks = JdbcWritableBridge.readInteger(19, __dbResults);
    this.media_engagements = JdbcWritableBridge.readInteger(20, __dbResults);
    this.likes = JdbcWritableBridge.readInteger(21, __dbResults);
    this.engagement_rate = JdbcWritableBridge.readDouble(22, __dbResults);
    this.link_clicks = JdbcWritableBridge.readInteger(23, __dbResults);
    this.app_clicks = JdbcWritableBridge.readInteger(24, __dbResults);
    this.cost_per_engagement = JdbcWritableBridge.readBigDecimal(25, __dbResults);
    this.cost_per_follow = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.cost_per_link_click = JdbcWritableBridge.readBigDecimal(27, __dbResults);
    this.card_engagements = JdbcWritableBridge.readInteger(28, __dbResults);
    this.tweets_send = JdbcWritableBridge.readInteger(29, __dbResults);
    this.qualified_impressions = JdbcWritableBridge.readInteger(30, __dbResults);
    this.video_views_25 = JdbcWritableBridge.readInteger(31, __dbResults);
    this.video_views_75 = JdbcWritableBridge.readInteger(32, __dbResults);
    this.video_views_100 = JdbcWritableBridge.readInteger(33, __dbResults);
    this.video_total_views = JdbcWritableBridge.readInteger(34, __dbResults);
    this.video_3s100pct_views = JdbcWritableBridge.readInteger(35, __dbResults);
    this.video_cta_clicks = JdbcWritableBridge.readInteger(36, __dbResults);
    this.video_content_starts = JdbcWritableBridge.readInteger(37, __dbResults);
    this.video_mrc_views = JdbcWritableBridge.readInteger(38, __dbResults);
    this.media_views = JdbcWritableBridge.readInteger(39, __dbResults);
    this.product = JdbcWritableBridge.readString(40, __dbResults);
    this.pos = JdbcWritableBridge.readString(41, __dbResults);
    this.mobile_os = JdbcWritableBridge.readString(42, __dbResults);
    this.language = JdbcWritableBridge.readString(43, __dbResults);
    this.sub_channel = JdbcWritableBridge.readString(44, __dbResults);
    this.channel = JdbcWritableBridge.readString(45, __dbResults);
    this.dim_channel_id = JdbcWritableBridge.readString(46, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(accountId, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(accountName, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(consumerKey, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(campaignId, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(campaign_name, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(day_date, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(start_date, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(end_date, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(daily_budget, 9 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(total_budget, 10 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(currency, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(spend, 12 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeInteger(impressions, 13 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(engagements, 14 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(billed_engagements, 15 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(retweets, 16 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(replies, 17 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(follows, 18 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(clicks, 19 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(media_engagements, 20 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(likes, 21 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeDouble(engagement_rate, 22 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeInteger(link_clicks, 23 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(app_clicks, 24 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(cost_per_engagement, 25 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(cost_per_follow, 26 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(cost_per_link_click, 27 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeInteger(card_engagements, 28 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(tweets_send, 29 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(qualified_impressions, 30 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(video_views_25, 31 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(video_views_75, 32 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(video_views_100, 33 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(video_total_views, 34 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(video_3s100pct_views, 35 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(video_cta_clicks, 36 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(video_content_starts, 37 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(video_mrc_views, 38 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(media_views, 39 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(product, 40 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(pos, 41 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(mobile_os, 42 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(language, 43 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(sub_channel, 44 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(channel, 45 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(dim_channel_id, 46 + __off, 12, __dbStmt);
    return 46;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(accountId, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(accountName, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(consumerKey, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(campaignId, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(campaign_name, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(day_date, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(start_date, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(end_date, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(daily_budget, 9 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(total_budget, 10 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(currency, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(spend, 12 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeInteger(impressions, 13 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(engagements, 14 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(billed_engagements, 15 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(retweets, 16 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(replies, 17 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(follows, 18 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(clicks, 19 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(media_engagements, 20 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(likes, 21 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeDouble(engagement_rate, 22 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeInteger(link_clicks, 23 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(app_clicks, 24 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(cost_per_engagement, 25 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(cost_per_follow, 26 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(cost_per_link_click, 27 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeInteger(card_engagements, 28 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(tweets_send, 29 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(qualified_impressions, 30 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(video_views_25, 31 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(video_views_75, 32 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(video_views_100, 33 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(video_total_views, 34 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(video_3s100pct_views, 35 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(video_cta_clicks, 36 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(video_content_starts, 37 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(video_mrc_views, 38 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(media_views, 39 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(product, 40 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(pos, 41 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(mobile_os, 42 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(language, 43 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(sub_channel, 44 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(channel, 45 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(dim_channel_id, 46 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.accountId = null;
    } else {
    this.accountId = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.accountName = null;
    } else {
    this.accountName = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.consumerKey = null;
    } else {
    this.consumerKey = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.campaignId = null;
    } else {
    this.campaignId = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.campaign_name = null;
    } else {
    this.campaign_name = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.day_date = null;
    } else {
    this.day_date = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.start_date = null;
    } else {
    this.start_date = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.end_date = null;
    } else {
    this.end_date = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.daily_budget = null;
    } else {
    this.daily_budget = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.total_budget = null;
    } else {
    this.total_budget = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.currency = null;
    } else {
    this.currency = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.spend = null;
    } else {
    this.spend = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.impressions = null;
    } else {
    this.impressions = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.engagements = null;
    } else {
    this.engagements = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.billed_engagements = null;
    } else {
    this.billed_engagements = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.retweets = null;
    } else {
    this.retweets = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.replies = null;
    } else {
    this.replies = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.follows = null;
    } else {
    this.follows = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.clicks = null;
    } else {
    this.clicks = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.media_engagements = null;
    } else {
    this.media_engagements = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.likes = null;
    } else {
    this.likes = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.engagement_rate = null;
    } else {
    this.engagement_rate = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.link_clicks = null;
    } else {
    this.link_clicks = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.app_clicks = null;
    } else {
    this.app_clicks = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.cost_per_engagement = null;
    } else {
    this.cost_per_engagement = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.cost_per_follow = null;
    } else {
    this.cost_per_follow = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.cost_per_link_click = null;
    } else {
    this.cost_per_link_click = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.card_engagements = null;
    } else {
    this.card_engagements = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.tweets_send = null;
    } else {
    this.tweets_send = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.qualified_impressions = null;
    } else {
    this.qualified_impressions = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.video_views_25 = null;
    } else {
    this.video_views_25 = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.video_views_75 = null;
    } else {
    this.video_views_75 = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.video_views_100 = null;
    } else {
    this.video_views_100 = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.video_total_views = null;
    } else {
    this.video_total_views = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.video_3s100pct_views = null;
    } else {
    this.video_3s100pct_views = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.video_cta_clicks = null;
    } else {
    this.video_cta_clicks = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.video_content_starts = null;
    } else {
    this.video_content_starts = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.video_mrc_views = null;
    } else {
    this.video_mrc_views = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.media_views = null;
    } else {
    this.media_views = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.product = null;
    } else {
    this.product = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.pos = null;
    } else {
    this.pos = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.mobile_os = null;
    } else {
    this.mobile_os = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.language = null;
    } else {
    this.language = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.sub_channel = null;
    } else {
    this.sub_channel = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.channel = null;
    } else {
    this.channel = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.dim_channel_id = null;
    } else {
    this.dim_channel_id = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.accountId) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, accountId);
    }
    if (null == this.accountName) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, accountName);
    }
    if (null == this.consumerKey) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, consumerKey);
    }
    if (null == this.campaignId) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, campaignId);
    }
    if (null == this.campaign_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, campaign_name);
    }
    if (null == this.day_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, day_date);
    }
    if (null == this.start_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, start_date);
    }
    if (null == this.end_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, end_date);
    }
    if (null == this.daily_budget) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.daily_budget, __dataOut);
    }
    if (null == this.total_budget) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.total_budget, __dataOut);
    }
    if (null == this.currency) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, currency);
    }
    if (null == this.spend) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.spend, __dataOut);
    }
    if (null == this.impressions) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.impressions);
    }
    if (null == this.engagements) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.engagements);
    }
    if (null == this.billed_engagements) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.billed_engagements);
    }
    if (null == this.retweets) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.retweets);
    }
    if (null == this.replies) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.replies);
    }
    if (null == this.follows) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.follows);
    }
    if (null == this.clicks) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.clicks);
    }
    if (null == this.media_engagements) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.media_engagements);
    }
    if (null == this.likes) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.likes);
    }
    if (null == this.engagement_rate) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.engagement_rate);
    }
    if (null == this.link_clicks) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.link_clicks);
    }
    if (null == this.app_clicks) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.app_clicks);
    }
    if (null == this.cost_per_engagement) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.cost_per_engagement, __dataOut);
    }
    if (null == this.cost_per_follow) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.cost_per_follow, __dataOut);
    }
    if (null == this.cost_per_link_click) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.cost_per_link_click, __dataOut);
    }
    if (null == this.card_engagements) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.card_engagements);
    }
    if (null == this.tweets_send) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.tweets_send);
    }
    if (null == this.qualified_impressions) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.qualified_impressions);
    }
    if (null == this.video_views_25) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.video_views_25);
    }
    if (null == this.video_views_75) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.video_views_75);
    }
    if (null == this.video_views_100) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.video_views_100);
    }
    if (null == this.video_total_views) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.video_total_views);
    }
    if (null == this.video_3s100pct_views) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.video_3s100pct_views);
    }
    if (null == this.video_cta_clicks) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.video_cta_clicks);
    }
    if (null == this.video_content_starts) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.video_content_starts);
    }
    if (null == this.video_mrc_views) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.video_mrc_views);
    }
    if (null == this.media_views) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.media_views);
    }
    if (null == this.product) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, product);
    }
    if (null == this.pos) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, pos);
    }
    if (null == this.mobile_os) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, mobile_os);
    }
    if (null == this.language) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, language);
    }
    if (null == this.sub_channel) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, sub_channel);
    }
    if (null == this.channel) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, channel);
    }
    if (null == this.dim_channel_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, dim_channel_id);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.accountId) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, accountId);
    }
    if (null == this.accountName) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, accountName);
    }
    if (null == this.consumerKey) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, consumerKey);
    }
    if (null == this.campaignId) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, campaignId);
    }
    if (null == this.campaign_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, campaign_name);
    }
    if (null == this.day_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, day_date);
    }
    if (null == this.start_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, start_date);
    }
    if (null == this.end_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, end_date);
    }
    if (null == this.daily_budget) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.daily_budget, __dataOut);
    }
    if (null == this.total_budget) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.total_budget, __dataOut);
    }
    if (null == this.currency) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, currency);
    }
    if (null == this.spend) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.spend, __dataOut);
    }
    if (null == this.impressions) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.impressions);
    }
    if (null == this.engagements) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.engagements);
    }
    if (null == this.billed_engagements) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.billed_engagements);
    }
    if (null == this.retweets) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.retweets);
    }
    if (null == this.replies) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.replies);
    }
    if (null == this.follows) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.follows);
    }
    if (null == this.clicks) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.clicks);
    }
    if (null == this.media_engagements) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.media_engagements);
    }
    if (null == this.likes) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.likes);
    }
    if (null == this.engagement_rate) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.engagement_rate);
    }
    if (null == this.link_clicks) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.link_clicks);
    }
    if (null == this.app_clicks) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.app_clicks);
    }
    if (null == this.cost_per_engagement) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.cost_per_engagement, __dataOut);
    }
    if (null == this.cost_per_follow) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.cost_per_follow, __dataOut);
    }
    if (null == this.cost_per_link_click) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.cost_per_link_click, __dataOut);
    }
    if (null == this.card_engagements) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.card_engagements);
    }
    if (null == this.tweets_send) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.tweets_send);
    }
    if (null == this.qualified_impressions) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.qualified_impressions);
    }
    if (null == this.video_views_25) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.video_views_25);
    }
    if (null == this.video_views_75) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.video_views_75);
    }
    if (null == this.video_views_100) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.video_views_100);
    }
    if (null == this.video_total_views) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.video_total_views);
    }
    if (null == this.video_3s100pct_views) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.video_3s100pct_views);
    }
    if (null == this.video_cta_clicks) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.video_cta_clicks);
    }
    if (null == this.video_content_starts) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.video_content_starts);
    }
    if (null == this.video_mrc_views) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.video_mrc_views);
    }
    if (null == this.media_views) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.media_views);
    }
    if (null == this.product) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, product);
    }
    if (null == this.pos) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, pos);
    }
    if (null == this.mobile_os) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, mobile_os);
    }
    if (null == this.language) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, language);
    }
    if (null == this.sub_channel) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, sub_channel);
    }
    if (null == this.channel) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, channel);
    }
    if (null == this.dim_channel_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, dim_channel_id);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(accountId==null?"null":accountId, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(accountName==null?"null":accountName, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(consumerKey==null?"null":consumerKey, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(campaignId==null?"null":campaignId, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(campaign_name==null?"null":campaign_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(day_date==null?"null":day_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(start_date==null?"null":start_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(end_date==null?"null":end_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(daily_budget==null?"null":daily_budget.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(total_budget==null?"null":total_budget.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(currency==null?"null":currency, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(spend==null?"null":spend.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(impressions==null?"null":"" + impressions, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(engagements==null?"null":"" + engagements, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(billed_engagements==null?"null":"" + billed_engagements, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(retweets==null?"null":"" + retweets, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(replies==null?"null":"" + replies, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(follows==null?"null":"" + follows, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(clicks==null?"null":"" + clicks, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(media_engagements==null?"null":"" + media_engagements, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(likes==null?"null":"" + likes, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(engagement_rate==null?"null":"" + engagement_rate, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(link_clicks==null?"null":"" + link_clicks, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(app_clicks==null?"null":"" + app_clicks, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cost_per_engagement==null?"null":cost_per_engagement.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cost_per_follow==null?"null":cost_per_follow.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cost_per_link_click==null?"null":cost_per_link_click.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(card_engagements==null?"null":"" + card_engagements, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(tweets_send==null?"null":"" + tweets_send, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(qualified_impressions==null?"null":"" + qualified_impressions, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(video_views_25==null?"null":"" + video_views_25, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(video_views_75==null?"null":"" + video_views_75, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(video_views_100==null?"null":"" + video_views_100, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(video_total_views==null?"null":"" + video_total_views, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(video_3s100pct_views==null?"null":"" + video_3s100pct_views, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(video_cta_clicks==null?"null":"" + video_cta_clicks, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(video_content_starts==null?"null":"" + video_content_starts, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(video_mrc_views==null?"null":"" + video_mrc_views, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(media_views==null?"null":"" + media_views, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(product==null?"null":product, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(pos==null?"null":pos, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(mobile_os==null?"null":mobile_os, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(language==null?"null":language, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(sub_channel==null?"null":sub_channel, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(channel==null?"null":channel, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dim_channel_id==null?"null":dim_channel_id, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(accountId==null?"null":accountId, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(accountName==null?"null":accountName, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(consumerKey==null?"null":consumerKey, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(campaignId==null?"null":campaignId, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(campaign_name==null?"null":campaign_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(day_date==null?"null":day_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(start_date==null?"null":start_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(end_date==null?"null":end_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(daily_budget==null?"null":daily_budget.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(total_budget==null?"null":total_budget.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(currency==null?"null":currency, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(spend==null?"null":spend.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(impressions==null?"null":"" + impressions, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(engagements==null?"null":"" + engagements, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(billed_engagements==null?"null":"" + billed_engagements, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(retweets==null?"null":"" + retweets, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(replies==null?"null":"" + replies, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(follows==null?"null":"" + follows, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(clicks==null?"null":"" + clicks, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(media_engagements==null?"null":"" + media_engagements, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(likes==null?"null":"" + likes, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(engagement_rate==null?"null":"" + engagement_rate, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(link_clicks==null?"null":"" + link_clicks, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(app_clicks==null?"null":"" + app_clicks, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cost_per_engagement==null?"null":cost_per_engagement.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cost_per_follow==null?"null":cost_per_follow.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cost_per_link_click==null?"null":cost_per_link_click.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(card_engagements==null?"null":"" + card_engagements, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(tweets_send==null?"null":"" + tweets_send, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(qualified_impressions==null?"null":"" + qualified_impressions, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(video_views_25==null?"null":"" + video_views_25, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(video_views_75==null?"null":"" + video_views_75, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(video_views_100==null?"null":"" + video_views_100, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(video_total_views==null?"null":"" + video_total_views, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(video_3s100pct_views==null?"null":"" + video_3s100pct_views, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(video_cta_clicks==null?"null":"" + video_cta_clicks, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(video_content_starts==null?"null":"" + video_content_starts, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(video_mrc_views==null?"null":"" + video_mrc_views, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(media_views==null?"null":"" + media_views, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(product==null?"null":product, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(pos==null?"null":pos, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(mobile_os==null?"null":mobile_os, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(language==null?"null":language, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(sub_channel==null?"null":sub_channel, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(channel==null?"null":channel, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dim_channel_id==null?"null":dim_channel_id, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 124, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.accountId = null; } else {
      this.accountId = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.accountName = null; } else {
      this.accountName = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.consumerKey = null; } else {
      this.consumerKey = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.campaignId = null; } else {
      this.campaignId = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.campaign_name = null; } else {
      this.campaign_name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.day_date = null; } else {
      this.day_date = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.start_date = null; } else {
      this.start_date = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.end_date = null; } else {
      this.end_date = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.daily_budget = null; } else {
      this.daily_budget = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.total_budget = null; } else {
      this.total_budget = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.currency = null; } else {
      this.currency = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.spend = null; } else {
      this.spend = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.impressions = null; } else {
      this.impressions = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.engagements = null; } else {
      this.engagements = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.billed_engagements = null; } else {
      this.billed_engagements = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.retweets = null; } else {
      this.retweets = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.replies = null; } else {
      this.replies = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.follows = null; } else {
      this.follows = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.clicks = null; } else {
      this.clicks = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.media_engagements = null; } else {
      this.media_engagements = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.likes = null; } else {
      this.likes = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.engagement_rate = null; } else {
      this.engagement_rate = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.link_clicks = null; } else {
      this.link_clicks = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.app_clicks = null; } else {
      this.app_clicks = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.cost_per_engagement = null; } else {
      this.cost_per_engagement = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.cost_per_follow = null; } else {
      this.cost_per_follow = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.cost_per_link_click = null; } else {
      this.cost_per_link_click = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.card_engagements = null; } else {
      this.card_engagements = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.tweets_send = null; } else {
      this.tweets_send = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.qualified_impressions = null; } else {
      this.qualified_impressions = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.video_views_25 = null; } else {
      this.video_views_25 = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.video_views_75 = null; } else {
      this.video_views_75 = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.video_views_100 = null; } else {
      this.video_views_100 = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.video_total_views = null; } else {
      this.video_total_views = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.video_3s100pct_views = null; } else {
      this.video_3s100pct_views = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.video_cta_clicks = null; } else {
      this.video_cta_clicks = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.video_content_starts = null; } else {
      this.video_content_starts = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.video_mrc_views = null; } else {
      this.video_mrc_views = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.media_views = null; } else {
      this.media_views = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.product = null; } else {
      this.product = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.pos = null; } else {
      this.pos = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.mobile_os = null; } else {
      this.mobile_os = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.language = null; } else {
      this.language = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.sub_channel = null; } else {
      this.sub_channel = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.channel = null; } else {
      this.channel = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.dim_channel_id = null; } else {
      this.dim_channel_id = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.accountId = null; } else {
      this.accountId = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.accountName = null; } else {
      this.accountName = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.consumerKey = null; } else {
      this.consumerKey = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.campaignId = null; } else {
      this.campaignId = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.campaign_name = null; } else {
      this.campaign_name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.day_date = null; } else {
      this.day_date = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.start_date = null; } else {
      this.start_date = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.end_date = null; } else {
      this.end_date = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.daily_budget = null; } else {
      this.daily_budget = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.total_budget = null; } else {
      this.total_budget = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.currency = null; } else {
      this.currency = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.spend = null; } else {
      this.spend = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.impressions = null; } else {
      this.impressions = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.engagements = null; } else {
      this.engagements = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.billed_engagements = null; } else {
      this.billed_engagements = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.retweets = null; } else {
      this.retweets = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.replies = null; } else {
      this.replies = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.follows = null; } else {
      this.follows = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.clicks = null; } else {
      this.clicks = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.media_engagements = null; } else {
      this.media_engagements = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.likes = null; } else {
      this.likes = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.engagement_rate = null; } else {
      this.engagement_rate = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.link_clicks = null; } else {
      this.link_clicks = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.app_clicks = null; } else {
      this.app_clicks = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.cost_per_engagement = null; } else {
      this.cost_per_engagement = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.cost_per_follow = null; } else {
      this.cost_per_follow = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.cost_per_link_click = null; } else {
      this.cost_per_link_click = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.card_engagements = null; } else {
      this.card_engagements = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.tweets_send = null; } else {
      this.tweets_send = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.qualified_impressions = null; } else {
      this.qualified_impressions = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.video_views_25 = null; } else {
      this.video_views_25 = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.video_views_75 = null; } else {
      this.video_views_75 = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.video_views_100 = null; } else {
      this.video_views_100 = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.video_total_views = null; } else {
      this.video_total_views = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.video_3s100pct_views = null; } else {
      this.video_3s100pct_views = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.video_cta_clicks = null; } else {
      this.video_cta_clicks = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.video_content_starts = null; } else {
      this.video_content_starts = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.video_mrc_views = null; } else {
      this.video_mrc_views = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.media_views = null; } else {
      this.media_views = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.product = null; } else {
      this.product = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.pos = null; } else {
      this.pos = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.mobile_os = null; } else {
      this.mobile_os = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.language = null; } else {
      this.language = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.sub_channel = null; } else {
      this.sub_channel = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.channel = null; } else {
      this.channel = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.dim_channel_id = null; } else {
      this.dim_channel_id = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    twitter_data_final o = (twitter_data_final) super.clone();
    return o;
  }

  public void clone0(twitter_data_final o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
    __sqoop$field_map.put("accountId", this.accountId);
    __sqoop$field_map.put("accountName", this.accountName);
    __sqoop$field_map.put("consumerKey", this.consumerKey);
    __sqoop$field_map.put("campaignId", this.campaignId);
    __sqoop$field_map.put("campaign_name", this.campaign_name);
    __sqoop$field_map.put("day_date", this.day_date);
    __sqoop$field_map.put("start_date", this.start_date);
    __sqoop$field_map.put("end_date", this.end_date);
    __sqoop$field_map.put("daily_budget", this.daily_budget);
    __sqoop$field_map.put("total_budget", this.total_budget);
    __sqoop$field_map.put("currency", this.currency);
    __sqoop$field_map.put("spend", this.spend);
    __sqoop$field_map.put("impressions", this.impressions);
    __sqoop$field_map.put("engagements", this.engagements);
    __sqoop$field_map.put("billed_engagements", this.billed_engagements);
    __sqoop$field_map.put("retweets", this.retweets);
    __sqoop$field_map.put("replies", this.replies);
    __sqoop$field_map.put("follows", this.follows);
    __sqoop$field_map.put("clicks", this.clicks);
    __sqoop$field_map.put("media_engagements", this.media_engagements);
    __sqoop$field_map.put("likes", this.likes);
    __sqoop$field_map.put("engagement_rate", this.engagement_rate);
    __sqoop$field_map.put("link_clicks", this.link_clicks);
    __sqoop$field_map.put("app_clicks", this.app_clicks);
    __sqoop$field_map.put("cost_per_engagement", this.cost_per_engagement);
    __sqoop$field_map.put("cost_per_follow", this.cost_per_follow);
    __sqoop$field_map.put("cost_per_link_click", this.cost_per_link_click);
    __sqoop$field_map.put("card_engagements", this.card_engagements);
    __sqoop$field_map.put("tweets_send", this.tweets_send);
    __sqoop$field_map.put("qualified_impressions", this.qualified_impressions);
    __sqoop$field_map.put("video_views_25", this.video_views_25);
    __sqoop$field_map.put("video_views_75", this.video_views_75);
    __sqoop$field_map.put("video_views_100", this.video_views_100);
    __sqoop$field_map.put("video_total_views", this.video_total_views);
    __sqoop$field_map.put("video_3s100pct_views", this.video_3s100pct_views);
    __sqoop$field_map.put("video_cta_clicks", this.video_cta_clicks);
    __sqoop$field_map.put("video_content_starts", this.video_content_starts);
    __sqoop$field_map.put("video_mrc_views", this.video_mrc_views);
    __sqoop$field_map.put("media_views", this.media_views);
    __sqoop$field_map.put("product", this.product);
    __sqoop$field_map.put("pos", this.pos);
    __sqoop$field_map.put("mobile_os", this.mobile_os);
    __sqoop$field_map.put("language", this.language);
    __sqoop$field_map.put("sub_channel", this.sub_channel);
    __sqoop$field_map.put("channel", this.channel);
    __sqoop$field_map.put("dim_channel_id", this.dim_channel_id);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("accountId", this.accountId);
    __sqoop$field_map.put("accountName", this.accountName);
    __sqoop$field_map.put("consumerKey", this.consumerKey);
    __sqoop$field_map.put("campaignId", this.campaignId);
    __sqoop$field_map.put("campaign_name", this.campaign_name);
    __sqoop$field_map.put("day_date", this.day_date);
    __sqoop$field_map.put("start_date", this.start_date);
    __sqoop$field_map.put("end_date", this.end_date);
    __sqoop$field_map.put("daily_budget", this.daily_budget);
    __sqoop$field_map.put("total_budget", this.total_budget);
    __sqoop$field_map.put("currency", this.currency);
    __sqoop$field_map.put("spend", this.spend);
    __sqoop$field_map.put("impressions", this.impressions);
    __sqoop$field_map.put("engagements", this.engagements);
    __sqoop$field_map.put("billed_engagements", this.billed_engagements);
    __sqoop$field_map.put("retweets", this.retweets);
    __sqoop$field_map.put("replies", this.replies);
    __sqoop$field_map.put("follows", this.follows);
    __sqoop$field_map.put("clicks", this.clicks);
    __sqoop$field_map.put("media_engagements", this.media_engagements);
    __sqoop$field_map.put("likes", this.likes);
    __sqoop$field_map.put("engagement_rate", this.engagement_rate);
    __sqoop$field_map.put("link_clicks", this.link_clicks);
    __sqoop$field_map.put("app_clicks", this.app_clicks);
    __sqoop$field_map.put("cost_per_engagement", this.cost_per_engagement);
    __sqoop$field_map.put("cost_per_follow", this.cost_per_follow);
    __sqoop$field_map.put("cost_per_link_click", this.cost_per_link_click);
    __sqoop$field_map.put("card_engagements", this.card_engagements);
    __sqoop$field_map.put("tweets_send", this.tweets_send);
    __sqoop$field_map.put("qualified_impressions", this.qualified_impressions);
    __sqoop$field_map.put("video_views_25", this.video_views_25);
    __sqoop$field_map.put("video_views_75", this.video_views_75);
    __sqoop$field_map.put("video_views_100", this.video_views_100);
    __sqoop$field_map.put("video_total_views", this.video_total_views);
    __sqoop$field_map.put("video_3s100pct_views", this.video_3s100pct_views);
    __sqoop$field_map.put("video_cta_clicks", this.video_cta_clicks);
    __sqoop$field_map.put("video_content_starts", this.video_content_starts);
    __sqoop$field_map.put("video_mrc_views", this.video_mrc_views);
    __sqoop$field_map.put("media_views", this.media_views);
    __sqoop$field_map.put("product", this.product);
    __sqoop$field_map.put("pos", this.pos);
    __sqoop$field_map.put("mobile_os", this.mobile_os);
    __sqoop$field_map.put("language", this.language);
    __sqoop$field_map.put("sub_channel", this.sub_channel);
    __sqoop$field_map.put("channel", this.channel);
    __sqoop$field_map.put("dim_channel_id", this.dim_channel_id);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if ("accountId".equals(__fieldName)) {
      this.accountId = (String) __fieldVal;
    }
    else    if ("accountName".equals(__fieldName)) {
      this.accountName = (String) __fieldVal;
    }
    else    if ("consumerKey".equals(__fieldName)) {
      this.consumerKey = (String) __fieldVal;
    }
    else    if ("campaignId".equals(__fieldName)) {
      this.campaignId = (String) __fieldVal;
    }
    else    if ("campaign_name".equals(__fieldName)) {
      this.campaign_name = (String) __fieldVal;
    }
    else    if ("day_date".equals(__fieldName)) {
      this.day_date = (String) __fieldVal;
    }
    else    if ("start_date".equals(__fieldName)) {
      this.start_date = (String) __fieldVal;
    }
    else    if ("end_date".equals(__fieldName)) {
      this.end_date = (String) __fieldVal;
    }
    else    if ("daily_budget".equals(__fieldName)) {
      this.daily_budget = (java.math.BigDecimal) __fieldVal;
    }
    else    if ("total_budget".equals(__fieldName)) {
      this.total_budget = (java.math.BigDecimal) __fieldVal;
    }
    else    if ("currency".equals(__fieldName)) {
      this.currency = (String) __fieldVal;
    }
    else    if ("spend".equals(__fieldName)) {
      this.spend = (java.math.BigDecimal) __fieldVal;
    }
    else    if ("impressions".equals(__fieldName)) {
      this.impressions = (Integer) __fieldVal;
    }
    else    if ("engagements".equals(__fieldName)) {
      this.engagements = (Integer) __fieldVal;
    }
    else    if ("billed_engagements".equals(__fieldName)) {
      this.billed_engagements = (Integer) __fieldVal;
    }
    else    if ("retweets".equals(__fieldName)) {
      this.retweets = (Integer) __fieldVal;
    }
    else    if ("replies".equals(__fieldName)) {
      this.replies = (Integer) __fieldVal;
    }
    else    if ("follows".equals(__fieldName)) {
      this.follows = (Integer) __fieldVal;
    }
    else    if ("clicks".equals(__fieldName)) {
      this.clicks = (Integer) __fieldVal;
    }
    else    if ("media_engagements".equals(__fieldName)) {
      this.media_engagements = (Integer) __fieldVal;
    }
    else    if ("likes".equals(__fieldName)) {
      this.likes = (Integer) __fieldVal;
    }
    else    if ("engagement_rate".equals(__fieldName)) {
      this.engagement_rate = (Double) __fieldVal;
    }
    else    if ("link_clicks".equals(__fieldName)) {
      this.link_clicks = (Integer) __fieldVal;
    }
    else    if ("app_clicks".equals(__fieldName)) {
      this.app_clicks = (Integer) __fieldVal;
    }
    else    if ("cost_per_engagement".equals(__fieldName)) {
      this.cost_per_engagement = (java.math.BigDecimal) __fieldVal;
    }
    else    if ("cost_per_follow".equals(__fieldName)) {
      this.cost_per_follow = (java.math.BigDecimal) __fieldVal;
    }
    else    if ("cost_per_link_click".equals(__fieldName)) {
      this.cost_per_link_click = (java.math.BigDecimal) __fieldVal;
    }
    else    if ("card_engagements".equals(__fieldName)) {
      this.card_engagements = (Integer) __fieldVal;
    }
    else    if ("tweets_send".equals(__fieldName)) {
      this.tweets_send = (Integer) __fieldVal;
    }
    else    if ("qualified_impressions".equals(__fieldName)) {
      this.qualified_impressions = (Integer) __fieldVal;
    }
    else    if ("video_views_25".equals(__fieldName)) {
      this.video_views_25 = (Integer) __fieldVal;
    }
    else    if ("video_views_75".equals(__fieldName)) {
      this.video_views_75 = (Integer) __fieldVal;
    }
    else    if ("video_views_100".equals(__fieldName)) {
      this.video_views_100 = (Integer) __fieldVal;
    }
    else    if ("video_total_views".equals(__fieldName)) {
      this.video_total_views = (Integer) __fieldVal;
    }
    else    if ("video_3s100pct_views".equals(__fieldName)) {
      this.video_3s100pct_views = (Integer) __fieldVal;
    }
    else    if ("video_cta_clicks".equals(__fieldName)) {
      this.video_cta_clicks = (Integer) __fieldVal;
    }
    else    if ("video_content_starts".equals(__fieldName)) {
      this.video_content_starts = (Integer) __fieldVal;
    }
    else    if ("video_mrc_views".equals(__fieldName)) {
      this.video_mrc_views = (Integer) __fieldVal;
    }
    else    if ("media_views".equals(__fieldName)) {
      this.media_views = (Integer) __fieldVal;
    }
    else    if ("product".equals(__fieldName)) {
      this.product = (String) __fieldVal;
    }
    else    if ("pos".equals(__fieldName)) {
      this.pos = (String) __fieldVal;
    }
    else    if ("mobile_os".equals(__fieldName)) {
      this.mobile_os = (String) __fieldVal;
    }
    else    if ("language".equals(__fieldName)) {
      this.language = (String) __fieldVal;
    }
    else    if ("sub_channel".equals(__fieldName)) {
      this.sub_channel = (String) __fieldVal;
    }
    else    if ("channel".equals(__fieldName)) {
      this.channel = (String) __fieldVal;
    }
    else    if ("dim_channel_id".equals(__fieldName)) {
      this.dim_channel_id = (String) __fieldVal;
    }
    else {
      throw new RuntimeException("No such field: " + __fieldName);
    }
  }
  public boolean setField0(String __fieldName, Object __fieldVal) {
    if ("accountId".equals(__fieldName)) {
      this.accountId = (String) __fieldVal;
      return true;
    }
    else    if ("accountName".equals(__fieldName)) {
      this.accountName = (String) __fieldVal;
      return true;
    }
    else    if ("consumerKey".equals(__fieldName)) {
      this.consumerKey = (String) __fieldVal;
      return true;
    }
    else    if ("campaignId".equals(__fieldName)) {
      this.campaignId = (String) __fieldVal;
      return true;
    }
    else    if ("campaign_name".equals(__fieldName)) {
      this.campaign_name = (String) __fieldVal;
      return true;
    }
    else    if ("day_date".equals(__fieldName)) {
      this.day_date = (String) __fieldVal;
      return true;
    }
    else    if ("start_date".equals(__fieldName)) {
      this.start_date = (String) __fieldVal;
      return true;
    }
    else    if ("end_date".equals(__fieldName)) {
      this.end_date = (String) __fieldVal;
      return true;
    }
    else    if ("daily_budget".equals(__fieldName)) {
      this.daily_budget = (java.math.BigDecimal) __fieldVal;
      return true;
    }
    else    if ("total_budget".equals(__fieldName)) {
      this.total_budget = (java.math.BigDecimal) __fieldVal;
      return true;
    }
    else    if ("currency".equals(__fieldName)) {
      this.currency = (String) __fieldVal;
      return true;
    }
    else    if ("spend".equals(__fieldName)) {
      this.spend = (java.math.BigDecimal) __fieldVal;
      return true;
    }
    else    if ("impressions".equals(__fieldName)) {
      this.impressions = (Integer) __fieldVal;
      return true;
    }
    else    if ("engagements".equals(__fieldName)) {
      this.engagements = (Integer) __fieldVal;
      return true;
    }
    else    if ("billed_engagements".equals(__fieldName)) {
      this.billed_engagements = (Integer) __fieldVal;
      return true;
    }
    else    if ("retweets".equals(__fieldName)) {
      this.retweets = (Integer) __fieldVal;
      return true;
    }
    else    if ("replies".equals(__fieldName)) {
      this.replies = (Integer) __fieldVal;
      return true;
    }
    else    if ("follows".equals(__fieldName)) {
      this.follows = (Integer) __fieldVal;
      return true;
    }
    else    if ("clicks".equals(__fieldName)) {
      this.clicks = (Integer) __fieldVal;
      return true;
    }
    else    if ("media_engagements".equals(__fieldName)) {
      this.media_engagements = (Integer) __fieldVal;
      return true;
    }
    else    if ("likes".equals(__fieldName)) {
      this.likes = (Integer) __fieldVal;
      return true;
    }
    else    if ("engagement_rate".equals(__fieldName)) {
      this.engagement_rate = (Double) __fieldVal;
      return true;
    }
    else    if ("link_clicks".equals(__fieldName)) {
      this.link_clicks = (Integer) __fieldVal;
      return true;
    }
    else    if ("app_clicks".equals(__fieldName)) {
      this.app_clicks = (Integer) __fieldVal;
      return true;
    }
    else    if ("cost_per_engagement".equals(__fieldName)) {
      this.cost_per_engagement = (java.math.BigDecimal) __fieldVal;
      return true;
    }
    else    if ("cost_per_follow".equals(__fieldName)) {
      this.cost_per_follow = (java.math.BigDecimal) __fieldVal;
      return true;
    }
    else    if ("cost_per_link_click".equals(__fieldName)) {
      this.cost_per_link_click = (java.math.BigDecimal) __fieldVal;
      return true;
    }
    else    if ("card_engagements".equals(__fieldName)) {
      this.card_engagements = (Integer) __fieldVal;
      return true;
    }
    else    if ("tweets_send".equals(__fieldName)) {
      this.tweets_send = (Integer) __fieldVal;
      return true;
    }
    else    if ("qualified_impressions".equals(__fieldName)) {
      this.qualified_impressions = (Integer) __fieldVal;
      return true;
    }
    else    if ("video_views_25".equals(__fieldName)) {
      this.video_views_25 = (Integer) __fieldVal;
      return true;
    }
    else    if ("video_views_75".equals(__fieldName)) {
      this.video_views_75 = (Integer) __fieldVal;
      return true;
    }
    else    if ("video_views_100".equals(__fieldName)) {
      this.video_views_100 = (Integer) __fieldVal;
      return true;
    }
    else    if ("video_total_views".equals(__fieldName)) {
      this.video_total_views = (Integer) __fieldVal;
      return true;
    }
    else    if ("video_3s100pct_views".equals(__fieldName)) {
      this.video_3s100pct_views = (Integer) __fieldVal;
      return true;
    }
    else    if ("video_cta_clicks".equals(__fieldName)) {
      this.video_cta_clicks = (Integer) __fieldVal;
      return true;
    }
    else    if ("video_content_starts".equals(__fieldName)) {
      this.video_content_starts = (Integer) __fieldVal;
      return true;
    }
    else    if ("video_mrc_views".equals(__fieldName)) {
      this.video_mrc_views = (Integer) __fieldVal;
      return true;
    }
    else    if ("media_views".equals(__fieldName)) {
      this.media_views = (Integer) __fieldVal;
      return true;
    }
    else    if ("product".equals(__fieldName)) {
      this.product = (String) __fieldVal;
      return true;
    }
    else    if ("pos".equals(__fieldName)) {
      this.pos = (String) __fieldVal;
      return true;
    }
    else    if ("mobile_os".equals(__fieldName)) {
      this.mobile_os = (String) __fieldVal;
      return true;
    }
    else    if ("language".equals(__fieldName)) {
      this.language = (String) __fieldVal;
      return true;
    }
    else    if ("sub_channel".equals(__fieldName)) {
      this.sub_channel = (String) __fieldVal;
      return true;
    }
    else    if ("channel".equals(__fieldName)) {
      this.channel = (String) __fieldVal;
      return true;
    }
    else    if ("dim_channel_id".equals(__fieldName)) {
      this.dim_channel_id = (String) __fieldVal;
      return true;
    }
    else {
      return false;    }
  }
}
