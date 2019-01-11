// ORM class for table 'twitter_data_final'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Wed Sep 13 12:50:09 UTC 2017
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
  private java.sql.Date day_date;
  public java.sql.Date get_day_date() {
    return day_date;
  }
  public void set_day_date(java.sql.Date day_date) {
    this.day_date = day_date;
  }
  public twitter_data_final with_day_date(java.sql.Date day_date) {
    this.day_date = day_date;
    return this;
  }
  private java.sql.Date start_date;
  public java.sql.Date get_start_date() {
    return start_date;
  }
  public void set_start_date(java.sql.Date start_date) {
    this.start_date = start_date;
  }
  public twitter_data_final with_start_date(java.sql.Date start_date) {
    this.start_date = start_date;
    return this;
  }
  private java.sql.Date end_date;
  public java.sql.Date get_end_date() {
    return end_date;
  }
  public void set_end_date(java.sql.Date end_date) {
    this.end_date = end_date;
  }
  public twitter_data_final with_end_date(java.sql.Date end_date) {
    this.end_date = end_date;
    return this;
  }
  private Double spend;
  public Double get_spend() {
    return spend;
  }
  public void set_spend(Double spend) {
    this.spend = spend;
  }
  public twitter_data_final with_spend(Double spend) {
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
  private Double cost_per_engagement;
  public Double get_cost_per_engagement() {
    return cost_per_engagement;
  }
  public void set_cost_per_engagement(Double cost_per_engagement) {
    this.cost_per_engagement = cost_per_engagement;
  }
  public twitter_data_final with_cost_per_engagement(Double cost_per_engagement) {
    this.cost_per_engagement = cost_per_engagement;
    return this;
  }
  private Double cost_per_follow;
  public Double get_cost_per_follow() {
    return cost_per_follow;
  }
  public void set_cost_per_follow(Double cost_per_follow) {
    this.cost_per_follow = cost_per_follow;
  }
  public twitter_data_final with_cost_per_follow(Double cost_per_follow) {
    this.cost_per_follow = cost_per_follow;
    return this;
  }
  private Double cost_per_link_click;
  public Double get_cost_per_link_click() {
    return cost_per_link_click;
  }
  public void set_cost_per_link_click(Double cost_per_link_click) {
    this.cost_per_link_click = cost_per_link_click;
  }
  public twitter_data_final with_cost_per_link_click(Double cost_per_link_click) {
    this.cost_per_link_click = cost_per_link_click;
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
    equal = equal && (this.campaignId == null ? that.campaignId == null : this.campaignId.equals(that.campaignId));
    equal = equal && (this.campaign_name == null ? that.campaign_name == null : this.campaign_name.equals(that.campaign_name));
    equal = equal && (this.day_date == null ? that.day_date == null : this.day_date.equals(that.day_date));
    equal = equal && (this.start_date == null ? that.start_date == null : this.start_date.equals(that.start_date));
    equal = equal && (this.end_date == null ? that.end_date == null : this.end_date.equals(that.end_date));
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
    equal = equal && (this.campaignId == null ? that.campaignId == null : this.campaignId.equals(that.campaignId));
    equal = equal && (this.campaign_name == null ? that.campaign_name == null : this.campaign_name.equals(that.campaign_name));
    equal = equal && (this.day_date == null ? that.day_date == null : this.day_date.equals(that.day_date));
    equal = equal && (this.start_date == null ? that.start_date == null : this.start_date.equals(that.start_date));
    equal = equal && (this.end_date == null ? that.end_date == null : this.end_date.equals(that.end_date));
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
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.campaignId = JdbcWritableBridge.readString(1, __dbResults);
    this.campaign_name = JdbcWritableBridge.readString(2, __dbResults);
    this.day_date = JdbcWritableBridge.readDate(3, __dbResults);
    this.start_date = JdbcWritableBridge.readDate(4, __dbResults);
    this.end_date = JdbcWritableBridge.readDate(5, __dbResults);
    this.spend = JdbcWritableBridge.readDouble(6, __dbResults);
    this.impressions = JdbcWritableBridge.readInteger(7, __dbResults);
    this.engagements = JdbcWritableBridge.readInteger(8, __dbResults);
    this.billed_engagements = JdbcWritableBridge.readInteger(9, __dbResults);
    this.retweets = JdbcWritableBridge.readInteger(10, __dbResults);
    this.replies = JdbcWritableBridge.readInteger(11, __dbResults);
    this.follows = JdbcWritableBridge.readInteger(12, __dbResults);
    this.clicks = JdbcWritableBridge.readInteger(13, __dbResults);
    this.media_engagements = JdbcWritableBridge.readInteger(14, __dbResults);
    this.likes = JdbcWritableBridge.readInteger(15, __dbResults);
    this.engagement_rate = JdbcWritableBridge.readDouble(16, __dbResults);
    this.link_clicks = JdbcWritableBridge.readInteger(17, __dbResults);
    this.app_clicks = JdbcWritableBridge.readInteger(18, __dbResults);
    this.cost_per_engagement = JdbcWritableBridge.readDouble(19, __dbResults);
    this.cost_per_follow = JdbcWritableBridge.readDouble(20, __dbResults);
    this.cost_per_link_click = JdbcWritableBridge.readDouble(21, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.campaignId = JdbcWritableBridge.readString(1, __dbResults);
    this.campaign_name = JdbcWritableBridge.readString(2, __dbResults);
    this.day_date = JdbcWritableBridge.readDate(3, __dbResults);
    this.start_date = JdbcWritableBridge.readDate(4, __dbResults);
    this.end_date = JdbcWritableBridge.readDate(5, __dbResults);
    this.spend = JdbcWritableBridge.readDouble(6, __dbResults);
    this.impressions = JdbcWritableBridge.readInteger(7, __dbResults);
    this.engagements = JdbcWritableBridge.readInteger(8, __dbResults);
    this.billed_engagements = JdbcWritableBridge.readInteger(9, __dbResults);
    this.retweets = JdbcWritableBridge.readInteger(10, __dbResults);
    this.replies = JdbcWritableBridge.readInteger(11, __dbResults);
    this.follows = JdbcWritableBridge.readInteger(12, __dbResults);
    this.clicks = JdbcWritableBridge.readInteger(13, __dbResults);
    this.media_engagements = JdbcWritableBridge.readInteger(14, __dbResults);
    this.likes = JdbcWritableBridge.readInteger(15, __dbResults);
    this.engagement_rate = JdbcWritableBridge.readDouble(16, __dbResults);
    this.link_clicks = JdbcWritableBridge.readInteger(17, __dbResults);
    this.app_clicks = JdbcWritableBridge.readInteger(18, __dbResults);
    this.cost_per_engagement = JdbcWritableBridge.readDouble(19, __dbResults);
    this.cost_per_follow = JdbcWritableBridge.readDouble(20, __dbResults);
    this.cost_per_link_click = JdbcWritableBridge.readDouble(21, __dbResults);
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
    JdbcWritableBridge.writeString(campaignId, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(campaign_name, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDate(day_date, 3 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDate(start_date, 4 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDate(end_date, 5 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDouble(spend, 6 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeInteger(impressions, 7 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(engagements, 8 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(billed_engagements, 9 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(retweets, 10 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(replies, 11 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(follows, 12 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(clicks, 13 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(media_engagements, 14 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(likes, 15 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeDouble(engagement_rate, 16 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeInteger(link_clicks, 17 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(app_clicks, 18 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeDouble(cost_per_engagement, 19 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(cost_per_follow, 20 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(cost_per_link_click, 21 + __off, 8, __dbStmt);
    return 21;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(campaignId, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(campaign_name, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDate(day_date, 3 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDate(start_date, 4 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDate(end_date, 5 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDouble(spend, 6 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeInteger(impressions, 7 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(engagements, 8 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(billed_engagements, 9 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(retweets, 10 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(replies, 11 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(follows, 12 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(clicks, 13 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(media_engagements, 14 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(likes, 15 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeDouble(engagement_rate, 16 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeInteger(link_clicks, 17 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(app_clicks, 18 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeDouble(cost_per_engagement, 19 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(cost_per_follow, 20 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(cost_per_link_click, 21 + __off, 8, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
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
    this.day_date = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.start_date = null;
    } else {
    this.start_date = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.end_date = null;
    } else {
    this.end_date = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.spend = null;
    } else {
    this.spend = Double.valueOf(__dataIn.readDouble());
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
    this.cost_per_engagement = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.cost_per_follow = null;
    } else {
    this.cost_per_follow = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.cost_per_link_click = null;
    } else {
    this.cost_per_link_click = Double.valueOf(__dataIn.readDouble());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
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
    __dataOut.writeLong(this.day_date.getTime());
    }
    if (null == this.start_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.start_date.getTime());
    }
    if (null == this.end_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.end_date.getTime());
    }
    if (null == this.spend) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.spend);
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
    __dataOut.writeDouble(this.cost_per_engagement);
    }
    if (null == this.cost_per_follow) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.cost_per_follow);
    }
    if (null == this.cost_per_link_click) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.cost_per_link_click);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
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
    __dataOut.writeLong(this.day_date.getTime());
    }
    if (null == this.start_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.start_date.getTime());
    }
    if (null == this.end_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.end_date.getTime());
    }
    if (null == this.spend) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.spend);
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
    __dataOut.writeDouble(this.cost_per_engagement);
    }
    if (null == this.cost_per_follow) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.cost_per_follow);
    }
    if (null == this.cost_per_link_click) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.cost_per_link_click);
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
    __sb.append(FieldFormatter.escapeAndEnclose(campaignId==null?"null":campaignId, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(campaign_name==null?"null":campaign_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(day_date==null?"null":"" + day_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(start_date==null?"null":"" + start_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(end_date==null?"null":"" + end_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(spend==null?"null":"" + spend, delimiters));
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
    __sb.append(FieldFormatter.escapeAndEnclose(cost_per_engagement==null?"null":"" + cost_per_engagement, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cost_per_follow==null?"null":"" + cost_per_follow, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cost_per_link_click==null?"null":"" + cost_per_link_click, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(campaignId==null?"null":campaignId, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(campaign_name==null?"null":campaign_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(day_date==null?"null":"" + day_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(start_date==null?"null":"" + start_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(end_date==null?"null":"" + end_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(spend==null?"null":"" + spend, delimiters));
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
    __sb.append(FieldFormatter.escapeAndEnclose(cost_per_engagement==null?"null":"" + cost_per_engagement, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cost_per_follow==null?"null":"" + cost_per_follow, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cost_per_link_click==null?"null":"" + cost_per_link_click, delimiters));
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
    if (__cur_str.equals("NULL")) { this.campaignId = null; } else {
      this.campaignId = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.campaign_name = null; } else {
      this.campaign_name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.day_date = null; } else {
      this.day_date = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.start_date = null; } else {
      this.start_date = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.end_date = null; } else {
      this.end_date = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.spend = null; } else {
      this.spend = Double.valueOf(__cur_str);
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
      this.cost_per_engagement = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.cost_per_follow = null; } else {
      this.cost_per_follow = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.cost_per_link_click = null; } else {
      this.cost_per_link_click = Double.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.campaignId = null; } else {
      this.campaignId = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.campaign_name = null; } else {
      this.campaign_name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.day_date = null; } else {
      this.day_date = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.start_date = null; } else {
      this.start_date = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.end_date = null; } else {
      this.end_date = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.spend = null; } else {
      this.spend = Double.valueOf(__cur_str);
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
      this.cost_per_engagement = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.cost_per_follow = null; } else {
      this.cost_per_follow = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL") || __cur_str.length() == 0) { this.cost_per_link_click = null; } else {
      this.cost_per_link_click = Double.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    twitter_data_final o = (twitter_data_final) super.clone();
    o.day_date = (o.day_date != null) ? (java.sql.Date) o.day_date.clone() : null;
    o.start_date = (o.start_date != null) ? (java.sql.Date) o.start_date.clone() : null;
    o.end_date = (o.end_date != null) ? (java.sql.Date) o.end_date.clone() : null;
    return o;
  }

  public void clone0(twitter_data_final o) throws CloneNotSupportedException {
    o.day_date = (o.day_date != null) ? (java.sql.Date) o.day_date.clone() : null;
    o.start_date = (o.start_date != null) ? (java.sql.Date) o.start_date.clone() : null;
    o.end_date = (o.end_date != null) ? (java.sql.Date) o.end_date.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
    __sqoop$field_map.put("campaignId", this.campaignId);
    __sqoop$field_map.put("campaign_name", this.campaign_name);
    __sqoop$field_map.put("day_date", this.day_date);
    __sqoop$field_map.put("start_date", this.start_date);
    __sqoop$field_map.put("end_date", this.end_date);
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
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("campaignId", this.campaignId);
    __sqoop$field_map.put("campaign_name", this.campaign_name);
    __sqoop$field_map.put("day_date", this.day_date);
    __sqoop$field_map.put("start_date", this.start_date);
    __sqoop$field_map.put("end_date", this.end_date);
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
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if ("campaignId".equals(__fieldName)) {
      this.campaignId = (String) __fieldVal;
    }
    else    if ("campaign_name".equals(__fieldName)) {
      this.campaign_name = (String) __fieldVal;
    }
    else    if ("day_date".equals(__fieldName)) {
      this.day_date = (java.sql.Date) __fieldVal;
    }
    else    if ("start_date".equals(__fieldName)) {
      this.start_date = (java.sql.Date) __fieldVal;
    }
    else    if ("end_date".equals(__fieldName)) {
      this.end_date = (java.sql.Date) __fieldVal;
    }
    else    if ("spend".equals(__fieldName)) {
      this.spend = (Double) __fieldVal;
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
      this.cost_per_engagement = (Double) __fieldVal;
    }
    else    if ("cost_per_follow".equals(__fieldName)) {
      this.cost_per_follow = (Double) __fieldVal;
    }
    else    if ("cost_per_link_click".equals(__fieldName)) {
      this.cost_per_link_click = (Double) __fieldVal;
    }
    else {
      throw new RuntimeException("No such field: " + __fieldName);
    }
  }
  public boolean setField0(String __fieldName, Object __fieldVal) {
    if ("campaignId".equals(__fieldName)) {
      this.campaignId = (String) __fieldVal;
      return true;
    }
    else    if ("campaign_name".equals(__fieldName)) {
      this.campaign_name = (String) __fieldVal;
      return true;
    }
    else    if ("day_date".equals(__fieldName)) {
      this.day_date = (java.sql.Date) __fieldVal;
      return true;
    }
    else    if ("start_date".equals(__fieldName)) {
      this.start_date = (java.sql.Date) __fieldVal;
      return true;
    }
    else    if ("end_date".equals(__fieldName)) {
      this.end_date = (java.sql.Date) __fieldVal;
      return true;
    }
    else    if ("spend".equals(__fieldName)) {
      this.spend = (Double) __fieldVal;
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
      this.cost_per_engagement = (Double) __fieldVal;
      return true;
    }
    else    if ("cost_per_follow".equals(__fieldName)) {
      this.cost_per_follow = (Double) __fieldVal;
      return true;
    }
    else    if ("cost_per_link_click".equals(__fieldName)) {
      this.cost_per_link_click = (Double) __fieldVal;
      return true;
    }
    else {
      return false;    }
  }
}
