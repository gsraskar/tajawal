// ORM class for table 'tmp_twitter_data'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Wed Sep 13 07:48:24 UTC 2017
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

public class tmp_twitter_data extends SqoopRecord  implements DBWritable, Writable {
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
  public tmp_twitter_data with_campaignId(String campaignId) {
    this.campaignId = campaignId;
    return this;
  }
  private String campaignName;
  public String get_campaignName() {
    return campaignName;
  }
  public void set_campaignName(String campaignName) {
    this.campaignName = campaignName;
  }
  public tmp_twitter_data with_campaignName(String campaignName) {
    this.campaignName = campaignName;
    return this;
  }
  private String cdate;
  public String get_cdate() {
    return cdate;
  }
  public void set_cdate(String cdate) {
    this.cdate = cdate;
  }
  public tmp_twitter_data with_cdate(String cdate) {
    this.cdate = cdate;
    return this;
  }
  private String startDate;
  public String get_startDate() {
    return startDate;
  }
  public void set_startDate(String startDate) {
    this.startDate = startDate;
  }
  public tmp_twitter_data with_startDate(String startDate) {
    this.startDate = startDate;
    return this;
  }
  private String endDate;
  public String get_endDate() {
    return endDate;
  }
  public void set_endDate(String endDate) {
    this.endDate = endDate;
  }
  public tmp_twitter_data with_endDate(String endDate) {
    this.endDate = endDate;
    return this;
  }
  private String billed_charge_local_micro;
  public String get_billed_charge_local_micro() {
    return billed_charge_local_micro;
  }
  public void set_billed_charge_local_micro(String billed_charge_local_micro) {
    this.billed_charge_local_micro = billed_charge_local_micro;
  }
  public tmp_twitter_data with_billed_charge_local_micro(String billed_charge_local_micro) {
    this.billed_charge_local_micro = billed_charge_local_micro;
    return this;
  }
  private String impressions;
  public String get_impressions() {
    return impressions;
  }
  public void set_impressions(String impressions) {
    this.impressions = impressions;
  }
  public tmp_twitter_data with_impressions(String impressions) {
    this.impressions = impressions;
    return this;
  }
  private String engagements;
  public String get_engagements() {
    return engagements;
  }
  public void set_engagements(String engagements) {
    this.engagements = engagements;
  }
  public tmp_twitter_data with_engagements(String engagements) {
    this.engagements = engagements;
    return this;
  }
  private String billed_engagements;
  public String get_billed_engagements() {
    return billed_engagements;
  }
  public void set_billed_engagements(String billed_engagements) {
    this.billed_engagements = billed_engagements;
  }
  public tmp_twitter_data with_billed_engagements(String billed_engagements) {
    this.billed_engagements = billed_engagements;
    return this;
  }
  private String retweets;
  public String get_retweets() {
    return retweets;
  }
  public void set_retweets(String retweets) {
    this.retweets = retweets;
  }
  public tmp_twitter_data with_retweets(String retweets) {
    this.retweets = retweets;
    return this;
  }
  private String replies;
  public String get_replies() {
    return replies;
  }
  public void set_replies(String replies) {
    this.replies = replies;
  }
  public tmp_twitter_data with_replies(String replies) {
    this.replies = replies;
    return this;
  }
  private String follows;
  public String get_follows() {
    return follows;
  }
  public void set_follows(String follows) {
    this.follows = follows;
  }
  public tmp_twitter_data with_follows(String follows) {
    this.follows = follows;
    return this;
  }
  private String clicks;
  public String get_clicks() {
    return clicks;
  }
  public void set_clicks(String clicks) {
    this.clicks = clicks;
  }
  public tmp_twitter_data with_clicks(String clicks) {
    this.clicks = clicks;
    return this;
  }
  private String media_engagements;
  public String get_media_engagements() {
    return media_engagements;
  }
  public void set_media_engagements(String media_engagements) {
    this.media_engagements = media_engagements;
  }
  public tmp_twitter_data with_media_engagements(String media_engagements) {
    this.media_engagements = media_engagements;
    return this;
  }
  private String likes;
  public String get_likes() {
    return likes;
  }
  public void set_likes(String likes) {
    this.likes = likes;
  }
  public tmp_twitter_data with_likes(String likes) {
    this.likes = likes;
    return this;
  }
  private String url_clicks;
  public String get_url_clicks() {
    return url_clicks;
  }
  public void set_url_clicks(String url_clicks) {
    this.url_clicks = url_clicks;
  }
  public tmp_twitter_data with_url_clicks(String url_clicks) {
    this.url_clicks = url_clicks;
    return this;
  }
  private String app_clicks;
  public String get_app_clicks() {
    return app_clicks;
  }
  public void set_app_clicks(String app_clicks) {
    this.app_clicks = app_clicks;
  }
  public tmp_twitter_data with_app_clicks(String app_clicks) {
    this.app_clicks = app_clicks;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof tmp_twitter_data)) {
      return false;
    }
    tmp_twitter_data that = (tmp_twitter_data) o;
    boolean equal = true;
    equal = equal && (this.campaignId == null ? that.campaignId == null : this.campaignId.equals(that.campaignId));
    equal = equal && (this.campaignName == null ? that.campaignName == null : this.campaignName.equals(that.campaignName));
    equal = equal && (this.cdate == null ? that.cdate == null : this.cdate.equals(that.cdate));
    equal = equal && (this.startDate == null ? that.startDate == null : this.startDate.equals(that.startDate));
    equal = equal && (this.endDate == null ? that.endDate == null : this.endDate.equals(that.endDate));
    equal = equal && (this.billed_charge_local_micro == null ? that.billed_charge_local_micro == null : this.billed_charge_local_micro.equals(that.billed_charge_local_micro));
    equal = equal && (this.impressions == null ? that.impressions == null : this.impressions.equals(that.impressions));
    equal = equal && (this.engagements == null ? that.engagements == null : this.engagements.equals(that.engagements));
    equal = equal && (this.billed_engagements == null ? that.billed_engagements == null : this.billed_engagements.equals(that.billed_engagements));
    equal = equal && (this.retweets == null ? that.retweets == null : this.retweets.equals(that.retweets));
    equal = equal && (this.replies == null ? that.replies == null : this.replies.equals(that.replies));
    equal = equal && (this.follows == null ? that.follows == null : this.follows.equals(that.follows));
    equal = equal && (this.clicks == null ? that.clicks == null : this.clicks.equals(that.clicks));
    equal = equal && (this.media_engagements == null ? that.media_engagements == null : this.media_engagements.equals(that.media_engagements));
    equal = equal && (this.likes == null ? that.likes == null : this.likes.equals(that.likes));
    equal = equal && (this.url_clicks == null ? that.url_clicks == null : this.url_clicks.equals(that.url_clicks));
    equal = equal && (this.app_clicks == null ? that.app_clicks == null : this.app_clicks.equals(that.app_clicks));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof tmp_twitter_data)) {
      return false;
    }
    tmp_twitter_data that = (tmp_twitter_data) o;
    boolean equal = true;
    equal = equal && (this.campaignId == null ? that.campaignId == null : this.campaignId.equals(that.campaignId));
    equal = equal && (this.campaignName == null ? that.campaignName == null : this.campaignName.equals(that.campaignName));
    equal = equal && (this.cdate == null ? that.cdate == null : this.cdate.equals(that.cdate));
    equal = equal && (this.startDate == null ? that.startDate == null : this.startDate.equals(that.startDate));
    equal = equal && (this.endDate == null ? that.endDate == null : this.endDate.equals(that.endDate));
    equal = equal && (this.billed_charge_local_micro == null ? that.billed_charge_local_micro == null : this.billed_charge_local_micro.equals(that.billed_charge_local_micro));
    equal = equal && (this.impressions == null ? that.impressions == null : this.impressions.equals(that.impressions));
    equal = equal && (this.engagements == null ? that.engagements == null : this.engagements.equals(that.engagements));
    equal = equal && (this.billed_engagements == null ? that.billed_engagements == null : this.billed_engagements.equals(that.billed_engagements));
    equal = equal && (this.retweets == null ? that.retweets == null : this.retweets.equals(that.retweets));
    equal = equal && (this.replies == null ? that.replies == null : this.replies.equals(that.replies));
    equal = equal && (this.follows == null ? that.follows == null : this.follows.equals(that.follows));
    equal = equal && (this.clicks == null ? that.clicks == null : this.clicks.equals(that.clicks));
    equal = equal && (this.media_engagements == null ? that.media_engagements == null : this.media_engagements.equals(that.media_engagements));
    equal = equal && (this.likes == null ? that.likes == null : this.likes.equals(that.likes));
    equal = equal && (this.url_clicks == null ? that.url_clicks == null : this.url_clicks.equals(that.url_clicks));
    equal = equal && (this.app_clicks == null ? that.app_clicks == null : this.app_clicks.equals(that.app_clicks));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.campaignId = JdbcWritableBridge.readString(1, __dbResults);
    this.campaignName = JdbcWritableBridge.readString(2, __dbResults);
    this.cdate = JdbcWritableBridge.readString(3, __dbResults);
    this.startDate = JdbcWritableBridge.readString(4, __dbResults);
    this.endDate = JdbcWritableBridge.readString(5, __dbResults);
    this.billed_charge_local_micro = JdbcWritableBridge.readString(6, __dbResults);
    this.impressions = JdbcWritableBridge.readString(7, __dbResults);
    this.engagements = JdbcWritableBridge.readString(8, __dbResults);
    this.billed_engagements = JdbcWritableBridge.readString(9, __dbResults);
    this.retweets = JdbcWritableBridge.readString(10, __dbResults);
    this.replies = JdbcWritableBridge.readString(11, __dbResults);
    this.follows = JdbcWritableBridge.readString(12, __dbResults);
    this.clicks = JdbcWritableBridge.readString(13, __dbResults);
    this.media_engagements = JdbcWritableBridge.readString(14, __dbResults);
    this.likes = JdbcWritableBridge.readString(15, __dbResults);
    this.url_clicks = JdbcWritableBridge.readString(16, __dbResults);
    this.app_clicks = JdbcWritableBridge.readString(17, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.campaignId = JdbcWritableBridge.readString(1, __dbResults);
    this.campaignName = JdbcWritableBridge.readString(2, __dbResults);
    this.cdate = JdbcWritableBridge.readString(3, __dbResults);
    this.startDate = JdbcWritableBridge.readString(4, __dbResults);
    this.endDate = JdbcWritableBridge.readString(5, __dbResults);
    this.billed_charge_local_micro = JdbcWritableBridge.readString(6, __dbResults);
    this.impressions = JdbcWritableBridge.readString(7, __dbResults);
    this.engagements = JdbcWritableBridge.readString(8, __dbResults);
    this.billed_engagements = JdbcWritableBridge.readString(9, __dbResults);
    this.retweets = JdbcWritableBridge.readString(10, __dbResults);
    this.replies = JdbcWritableBridge.readString(11, __dbResults);
    this.follows = JdbcWritableBridge.readString(12, __dbResults);
    this.clicks = JdbcWritableBridge.readString(13, __dbResults);
    this.media_engagements = JdbcWritableBridge.readString(14, __dbResults);
    this.likes = JdbcWritableBridge.readString(15, __dbResults);
    this.url_clicks = JdbcWritableBridge.readString(16, __dbResults);
    this.app_clicks = JdbcWritableBridge.readString(17, __dbResults);
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
    JdbcWritableBridge.writeString(campaignName, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(cdate, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(startDate, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(endDate, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(billed_charge_local_micro, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(impressions, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(engagements, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(billed_engagements, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(retweets, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(replies, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(follows, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(clicks, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(media_engagements, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(likes, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(url_clicks, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(app_clicks, 17 + __off, 12, __dbStmt);
    return 17;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(campaignId, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(campaignName, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(cdate, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(startDate, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(endDate, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(billed_charge_local_micro, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(impressions, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(engagements, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(billed_engagements, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(retweets, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(replies, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(follows, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(clicks, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(media_engagements, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(likes, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(url_clicks, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(app_clicks, 17 + __off, 12, __dbStmt);
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
        this.campaignName = null;
    } else {
    this.campaignName = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.cdate = null;
    } else {
    this.cdate = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.startDate = null;
    } else {
    this.startDate = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.endDate = null;
    } else {
    this.endDate = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.billed_charge_local_micro = null;
    } else {
    this.billed_charge_local_micro = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.impressions = null;
    } else {
    this.impressions = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.engagements = null;
    } else {
    this.engagements = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.billed_engagements = null;
    } else {
    this.billed_engagements = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.retweets = null;
    } else {
    this.retweets = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.replies = null;
    } else {
    this.replies = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.follows = null;
    } else {
    this.follows = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.clicks = null;
    } else {
    this.clicks = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.media_engagements = null;
    } else {
    this.media_engagements = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.likes = null;
    } else {
    this.likes = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.url_clicks = null;
    } else {
    this.url_clicks = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.app_clicks = null;
    } else {
    this.app_clicks = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.campaignId) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, campaignId);
    }
    if (null == this.campaignName) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, campaignName);
    }
    if (null == this.cdate) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cdate);
    }
    if (null == this.startDate) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, startDate);
    }
    if (null == this.endDate) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, endDate);
    }
    if (null == this.billed_charge_local_micro) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, billed_charge_local_micro);
    }
    if (null == this.impressions) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, impressions);
    }
    if (null == this.engagements) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, engagements);
    }
    if (null == this.billed_engagements) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, billed_engagements);
    }
    if (null == this.retweets) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, retweets);
    }
    if (null == this.replies) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, replies);
    }
    if (null == this.follows) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, follows);
    }
    if (null == this.clicks) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, clicks);
    }
    if (null == this.media_engagements) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, media_engagements);
    }
    if (null == this.likes) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, likes);
    }
    if (null == this.url_clicks) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, url_clicks);
    }
    if (null == this.app_clicks) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, app_clicks);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.campaignId) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, campaignId);
    }
    if (null == this.campaignName) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, campaignName);
    }
    if (null == this.cdate) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cdate);
    }
    if (null == this.startDate) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, startDate);
    }
    if (null == this.endDate) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, endDate);
    }
    if (null == this.billed_charge_local_micro) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, billed_charge_local_micro);
    }
    if (null == this.impressions) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, impressions);
    }
    if (null == this.engagements) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, engagements);
    }
    if (null == this.billed_engagements) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, billed_engagements);
    }
    if (null == this.retweets) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, retweets);
    }
    if (null == this.replies) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, replies);
    }
    if (null == this.follows) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, follows);
    }
    if (null == this.clicks) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, clicks);
    }
    if (null == this.media_engagements) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, media_engagements);
    }
    if (null == this.likes) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, likes);
    }
    if (null == this.url_clicks) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, url_clicks);
    }
    if (null == this.app_clicks) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, app_clicks);
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
    __sb.append(FieldFormatter.escapeAndEnclose(campaignName==null?"null":campaignName, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cdate==null?"null":cdate, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(startDate==null?"null":startDate, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(endDate==null?"null":endDate, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(billed_charge_local_micro==null?"null":billed_charge_local_micro, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(impressions==null?"null":impressions, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(engagements==null?"null":engagements, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(billed_engagements==null?"null":billed_engagements, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(retweets==null?"null":retweets, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(replies==null?"null":replies, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(follows==null?"null":follows, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(clicks==null?"null":clicks, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(media_engagements==null?"null":media_engagements, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(likes==null?"null":likes, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(url_clicks==null?"null":url_clicks, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(app_clicks==null?"null":app_clicks, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(campaignId==null?"null":campaignId, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(campaignName==null?"null":campaignName, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cdate==null?"null":cdate, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(startDate==null?"null":startDate, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(endDate==null?"null":endDate, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(billed_charge_local_micro==null?"null":billed_charge_local_micro, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(impressions==null?"null":impressions, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(engagements==null?"null":engagements, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(billed_engagements==null?"null":billed_engagements, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(retweets==null?"null":retweets, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(replies==null?"null":replies, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(follows==null?"null":follows, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(clicks==null?"null":clicks, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(media_engagements==null?"null":media_engagements, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(likes==null?"null":likes, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(url_clicks==null?"null":url_clicks, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(app_clicks==null?"null":app_clicks, delimiters));
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
    if (__cur_str.equals("NULL")) { this.campaignName = null; } else {
      this.campaignName = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.cdate = null; } else {
      this.cdate = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.startDate = null; } else {
      this.startDate = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.endDate = null; } else {
      this.endDate = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.billed_charge_local_micro = null; } else {
      this.billed_charge_local_micro = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.impressions = null; } else {
      this.impressions = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.engagements = null; } else {
      this.engagements = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.billed_engagements = null; } else {
      this.billed_engagements = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.retweets = null; } else {
      this.retweets = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.replies = null; } else {
      this.replies = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.follows = null; } else {
      this.follows = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.clicks = null; } else {
      this.clicks = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.media_engagements = null; } else {
      this.media_engagements = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.likes = null; } else {
      this.likes = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.url_clicks = null; } else {
      this.url_clicks = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.app_clicks = null; } else {
      this.app_clicks = __cur_str;
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
    if (__cur_str.equals("NULL")) { this.campaignName = null; } else {
      this.campaignName = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.cdate = null; } else {
      this.cdate = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.startDate = null; } else {
      this.startDate = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.endDate = null; } else {
      this.endDate = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.billed_charge_local_micro = null; } else {
      this.billed_charge_local_micro = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.impressions = null; } else {
      this.impressions = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.engagements = null; } else {
      this.engagements = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.billed_engagements = null; } else {
      this.billed_engagements = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.retweets = null; } else {
      this.retweets = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.replies = null; } else {
      this.replies = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.follows = null; } else {
      this.follows = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.clicks = null; } else {
      this.clicks = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.media_engagements = null; } else {
      this.media_engagements = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.likes = null; } else {
      this.likes = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.url_clicks = null; } else {
      this.url_clicks = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("NULL")) { this.app_clicks = null; } else {
      this.app_clicks = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    tmp_twitter_data o = (tmp_twitter_data) super.clone();
    return o;
  }

  public void clone0(tmp_twitter_data o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
    __sqoop$field_map.put("campaignId", this.campaignId);
    __sqoop$field_map.put("campaignName", this.campaignName);
    __sqoop$field_map.put("cdate", this.cdate);
    __sqoop$field_map.put("startDate", this.startDate);
    __sqoop$field_map.put("endDate", this.endDate);
    __sqoop$field_map.put("billed_charge_local_micro", this.billed_charge_local_micro);
    __sqoop$field_map.put("impressions", this.impressions);
    __sqoop$field_map.put("engagements", this.engagements);
    __sqoop$field_map.put("billed_engagements", this.billed_engagements);
    __sqoop$field_map.put("retweets", this.retweets);
    __sqoop$field_map.put("replies", this.replies);
    __sqoop$field_map.put("follows", this.follows);
    __sqoop$field_map.put("clicks", this.clicks);
    __sqoop$field_map.put("media_engagements", this.media_engagements);
    __sqoop$field_map.put("likes", this.likes);
    __sqoop$field_map.put("url_clicks", this.url_clicks);
    __sqoop$field_map.put("app_clicks", this.app_clicks);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("campaignId", this.campaignId);
    __sqoop$field_map.put("campaignName", this.campaignName);
    __sqoop$field_map.put("cdate", this.cdate);
    __sqoop$field_map.put("startDate", this.startDate);
    __sqoop$field_map.put("endDate", this.endDate);
    __sqoop$field_map.put("billed_charge_local_micro", this.billed_charge_local_micro);
    __sqoop$field_map.put("impressions", this.impressions);
    __sqoop$field_map.put("engagements", this.engagements);
    __sqoop$field_map.put("billed_engagements", this.billed_engagements);
    __sqoop$field_map.put("retweets", this.retweets);
    __sqoop$field_map.put("replies", this.replies);
    __sqoop$field_map.put("follows", this.follows);
    __sqoop$field_map.put("clicks", this.clicks);
    __sqoop$field_map.put("media_engagements", this.media_engagements);
    __sqoop$field_map.put("likes", this.likes);
    __sqoop$field_map.put("url_clicks", this.url_clicks);
    __sqoop$field_map.put("app_clicks", this.app_clicks);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if ("campaignId".equals(__fieldName)) {
      this.campaignId = (String) __fieldVal;
    }
    else    if ("campaignName".equals(__fieldName)) {
      this.campaignName = (String) __fieldVal;
    }
    else    if ("cdate".equals(__fieldName)) {
      this.cdate = (String) __fieldVal;
    }
    else    if ("startDate".equals(__fieldName)) {
      this.startDate = (String) __fieldVal;
    }
    else    if ("endDate".equals(__fieldName)) {
      this.endDate = (String) __fieldVal;
    }
    else    if ("billed_charge_local_micro".equals(__fieldName)) {
      this.billed_charge_local_micro = (String) __fieldVal;
    }
    else    if ("impressions".equals(__fieldName)) {
      this.impressions = (String) __fieldVal;
    }
    else    if ("engagements".equals(__fieldName)) {
      this.engagements = (String) __fieldVal;
    }
    else    if ("billed_engagements".equals(__fieldName)) {
      this.billed_engagements = (String) __fieldVal;
    }
    else    if ("retweets".equals(__fieldName)) {
      this.retweets = (String) __fieldVal;
    }
    else    if ("replies".equals(__fieldName)) {
      this.replies = (String) __fieldVal;
    }
    else    if ("follows".equals(__fieldName)) {
      this.follows = (String) __fieldVal;
    }
    else    if ("clicks".equals(__fieldName)) {
      this.clicks = (String) __fieldVal;
    }
    else    if ("media_engagements".equals(__fieldName)) {
      this.media_engagements = (String) __fieldVal;
    }
    else    if ("likes".equals(__fieldName)) {
      this.likes = (String) __fieldVal;
    }
    else    if ("url_clicks".equals(__fieldName)) {
      this.url_clicks = (String) __fieldVal;
    }
    else    if ("app_clicks".equals(__fieldName)) {
      this.app_clicks = (String) __fieldVal;
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
    else    if ("campaignName".equals(__fieldName)) {
      this.campaignName = (String) __fieldVal;
      return true;
    }
    else    if ("cdate".equals(__fieldName)) {
      this.cdate = (String) __fieldVal;
      return true;
    }
    else    if ("startDate".equals(__fieldName)) {
      this.startDate = (String) __fieldVal;
      return true;
    }
    else    if ("endDate".equals(__fieldName)) {
      this.endDate = (String) __fieldVal;
      return true;
    }
    else    if ("billed_charge_local_micro".equals(__fieldName)) {
      this.billed_charge_local_micro = (String) __fieldVal;
      return true;
    }
    else    if ("impressions".equals(__fieldName)) {
      this.impressions = (String) __fieldVal;
      return true;
    }
    else    if ("engagements".equals(__fieldName)) {
      this.engagements = (String) __fieldVal;
      return true;
    }
    else    if ("billed_engagements".equals(__fieldName)) {
      this.billed_engagements = (String) __fieldVal;
      return true;
    }
    else    if ("retweets".equals(__fieldName)) {
      this.retweets = (String) __fieldVal;
      return true;
    }
    else    if ("replies".equals(__fieldName)) {
      this.replies = (String) __fieldVal;
      return true;
    }
    else    if ("follows".equals(__fieldName)) {
      this.follows = (String) __fieldVal;
      return true;
    }
    else    if ("clicks".equals(__fieldName)) {
      this.clicks = (String) __fieldVal;
      return true;
    }
    else    if ("media_engagements".equals(__fieldName)) {
      this.media_engagements = (String) __fieldVal;
      return true;
    }
    else    if ("likes".equals(__fieldName)) {
      this.likes = (String) __fieldVal;
      return true;
    }
    else    if ("url_clicks".equals(__fieldName)) {
      this.url_clicks = (String) __fieldVal;
      return true;
    }
    else    if ("app_clicks".equals(__fieldName)) {
      this.app_clicks = (String) __fieldVal;
      return true;
    }
    else {
      return false;    }
  }
}
