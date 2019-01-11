// ORM class for table 'adjust_data'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Tue May 23 10:14:55 UTC 2017
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
import java.util.HashMap;

public class adjust_data extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("tracker_token", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        tracker_token = (String)value;
      }
    });
    setters.put("network", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        network = (String)value;
      }
    });
    setters.put("campaign", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        campaign = (String)value;
      }
    });
    setters.put("adgroup", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        adgroup = (String)value;
      }
    });
    setters.put("creative", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        creative = (String)value;
      }
    });
    setters.put("date", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        date = (String)value;
      }
    });
    setters.put("clicks", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        clicks = (String)value;
      }
    });
    setters.put("impressions", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        impressions = (String)value;
      }
    });
    setters.put("installs", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        installs = (String)value;
      }
    });
    setters.put("click_conversion_rate", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        click_conversion_rate = (String)value;
      }
    });
    setters.put("ctr", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ctr = (String)value;
      }
    });
    setters.put("impression_conversion_rate", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        impression_conversion_rate = (String)value;
      }
    });
    setters.put("reattributions", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        reattributions = (String)value;
      }
    });
    setters.put("sessions", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        sessions = (String)value;
      }
    });
    setters.put("revenue_events", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        revenue_events = (String)value;
      }
    });
    setters.put("revenue", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        revenue = (String)value;
      }
    });
    setters.put("daus", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        daus = (String)value;
      }
    });
    setters.put("waus", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        waus = (String)value;
      }
    });
    setters.put("maus", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        maus = (String)value;
      }
    });
    setters.put("22o1ir_events", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        _22o1ir_events = (String)value;
      }
    });
    setters.put("2t2mp6_events", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        _2t2mp6_events = (String)value;
      }
    });
    setters.put("87hoor_events", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        _87hoor_events = (String)value;
      }
    });
    setters.put("96qx7c_events", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        _96qx7c_events = (String)value;
      }
    });
    setters.put("bcftn0_events", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        bcftn0_events = (String)value;
      }
    });
    setters.put("dws9tj_events", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        dws9tj_events = (String)value;
      }
    });
    setters.put("ik4edh_revenue", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ik4edh_revenue = (String)value;
      }
    });
    setters.put("ik4edh_events", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ik4edh_events = (String)value;
      }
    });
    setters.put("ik4edh_revenue_per_event", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ik4edh_revenue_per_event = (String)value;
      }
    });
    setters.put("kfg79g_events", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        kfg79g_events = (String)value;
      }
    });
    setters.put("lhfwnt_events", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        lhfwnt_events = (String)value;
      }
    });
    setters.put("o9e3bk_events", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        o9e3bk_events = (String)value;
      }
    });
    setters.put("smho8i_revenue", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        smho8i_revenue = (String)value;
      }
    });
    setters.put("smho8i_events", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        smho8i_events = (String)value;
      }
    });
    setters.put("smho8i_revenue_per_event", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        smho8i_revenue_per_event = (String)value;
      }
    });
    setters.put("snmvax_events", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        snmvax_events = (String)value;
      }
    });
    setters.put("wpj4el_events", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        wpj4el_events = (String)value;
      }
    });
    setters.put("x1g8p2_events", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        x1g8p2_events = (String)value;
      }
    });
    setters.put("ylmlgd_revenue", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ylmlgd_revenue = (String)value;
      }
    });
    setters.put("ylmlgd_events", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ylmlgd_events = (String)value;
      }
    });
    setters.put("ylmlgd_revenue_per_event", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ylmlgd_revenue_per_event = (String)value;
      }
    });
  }
  public adjust_data() {
    init0();
  }
  private String tracker_token;
  public String get_tracker_token() {
    return tracker_token;
  }
  public void set_tracker_token(String tracker_token) {
    this.tracker_token = tracker_token;
  }
  public adjust_data with_tracker_token(String tracker_token) {
    this.tracker_token = tracker_token;
    return this;
  }
  private String network;
  public String get_network() {
    return network;
  }
  public void set_network(String network) {
    this.network = network;
  }
  public adjust_data with_network(String network) {
    this.network = network;
    return this;
  }
  private String campaign;
  public String get_campaign() {
    return campaign;
  }
  public void set_campaign(String campaign) {
    this.campaign = campaign;
  }
  public adjust_data with_campaign(String campaign) {
    this.campaign = campaign;
    return this;
  }
  private String adgroup;
  public String get_adgroup() {
    return adgroup;
  }
  public void set_adgroup(String adgroup) {
    this.adgroup = adgroup;
  }
  public adjust_data with_adgroup(String adgroup) {
    this.adgroup = adgroup;
    return this;
  }
  private String creative;
  public String get_creative() {
    return creative;
  }
  public void set_creative(String creative) {
    this.creative = creative;
  }
  public adjust_data with_creative(String creative) {
    this.creative = creative;
    return this;
  }
  private String date;
  public String get_date() {
    return date;
  }
  public void set_date(String date) {
    this.date = date;
  }
  public adjust_data with_date(String date) {
    this.date = date;
    return this;
  }
  private String clicks;
  public String get_clicks() {
    return clicks;
  }
  public void set_clicks(String clicks) {
    this.clicks = clicks;
  }
  public adjust_data with_clicks(String clicks) {
    this.clicks = clicks;
    return this;
  }
  private String impressions;
  public String get_impressions() {
    return impressions;
  }
  public void set_impressions(String impressions) {
    this.impressions = impressions;
  }
  public adjust_data with_impressions(String impressions) {
    this.impressions = impressions;
    return this;
  }
  private String installs;
  public String get_installs() {
    return installs;
  }
  public void set_installs(String installs) {
    this.installs = installs;
  }
  public adjust_data with_installs(String installs) {
    this.installs = installs;
    return this;
  }
  private String click_conversion_rate;
  public String get_click_conversion_rate() {
    return click_conversion_rate;
  }
  public void set_click_conversion_rate(String click_conversion_rate) {
    this.click_conversion_rate = click_conversion_rate;
  }
  public adjust_data with_click_conversion_rate(String click_conversion_rate) {
    this.click_conversion_rate = click_conversion_rate;
    return this;
  }
  private String ctr;
  public String get_ctr() {
    return ctr;
  }
  public void set_ctr(String ctr) {
    this.ctr = ctr;
  }
  public adjust_data with_ctr(String ctr) {
    this.ctr = ctr;
    return this;
  }
  private String impression_conversion_rate;
  public String get_impression_conversion_rate() {
    return impression_conversion_rate;
  }
  public void set_impression_conversion_rate(String impression_conversion_rate) {
    this.impression_conversion_rate = impression_conversion_rate;
  }
  public adjust_data with_impression_conversion_rate(String impression_conversion_rate) {
    this.impression_conversion_rate = impression_conversion_rate;
    return this;
  }
  private String reattributions;
  public String get_reattributions() {
    return reattributions;
  }
  public void set_reattributions(String reattributions) {
    this.reattributions = reattributions;
  }
  public adjust_data with_reattributions(String reattributions) {
    this.reattributions = reattributions;
    return this;
  }
  private String sessions;
  public String get_sessions() {
    return sessions;
  }
  public void set_sessions(String sessions) {
    this.sessions = sessions;
  }
  public adjust_data with_sessions(String sessions) {
    this.sessions = sessions;
    return this;
  }
  private String revenue_events;
  public String get_revenue_events() {
    return revenue_events;
  }
  public void set_revenue_events(String revenue_events) {
    this.revenue_events = revenue_events;
  }
  public adjust_data with_revenue_events(String revenue_events) {
    this.revenue_events = revenue_events;
    return this;
  }
  private String revenue;
  public String get_revenue() {
    return revenue;
  }
  public void set_revenue(String revenue) {
    this.revenue = revenue;
  }
  public adjust_data with_revenue(String revenue) {
    this.revenue = revenue;
    return this;
  }
  private String daus;
  public String get_daus() {
    return daus;
  }
  public void set_daus(String daus) {
    this.daus = daus;
  }
  public adjust_data with_daus(String daus) {
    this.daus = daus;
    return this;
  }
  private String waus;
  public String get_waus() {
    return waus;
  }
  public void set_waus(String waus) {
    this.waus = waus;
  }
  public adjust_data with_waus(String waus) {
    this.waus = waus;
    return this;
  }
  private String maus;
  public String get_maus() {
    return maus;
  }
  public void set_maus(String maus) {
    this.maus = maus;
  }
  public adjust_data with_maus(String maus) {
    this.maus = maus;
    return this;
  }
  private String _22o1ir_events;
  public String get__22o1ir_events() {
    return _22o1ir_events;
  }
  public void set__22o1ir_events(String _22o1ir_events) {
    this._22o1ir_events = _22o1ir_events;
  }
  public adjust_data with__22o1ir_events(String _22o1ir_events) {
    this._22o1ir_events = _22o1ir_events;
    return this;
  }
  private String _2t2mp6_events;
  public String get__2t2mp6_events() {
    return _2t2mp6_events;
  }
  public void set__2t2mp6_events(String _2t2mp6_events) {
    this._2t2mp6_events = _2t2mp6_events;
  }
  public adjust_data with__2t2mp6_events(String _2t2mp6_events) {
    this._2t2mp6_events = _2t2mp6_events;
    return this;
  }
  private String _87hoor_events;
  public String get__87hoor_events() {
    return _87hoor_events;
  }
  public void set__87hoor_events(String _87hoor_events) {
    this._87hoor_events = _87hoor_events;
  }
  public adjust_data with__87hoor_events(String _87hoor_events) {
    this._87hoor_events = _87hoor_events;
    return this;
  }
  private String _96qx7c_events;
  public String get__96qx7c_events() {
    return _96qx7c_events;
  }
  public void set__96qx7c_events(String _96qx7c_events) {
    this._96qx7c_events = _96qx7c_events;
  }
  public adjust_data with__96qx7c_events(String _96qx7c_events) {
    this._96qx7c_events = _96qx7c_events;
    return this;
  }
  private String bcftn0_events;
  public String get_bcftn0_events() {
    return bcftn0_events;
  }
  public void set_bcftn0_events(String bcftn0_events) {
    this.bcftn0_events = bcftn0_events;
  }
  public adjust_data with_bcftn0_events(String bcftn0_events) {
    this.bcftn0_events = bcftn0_events;
    return this;
  }
  private String dws9tj_events;
  public String get_dws9tj_events() {
    return dws9tj_events;
  }
  public void set_dws9tj_events(String dws9tj_events) {
    this.dws9tj_events = dws9tj_events;
  }
  public adjust_data with_dws9tj_events(String dws9tj_events) {
    this.dws9tj_events = dws9tj_events;
    return this;
  }
  private String ik4edh_revenue;
  public String get_ik4edh_revenue() {
    return ik4edh_revenue;
  }
  public void set_ik4edh_revenue(String ik4edh_revenue) {
    this.ik4edh_revenue = ik4edh_revenue;
  }
  public adjust_data with_ik4edh_revenue(String ik4edh_revenue) {
    this.ik4edh_revenue = ik4edh_revenue;
    return this;
  }
  private String ik4edh_events;
  public String get_ik4edh_events() {
    return ik4edh_events;
  }
  public void set_ik4edh_events(String ik4edh_events) {
    this.ik4edh_events = ik4edh_events;
  }
  public adjust_data with_ik4edh_events(String ik4edh_events) {
    this.ik4edh_events = ik4edh_events;
    return this;
  }
  private String ik4edh_revenue_per_event;
  public String get_ik4edh_revenue_per_event() {
    return ik4edh_revenue_per_event;
  }
  public void set_ik4edh_revenue_per_event(String ik4edh_revenue_per_event) {
    this.ik4edh_revenue_per_event = ik4edh_revenue_per_event;
  }
  public adjust_data with_ik4edh_revenue_per_event(String ik4edh_revenue_per_event) {
    this.ik4edh_revenue_per_event = ik4edh_revenue_per_event;
    return this;
  }
  private String kfg79g_events;
  public String get_kfg79g_events() {
    return kfg79g_events;
  }
  public void set_kfg79g_events(String kfg79g_events) {
    this.kfg79g_events = kfg79g_events;
  }
  public adjust_data with_kfg79g_events(String kfg79g_events) {
    this.kfg79g_events = kfg79g_events;
    return this;
  }
  private String lhfwnt_events;
  public String get_lhfwnt_events() {
    return lhfwnt_events;
  }
  public void set_lhfwnt_events(String lhfwnt_events) {
    this.lhfwnt_events = lhfwnt_events;
  }
  public adjust_data with_lhfwnt_events(String lhfwnt_events) {
    this.lhfwnt_events = lhfwnt_events;
    return this;
  }
  private String o9e3bk_events;
  public String get_o9e3bk_events() {
    return o9e3bk_events;
  }
  public void set_o9e3bk_events(String o9e3bk_events) {
    this.o9e3bk_events = o9e3bk_events;
  }
  public adjust_data with_o9e3bk_events(String o9e3bk_events) {
    this.o9e3bk_events = o9e3bk_events;
    return this;
  }
  private String smho8i_revenue;
  public String get_smho8i_revenue() {
    return smho8i_revenue;
  }
  public void set_smho8i_revenue(String smho8i_revenue) {
    this.smho8i_revenue = smho8i_revenue;
  }
  public adjust_data with_smho8i_revenue(String smho8i_revenue) {
    this.smho8i_revenue = smho8i_revenue;
    return this;
  }
  private String smho8i_events;
  public String get_smho8i_events() {
    return smho8i_events;
  }
  public void set_smho8i_events(String smho8i_events) {
    this.smho8i_events = smho8i_events;
  }
  public adjust_data with_smho8i_events(String smho8i_events) {
    this.smho8i_events = smho8i_events;
    return this;
  }
  private String smho8i_revenue_per_event;
  public String get_smho8i_revenue_per_event() {
    return smho8i_revenue_per_event;
  }
  public void set_smho8i_revenue_per_event(String smho8i_revenue_per_event) {
    this.smho8i_revenue_per_event = smho8i_revenue_per_event;
  }
  public adjust_data with_smho8i_revenue_per_event(String smho8i_revenue_per_event) {
    this.smho8i_revenue_per_event = smho8i_revenue_per_event;
    return this;
  }
  private String snmvax_events;
  public String get_snmvax_events() {
    return snmvax_events;
  }
  public void set_snmvax_events(String snmvax_events) {
    this.snmvax_events = snmvax_events;
  }
  public adjust_data with_snmvax_events(String snmvax_events) {
    this.snmvax_events = snmvax_events;
    return this;
  }
  private String wpj4el_events;
  public String get_wpj4el_events() {
    return wpj4el_events;
  }
  public void set_wpj4el_events(String wpj4el_events) {
    this.wpj4el_events = wpj4el_events;
  }
  public adjust_data with_wpj4el_events(String wpj4el_events) {
    this.wpj4el_events = wpj4el_events;
    return this;
  }
  private String x1g8p2_events;
  public String get_x1g8p2_events() {
    return x1g8p2_events;
  }
  public void set_x1g8p2_events(String x1g8p2_events) {
    this.x1g8p2_events = x1g8p2_events;
  }
  public adjust_data with_x1g8p2_events(String x1g8p2_events) {
    this.x1g8p2_events = x1g8p2_events;
    return this;
  }
  private String ylmlgd_revenue;
  public String get_ylmlgd_revenue() {
    return ylmlgd_revenue;
  }
  public void set_ylmlgd_revenue(String ylmlgd_revenue) {
    this.ylmlgd_revenue = ylmlgd_revenue;
  }
  public adjust_data with_ylmlgd_revenue(String ylmlgd_revenue) {
    this.ylmlgd_revenue = ylmlgd_revenue;
    return this;
  }
  private String ylmlgd_events;
  public String get_ylmlgd_events() {
    return ylmlgd_events;
  }
  public void set_ylmlgd_events(String ylmlgd_events) {
    this.ylmlgd_events = ylmlgd_events;
  }
  public adjust_data with_ylmlgd_events(String ylmlgd_events) {
    this.ylmlgd_events = ylmlgd_events;
    return this;
  }
  private String ylmlgd_revenue_per_event;
  public String get_ylmlgd_revenue_per_event() {
    return ylmlgd_revenue_per_event;
  }
  public void set_ylmlgd_revenue_per_event(String ylmlgd_revenue_per_event) {
    this.ylmlgd_revenue_per_event = ylmlgd_revenue_per_event;
  }
  public adjust_data with_ylmlgd_revenue_per_event(String ylmlgd_revenue_per_event) {
    this.ylmlgd_revenue_per_event = ylmlgd_revenue_per_event;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof adjust_data)) {
      return false;
    }
    adjust_data that = (adjust_data) o;
    boolean equal = true;
    equal = equal && (this.tracker_token == null ? that.tracker_token == null : this.tracker_token.equals(that.tracker_token));
    equal = equal && (this.network == null ? that.network == null : this.network.equals(that.network));
    equal = equal && (this.campaign == null ? that.campaign == null : this.campaign.equals(that.campaign));
    equal = equal && (this.adgroup == null ? that.adgroup == null : this.adgroup.equals(that.adgroup));
    equal = equal && (this.creative == null ? that.creative == null : this.creative.equals(that.creative));
    equal = equal && (this.date == null ? that.date == null : this.date.equals(that.date));
    equal = equal && (this.clicks == null ? that.clicks == null : this.clicks.equals(that.clicks));
    equal = equal && (this.impressions == null ? that.impressions == null : this.impressions.equals(that.impressions));
    equal = equal && (this.installs == null ? that.installs == null : this.installs.equals(that.installs));
    equal = equal && (this.click_conversion_rate == null ? that.click_conversion_rate == null : this.click_conversion_rate.equals(that.click_conversion_rate));
    equal = equal && (this.ctr == null ? that.ctr == null : this.ctr.equals(that.ctr));
    equal = equal && (this.impression_conversion_rate == null ? that.impression_conversion_rate == null : this.impression_conversion_rate.equals(that.impression_conversion_rate));
    equal = equal && (this.reattributions == null ? that.reattributions == null : this.reattributions.equals(that.reattributions));
    equal = equal && (this.sessions == null ? that.sessions == null : this.sessions.equals(that.sessions));
    equal = equal && (this.revenue_events == null ? that.revenue_events == null : this.revenue_events.equals(that.revenue_events));
    equal = equal && (this.revenue == null ? that.revenue == null : this.revenue.equals(that.revenue));
    equal = equal && (this.daus == null ? that.daus == null : this.daus.equals(that.daus));
    equal = equal && (this.waus == null ? that.waus == null : this.waus.equals(that.waus));
    equal = equal && (this.maus == null ? that.maus == null : this.maus.equals(that.maus));
    equal = equal && (this._22o1ir_events == null ? that._22o1ir_events == null : this._22o1ir_events.equals(that._22o1ir_events));
    equal = equal && (this._2t2mp6_events == null ? that._2t2mp6_events == null : this._2t2mp6_events.equals(that._2t2mp6_events));
    equal = equal && (this._87hoor_events == null ? that._87hoor_events == null : this._87hoor_events.equals(that._87hoor_events));
    equal = equal && (this._96qx7c_events == null ? that._96qx7c_events == null : this._96qx7c_events.equals(that._96qx7c_events));
    equal = equal && (this.bcftn0_events == null ? that.bcftn0_events == null : this.bcftn0_events.equals(that.bcftn0_events));
    equal = equal && (this.dws9tj_events == null ? that.dws9tj_events == null : this.dws9tj_events.equals(that.dws9tj_events));
    equal = equal && (this.ik4edh_revenue == null ? that.ik4edh_revenue == null : this.ik4edh_revenue.equals(that.ik4edh_revenue));
    equal = equal && (this.ik4edh_events == null ? that.ik4edh_events == null : this.ik4edh_events.equals(that.ik4edh_events));
    equal = equal && (this.ik4edh_revenue_per_event == null ? that.ik4edh_revenue_per_event == null : this.ik4edh_revenue_per_event.equals(that.ik4edh_revenue_per_event));
    equal = equal && (this.kfg79g_events == null ? that.kfg79g_events == null : this.kfg79g_events.equals(that.kfg79g_events));
    equal = equal && (this.lhfwnt_events == null ? that.lhfwnt_events == null : this.lhfwnt_events.equals(that.lhfwnt_events));
    equal = equal && (this.o9e3bk_events == null ? that.o9e3bk_events == null : this.o9e3bk_events.equals(that.o9e3bk_events));
    equal = equal && (this.smho8i_revenue == null ? that.smho8i_revenue == null : this.smho8i_revenue.equals(that.smho8i_revenue));
    equal = equal && (this.smho8i_events == null ? that.smho8i_events == null : this.smho8i_events.equals(that.smho8i_events));
    equal = equal && (this.smho8i_revenue_per_event == null ? that.smho8i_revenue_per_event == null : this.smho8i_revenue_per_event.equals(that.smho8i_revenue_per_event));
    equal = equal && (this.snmvax_events == null ? that.snmvax_events == null : this.snmvax_events.equals(that.snmvax_events));
    equal = equal && (this.wpj4el_events == null ? that.wpj4el_events == null : this.wpj4el_events.equals(that.wpj4el_events));
    equal = equal && (this.x1g8p2_events == null ? that.x1g8p2_events == null : this.x1g8p2_events.equals(that.x1g8p2_events));
    equal = equal && (this.ylmlgd_revenue == null ? that.ylmlgd_revenue == null : this.ylmlgd_revenue.equals(that.ylmlgd_revenue));
    equal = equal && (this.ylmlgd_events == null ? that.ylmlgd_events == null : this.ylmlgd_events.equals(that.ylmlgd_events));
    equal = equal && (this.ylmlgd_revenue_per_event == null ? that.ylmlgd_revenue_per_event == null : this.ylmlgd_revenue_per_event.equals(that.ylmlgd_revenue_per_event));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof adjust_data)) {
      return false;
    }
    adjust_data that = (adjust_data) o;
    boolean equal = true;
    equal = equal && (this.tracker_token == null ? that.tracker_token == null : this.tracker_token.equals(that.tracker_token));
    equal = equal && (this.network == null ? that.network == null : this.network.equals(that.network));
    equal = equal && (this.campaign == null ? that.campaign == null : this.campaign.equals(that.campaign));
    equal = equal && (this.adgroup == null ? that.adgroup == null : this.adgroup.equals(that.adgroup));
    equal = equal && (this.creative == null ? that.creative == null : this.creative.equals(that.creative));
    equal = equal && (this.date == null ? that.date == null : this.date.equals(that.date));
    equal = equal && (this.clicks == null ? that.clicks == null : this.clicks.equals(that.clicks));
    equal = equal && (this.impressions == null ? that.impressions == null : this.impressions.equals(that.impressions));
    equal = equal && (this.installs == null ? that.installs == null : this.installs.equals(that.installs));
    equal = equal && (this.click_conversion_rate == null ? that.click_conversion_rate == null : this.click_conversion_rate.equals(that.click_conversion_rate));
    equal = equal && (this.ctr == null ? that.ctr == null : this.ctr.equals(that.ctr));
    equal = equal && (this.impression_conversion_rate == null ? that.impression_conversion_rate == null : this.impression_conversion_rate.equals(that.impression_conversion_rate));
    equal = equal && (this.reattributions == null ? that.reattributions == null : this.reattributions.equals(that.reattributions));
    equal = equal && (this.sessions == null ? that.sessions == null : this.sessions.equals(that.sessions));
    equal = equal && (this.revenue_events == null ? that.revenue_events == null : this.revenue_events.equals(that.revenue_events));
    equal = equal && (this.revenue == null ? that.revenue == null : this.revenue.equals(that.revenue));
    equal = equal && (this.daus == null ? that.daus == null : this.daus.equals(that.daus));
    equal = equal && (this.waus == null ? that.waus == null : this.waus.equals(that.waus));
    equal = equal && (this.maus == null ? that.maus == null : this.maus.equals(that.maus));
    equal = equal && (this._22o1ir_events == null ? that._22o1ir_events == null : this._22o1ir_events.equals(that._22o1ir_events));
    equal = equal && (this._2t2mp6_events == null ? that._2t2mp6_events == null : this._2t2mp6_events.equals(that._2t2mp6_events));
    equal = equal && (this._87hoor_events == null ? that._87hoor_events == null : this._87hoor_events.equals(that._87hoor_events));
    equal = equal && (this._96qx7c_events == null ? that._96qx7c_events == null : this._96qx7c_events.equals(that._96qx7c_events));
    equal = equal && (this.bcftn0_events == null ? that.bcftn0_events == null : this.bcftn0_events.equals(that.bcftn0_events));
    equal = equal && (this.dws9tj_events == null ? that.dws9tj_events == null : this.dws9tj_events.equals(that.dws9tj_events));
    equal = equal && (this.ik4edh_revenue == null ? that.ik4edh_revenue == null : this.ik4edh_revenue.equals(that.ik4edh_revenue));
    equal = equal && (this.ik4edh_events == null ? that.ik4edh_events == null : this.ik4edh_events.equals(that.ik4edh_events));
    equal = equal && (this.ik4edh_revenue_per_event == null ? that.ik4edh_revenue_per_event == null : this.ik4edh_revenue_per_event.equals(that.ik4edh_revenue_per_event));
    equal = equal && (this.kfg79g_events == null ? that.kfg79g_events == null : this.kfg79g_events.equals(that.kfg79g_events));
    equal = equal && (this.lhfwnt_events == null ? that.lhfwnt_events == null : this.lhfwnt_events.equals(that.lhfwnt_events));
    equal = equal && (this.o9e3bk_events == null ? that.o9e3bk_events == null : this.o9e3bk_events.equals(that.o9e3bk_events));
    equal = equal && (this.smho8i_revenue == null ? that.smho8i_revenue == null : this.smho8i_revenue.equals(that.smho8i_revenue));
    equal = equal && (this.smho8i_events == null ? that.smho8i_events == null : this.smho8i_events.equals(that.smho8i_events));
    equal = equal && (this.smho8i_revenue_per_event == null ? that.smho8i_revenue_per_event == null : this.smho8i_revenue_per_event.equals(that.smho8i_revenue_per_event));
    equal = equal && (this.snmvax_events == null ? that.snmvax_events == null : this.snmvax_events.equals(that.snmvax_events));
    equal = equal && (this.wpj4el_events == null ? that.wpj4el_events == null : this.wpj4el_events.equals(that.wpj4el_events));
    equal = equal && (this.x1g8p2_events == null ? that.x1g8p2_events == null : this.x1g8p2_events.equals(that.x1g8p2_events));
    equal = equal && (this.ylmlgd_revenue == null ? that.ylmlgd_revenue == null : this.ylmlgd_revenue.equals(that.ylmlgd_revenue));
    equal = equal && (this.ylmlgd_events == null ? that.ylmlgd_events == null : this.ylmlgd_events.equals(that.ylmlgd_events));
    equal = equal && (this.ylmlgd_revenue_per_event == null ? that.ylmlgd_revenue_per_event == null : this.ylmlgd_revenue_per_event.equals(that.ylmlgd_revenue_per_event));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.tracker_token = JdbcWritableBridge.readString(1, __dbResults);
    this.network = JdbcWritableBridge.readString(2, __dbResults);
    this.campaign = JdbcWritableBridge.readString(3, __dbResults);
    this.adgroup = JdbcWritableBridge.readString(4, __dbResults);
    this.creative = JdbcWritableBridge.readString(5, __dbResults);
    this.date = JdbcWritableBridge.readString(6, __dbResults);
    this.clicks = JdbcWritableBridge.readString(7, __dbResults);
    this.impressions = JdbcWritableBridge.readString(8, __dbResults);
    this.installs = JdbcWritableBridge.readString(9, __dbResults);
    this.click_conversion_rate = JdbcWritableBridge.readString(10, __dbResults);
    this.ctr = JdbcWritableBridge.readString(11, __dbResults);
    this.impression_conversion_rate = JdbcWritableBridge.readString(12, __dbResults);
    this.reattributions = JdbcWritableBridge.readString(13, __dbResults);
    this.sessions = JdbcWritableBridge.readString(14, __dbResults);
    this.revenue_events = JdbcWritableBridge.readString(15, __dbResults);
    this.revenue = JdbcWritableBridge.readString(16, __dbResults);
    this.daus = JdbcWritableBridge.readString(17, __dbResults);
    this.waus = JdbcWritableBridge.readString(18, __dbResults);
    this.maus = JdbcWritableBridge.readString(19, __dbResults);
    this._22o1ir_events = JdbcWritableBridge.readString(20, __dbResults);
    this._2t2mp6_events = JdbcWritableBridge.readString(21, __dbResults);
    this._87hoor_events = JdbcWritableBridge.readString(22, __dbResults);
    this._96qx7c_events = JdbcWritableBridge.readString(23, __dbResults);
    this.bcftn0_events = JdbcWritableBridge.readString(24, __dbResults);
    this.dws9tj_events = JdbcWritableBridge.readString(25, __dbResults);
    this.ik4edh_revenue = JdbcWritableBridge.readString(26, __dbResults);
    this.ik4edh_events = JdbcWritableBridge.readString(27, __dbResults);
    this.ik4edh_revenue_per_event = JdbcWritableBridge.readString(28, __dbResults);
    this.kfg79g_events = JdbcWritableBridge.readString(29, __dbResults);
    this.lhfwnt_events = JdbcWritableBridge.readString(30, __dbResults);
    this.o9e3bk_events = JdbcWritableBridge.readString(31, __dbResults);
    this.smho8i_revenue = JdbcWritableBridge.readString(32, __dbResults);
    this.smho8i_events = JdbcWritableBridge.readString(33, __dbResults);
    this.smho8i_revenue_per_event = JdbcWritableBridge.readString(34, __dbResults);
    this.snmvax_events = JdbcWritableBridge.readString(35, __dbResults);
    this.wpj4el_events = JdbcWritableBridge.readString(36, __dbResults);
    this.x1g8p2_events = JdbcWritableBridge.readString(37, __dbResults);
    this.ylmlgd_revenue = JdbcWritableBridge.readString(38, __dbResults);
    this.ylmlgd_events = JdbcWritableBridge.readString(39, __dbResults);
    this.ylmlgd_revenue_per_event = JdbcWritableBridge.readString(40, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.tracker_token = JdbcWritableBridge.readString(1, __dbResults);
    this.network = JdbcWritableBridge.readString(2, __dbResults);
    this.campaign = JdbcWritableBridge.readString(3, __dbResults);
    this.adgroup = JdbcWritableBridge.readString(4, __dbResults);
    this.creative = JdbcWritableBridge.readString(5, __dbResults);
    this.date = JdbcWritableBridge.readString(6, __dbResults);
    this.clicks = JdbcWritableBridge.readString(7, __dbResults);
    this.impressions = JdbcWritableBridge.readString(8, __dbResults);
    this.installs = JdbcWritableBridge.readString(9, __dbResults);
    this.click_conversion_rate = JdbcWritableBridge.readString(10, __dbResults);
    this.ctr = JdbcWritableBridge.readString(11, __dbResults);
    this.impression_conversion_rate = JdbcWritableBridge.readString(12, __dbResults);
    this.reattributions = JdbcWritableBridge.readString(13, __dbResults);
    this.sessions = JdbcWritableBridge.readString(14, __dbResults);
    this.revenue_events = JdbcWritableBridge.readString(15, __dbResults);
    this.revenue = JdbcWritableBridge.readString(16, __dbResults);
    this.daus = JdbcWritableBridge.readString(17, __dbResults);
    this.waus = JdbcWritableBridge.readString(18, __dbResults);
    this.maus = JdbcWritableBridge.readString(19, __dbResults);
    this._22o1ir_events = JdbcWritableBridge.readString(20, __dbResults);
    this._2t2mp6_events = JdbcWritableBridge.readString(21, __dbResults);
    this._87hoor_events = JdbcWritableBridge.readString(22, __dbResults);
    this._96qx7c_events = JdbcWritableBridge.readString(23, __dbResults);
    this.bcftn0_events = JdbcWritableBridge.readString(24, __dbResults);
    this.dws9tj_events = JdbcWritableBridge.readString(25, __dbResults);
    this.ik4edh_revenue = JdbcWritableBridge.readString(26, __dbResults);
    this.ik4edh_events = JdbcWritableBridge.readString(27, __dbResults);
    this.ik4edh_revenue_per_event = JdbcWritableBridge.readString(28, __dbResults);
    this.kfg79g_events = JdbcWritableBridge.readString(29, __dbResults);
    this.lhfwnt_events = JdbcWritableBridge.readString(30, __dbResults);
    this.o9e3bk_events = JdbcWritableBridge.readString(31, __dbResults);
    this.smho8i_revenue = JdbcWritableBridge.readString(32, __dbResults);
    this.smho8i_events = JdbcWritableBridge.readString(33, __dbResults);
    this.smho8i_revenue_per_event = JdbcWritableBridge.readString(34, __dbResults);
    this.snmvax_events = JdbcWritableBridge.readString(35, __dbResults);
    this.wpj4el_events = JdbcWritableBridge.readString(36, __dbResults);
    this.x1g8p2_events = JdbcWritableBridge.readString(37, __dbResults);
    this.ylmlgd_revenue = JdbcWritableBridge.readString(38, __dbResults);
    this.ylmlgd_events = JdbcWritableBridge.readString(39, __dbResults);
    this.ylmlgd_revenue_per_event = JdbcWritableBridge.readString(40, __dbResults);
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
    JdbcWritableBridge.writeString(tracker_token, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(network, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(campaign, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(adgroup, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(creative, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(date, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(clicks, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(impressions, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(installs, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(click_conversion_rate, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ctr, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(impression_conversion_rate, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(reattributions, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(sessions, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(revenue_events, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(revenue, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(daus, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(waus, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(maus, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(_22o1ir_events, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(_2t2mp6_events, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(_87hoor_events, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(_96qx7c_events, 23 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(bcftn0_events, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(dws9tj_events, 25 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ik4edh_revenue, 26 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ik4edh_events, 27 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ik4edh_revenue_per_event, 28 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(kfg79g_events, 29 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(lhfwnt_events, 30 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(o9e3bk_events, 31 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(smho8i_revenue, 32 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(smho8i_events, 33 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(smho8i_revenue_per_event, 34 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(snmvax_events, 35 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(wpj4el_events, 36 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(x1g8p2_events, 37 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ylmlgd_revenue, 38 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ylmlgd_events, 39 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ylmlgd_revenue_per_event, 40 + __off, 12, __dbStmt);
    return 40;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(tracker_token, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(network, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(campaign, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(adgroup, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(creative, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(date, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(clicks, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(impressions, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(installs, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(click_conversion_rate, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ctr, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(impression_conversion_rate, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(reattributions, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(sessions, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(revenue_events, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(revenue, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(daus, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(waus, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(maus, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(_22o1ir_events, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(_2t2mp6_events, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(_87hoor_events, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(_96qx7c_events, 23 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(bcftn0_events, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(dws9tj_events, 25 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ik4edh_revenue, 26 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ik4edh_events, 27 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ik4edh_revenue_per_event, 28 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(kfg79g_events, 29 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(lhfwnt_events, 30 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(o9e3bk_events, 31 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(smho8i_revenue, 32 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(smho8i_events, 33 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(smho8i_revenue_per_event, 34 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(snmvax_events, 35 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(wpj4el_events, 36 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(x1g8p2_events, 37 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ylmlgd_revenue, 38 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ylmlgd_events, 39 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ylmlgd_revenue_per_event, 40 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.tracker_token = null;
    } else {
    this.tracker_token = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.network = null;
    } else {
    this.network = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.campaign = null;
    } else {
    this.campaign = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.adgroup = null;
    } else {
    this.adgroup = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.creative = null;
    } else {
    this.creative = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.date = null;
    } else {
    this.date = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.clicks = null;
    } else {
    this.clicks = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.impressions = null;
    } else {
    this.impressions = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.installs = null;
    } else {
    this.installs = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.click_conversion_rate = null;
    } else {
    this.click_conversion_rate = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ctr = null;
    } else {
    this.ctr = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.impression_conversion_rate = null;
    } else {
    this.impression_conversion_rate = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.reattributions = null;
    } else {
    this.reattributions = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.sessions = null;
    } else {
    this.sessions = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.revenue_events = null;
    } else {
    this.revenue_events = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.revenue = null;
    } else {
    this.revenue = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.daus = null;
    } else {
    this.daus = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.waus = null;
    } else {
    this.waus = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.maus = null;
    } else {
    this.maus = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this._22o1ir_events = null;
    } else {
    this._22o1ir_events = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this._2t2mp6_events = null;
    } else {
    this._2t2mp6_events = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this._87hoor_events = null;
    } else {
    this._87hoor_events = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this._96qx7c_events = null;
    } else {
    this._96qx7c_events = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.bcftn0_events = null;
    } else {
    this.bcftn0_events = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.dws9tj_events = null;
    } else {
    this.dws9tj_events = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ik4edh_revenue = null;
    } else {
    this.ik4edh_revenue = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ik4edh_events = null;
    } else {
    this.ik4edh_events = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ik4edh_revenue_per_event = null;
    } else {
    this.ik4edh_revenue_per_event = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.kfg79g_events = null;
    } else {
    this.kfg79g_events = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.lhfwnt_events = null;
    } else {
    this.lhfwnt_events = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.o9e3bk_events = null;
    } else {
    this.o9e3bk_events = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.smho8i_revenue = null;
    } else {
    this.smho8i_revenue = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.smho8i_events = null;
    } else {
    this.smho8i_events = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.smho8i_revenue_per_event = null;
    } else {
    this.smho8i_revenue_per_event = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.snmvax_events = null;
    } else {
    this.snmvax_events = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.wpj4el_events = null;
    } else {
    this.wpj4el_events = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.x1g8p2_events = null;
    } else {
    this.x1g8p2_events = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ylmlgd_revenue = null;
    } else {
    this.ylmlgd_revenue = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ylmlgd_events = null;
    } else {
    this.ylmlgd_events = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ylmlgd_revenue_per_event = null;
    } else {
    this.ylmlgd_revenue_per_event = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.tracker_token) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, tracker_token);
    }
    if (null == this.network) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, network);
    }
    if (null == this.campaign) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, campaign);
    }
    if (null == this.adgroup) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, adgroup);
    }
    if (null == this.creative) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, creative);
    }
    if (null == this.date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, date);
    }
    if (null == this.clicks) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, clicks);
    }
    if (null == this.impressions) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, impressions);
    }
    if (null == this.installs) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, installs);
    }
    if (null == this.click_conversion_rate) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, click_conversion_rate);
    }
    if (null == this.ctr) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ctr);
    }
    if (null == this.impression_conversion_rate) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, impression_conversion_rate);
    }
    if (null == this.reattributions) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, reattributions);
    }
    if (null == this.sessions) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, sessions);
    }
    if (null == this.revenue_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, revenue_events);
    }
    if (null == this.revenue) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, revenue);
    }
    if (null == this.daus) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, daus);
    }
    if (null == this.waus) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, waus);
    }
    if (null == this.maus) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, maus);
    }
    if (null == this._22o1ir_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, _22o1ir_events);
    }
    if (null == this._2t2mp6_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, _2t2mp6_events);
    }
    if (null == this._87hoor_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, _87hoor_events);
    }
    if (null == this._96qx7c_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, _96qx7c_events);
    }
    if (null == this.bcftn0_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, bcftn0_events);
    }
    if (null == this.dws9tj_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, dws9tj_events);
    }
    if (null == this.ik4edh_revenue) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ik4edh_revenue);
    }
    if (null == this.ik4edh_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ik4edh_events);
    }
    if (null == this.ik4edh_revenue_per_event) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ik4edh_revenue_per_event);
    }
    if (null == this.kfg79g_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, kfg79g_events);
    }
    if (null == this.lhfwnt_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, lhfwnt_events);
    }
    if (null == this.o9e3bk_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, o9e3bk_events);
    }
    if (null == this.smho8i_revenue) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, smho8i_revenue);
    }
    if (null == this.smho8i_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, smho8i_events);
    }
    if (null == this.smho8i_revenue_per_event) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, smho8i_revenue_per_event);
    }
    if (null == this.snmvax_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, snmvax_events);
    }
    if (null == this.wpj4el_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, wpj4el_events);
    }
    if (null == this.x1g8p2_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, x1g8p2_events);
    }
    if (null == this.ylmlgd_revenue) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ylmlgd_revenue);
    }
    if (null == this.ylmlgd_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ylmlgd_events);
    }
    if (null == this.ylmlgd_revenue_per_event) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ylmlgd_revenue_per_event);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.tracker_token) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, tracker_token);
    }
    if (null == this.network) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, network);
    }
    if (null == this.campaign) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, campaign);
    }
    if (null == this.adgroup) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, adgroup);
    }
    if (null == this.creative) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, creative);
    }
    if (null == this.date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, date);
    }
    if (null == this.clicks) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, clicks);
    }
    if (null == this.impressions) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, impressions);
    }
    if (null == this.installs) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, installs);
    }
    if (null == this.click_conversion_rate) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, click_conversion_rate);
    }
    if (null == this.ctr) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ctr);
    }
    if (null == this.impression_conversion_rate) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, impression_conversion_rate);
    }
    if (null == this.reattributions) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, reattributions);
    }
    if (null == this.sessions) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, sessions);
    }
    if (null == this.revenue_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, revenue_events);
    }
    if (null == this.revenue) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, revenue);
    }
    if (null == this.daus) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, daus);
    }
    if (null == this.waus) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, waus);
    }
    if (null == this.maus) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, maus);
    }
    if (null == this._22o1ir_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, _22o1ir_events);
    }
    if (null == this._2t2mp6_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, _2t2mp6_events);
    }
    if (null == this._87hoor_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, _87hoor_events);
    }
    if (null == this._96qx7c_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, _96qx7c_events);
    }
    if (null == this.bcftn0_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, bcftn0_events);
    }
    if (null == this.dws9tj_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, dws9tj_events);
    }
    if (null == this.ik4edh_revenue) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ik4edh_revenue);
    }
    if (null == this.ik4edh_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ik4edh_events);
    }
    if (null == this.ik4edh_revenue_per_event) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ik4edh_revenue_per_event);
    }
    if (null == this.kfg79g_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, kfg79g_events);
    }
    if (null == this.lhfwnt_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, lhfwnt_events);
    }
    if (null == this.o9e3bk_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, o9e3bk_events);
    }
    if (null == this.smho8i_revenue) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, smho8i_revenue);
    }
    if (null == this.smho8i_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, smho8i_events);
    }
    if (null == this.smho8i_revenue_per_event) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, smho8i_revenue_per_event);
    }
    if (null == this.snmvax_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, snmvax_events);
    }
    if (null == this.wpj4el_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, wpj4el_events);
    }
    if (null == this.x1g8p2_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, x1g8p2_events);
    }
    if (null == this.ylmlgd_revenue) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ylmlgd_revenue);
    }
    if (null == this.ylmlgd_events) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ylmlgd_events);
    }
    if (null == this.ylmlgd_revenue_per_event) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ylmlgd_revenue_per_event);
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
    __sb.append(FieldFormatter.escapeAndEnclose(tracker_token==null?"null":tracker_token, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(network==null?"null":network, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(campaign==null?"null":campaign, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(adgroup==null?"null":adgroup, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(creative==null?"null":creative, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(date==null?"null":date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(clicks==null?"null":clicks, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(impressions==null?"null":impressions, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(installs==null?"null":installs, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(click_conversion_rate==null?"null":click_conversion_rate, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ctr==null?"null":ctr, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(impression_conversion_rate==null?"null":impression_conversion_rate, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(reattributions==null?"null":reattributions, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(sessions==null?"null":sessions, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(revenue_events==null?"null":revenue_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(revenue==null?"null":revenue, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(daus==null?"null":daus, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(waus==null?"null":waus, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(maus==null?"null":maus, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(_22o1ir_events==null?"null":_22o1ir_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(_2t2mp6_events==null?"null":_2t2mp6_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(_87hoor_events==null?"null":_87hoor_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(_96qx7c_events==null?"null":_96qx7c_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(bcftn0_events==null?"null":bcftn0_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dws9tj_events==null?"null":dws9tj_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ik4edh_revenue==null?"null":ik4edh_revenue, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ik4edh_events==null?"null":ik4edh_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ik4edh_revenue_per_event==null?"null":ik4edh_revenue_per_event, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(kfg79g_events==null?"null":kfg79g_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(lhfwnt_events==null?"null":lhfwnt_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(o9e3bk_events==null?"null":o9e3bk_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(smho8i_revenue==null?"null":smho8i_revenue, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(smho8i_events==null?"null":smho8i_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(smho8i_revenue_per_event==null?"null":smho8i_revenue_per_event, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(snmvax_events==null?"null":snmvax_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wpj4el_events==null?"null":wpj4el_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(x1g8p2_events==null?"null":x1g8p2_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ylmlgd_revenue==null?"null":ylmlgd_revenue, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ylmlgd_events==null?"null":ylmlgd_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ylmlgd_revenue_per_event==null?"null":ylmlgd_revenue_per_event, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(tracker_token==null?"null":tracker_token, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(network==null?"null":network, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(campaign==null?"null":campaign, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(adgroup==null?"null":adgroup, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(creative==null?"null":creative, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(date==null?"null":date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(clicks==null?"null":clicks, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(impressions==null?"null":impressions, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(installs==null?"null":installs, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(click_conversion_rate==null?"null":click_conversion_rate, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ctr==null?"null":ctr, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(impression_conversion_rate==null?"null":impression_conversion_rate, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(reattributions==null?"null":reattributions, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(sessions==null?"null":sessions, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(revenue_events==null?"null":revenue_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(revenue==null?"null":revenue, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(daus==null?"null":daus, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(waus==null?"null":waus, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(maus==null?"null":maus, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(_22o1ir_events==null?"null":_22o1ir_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(_2t2mp6_events==null?"null":_2t2mp6_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(_87hoor_events==null?"null":_87hoor_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(_96qx7c_events==null?"null":_96qx7c_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(bcftn0_events==null?"null":bcftn0_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dws9tj_events==null?"null":dws9tj_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ik4edh_revenue==null?"null":ik4edh_revenue, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ik4edh_events==null?"null":ik4edh_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ik4edh_revenue_per_event==null?"null":ik4edh_revenue_per_event, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(kfg79g_events==null?"null":kfg79g_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(lhfwnt_events==null?"null":lhfwnt_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(o9e3bk_events==null?"null":o9e3bk_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(smho8i_revenue==null?"null":smho8i_revenue, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(smho8i_events==null?"null":smho8i_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(smho8i_revenue_per_event==null?"null":smho8i_revenue_per_event, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(snmvax_events==null?"null":snmvax_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wpj4el_events==null?"null":wpj4el_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(x1g8p2_events==null?"null":x1g8p2_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ylmlgd_revenue==null?"null":ylmlgd_revenue, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ylmlgd_events==null?"null":ylmlgd_events, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ylmlgd_revenue_per_event==null?"null":ylmlgd_revenue_per_event, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
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
    if (__cur_str.equals("null")) { this.tracker_token = null; } else {
      this.tracker_token = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.network = null; } else {
      this.network = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.campaign = null; } else {
      this.campaign = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.adgroup = null; } else {
      this.adgroup = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.creative = null; } else {
      this.creative = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.date = null; } else {
      this.date = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.clicks = null; } else {
      this.clicks = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.impressions = null; } else {
      this.impressions = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.installs = null; } else {
      this.installs = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.click_conversion_rate = null; } else {
      this.click_conversion_rate = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ctr = null; } else {
      this.ctr = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.impression_conversion_rate = null; } else {
      this.impression_conversion_rate = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.reattributions = null; } else {
      this.reattributions = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.sessions = null; } else {
      this.sessions = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.revenue_events = null; } else {
      this.revenue_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.revenue = null; } else {
      this.revenue = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.daus = null; } else {
      this.daus = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.waus = null; } else {
      this.waus = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.maus = null; } else {
      this.maus = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this._22o1ir_events = null; } else {
      this._22o1ir_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this._2t2mp6_events = null; } else {
      this._2t2mp6_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this._87hoor_events = null; } else {
      this._87hoor_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this._96qx7c_events = null; } else {
      this._96qx7c_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.bcftn0_events = null; } else {
      this.bcftn0_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.dws9tj_events = null; } else {
      this.dws9tj_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ik4edh_revenue = null; } else {
      this.ik4edh_revenue = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ik4edh_events = null; } else {
      this.ik4edh_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ik4edh_revenue_per_event = null; } else {
      this.ik4edh_revenue_per_event = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.kfg79g_events = null; } else {
      this.kfg79g_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.lhfwnt_events = null; } else {
      this.lhfwnt_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.o9e3bk_events = null; } else {
      this.o9e3bk_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.smho8i_revenue = null; } else {
      this.smho8i_revenue = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.smho8i_events = null; } else {
      this.smho8i_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.smho8i_revenue_per_event = null; } else {
      this.smho8i_revenue_per_event = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.snmvax_events = null; } else {
      this.snmvax_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.wpj4el_events = null; } else {
      this.wpj4el_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.x1g8p2_events = null; } else {
      this.x1g8p2_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ylmlgd_revenue = null; } else {
      this.ylmlgd_revenue = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ylmlgd_events = null; } else {
      this.ylmlgd_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ylmlgd_revenue_per_event = null; } else {
      this.ylmlgd_revenue_per_event = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.tracker_token = null; } else {
      this.tracker_token = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.network = null; } else {
      this.network = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.campaign = null; } else {
      this.campaign = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.adgroup = null; } else {
      this.adgroup = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.creative = null; } else {
      this.creative = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.date = null; } else {
      this.date = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.clicks = null; } else {
      this.clicks = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.impressions = null; } else {
      this.impressions = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.installs = null; } else {
      this.installs = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.click_conversion_rate = null; } else {
      this.click_conversion_rate = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ctr = null; } else {
      this.ctr = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.impression_conversion_rate = null; } else {
      this.impression_conversion_rate = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.reattributions = null; } else {
      this.reattributions = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.sessions = null; } else {
      this.sessions = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.revenue_events = null; } else {
      this.revenue_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.revenue = null; } else {
      this.revenue = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.daus = null; } else {
      this.daus = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.waus = null; } else {
      this.waus = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.maus = null; } else {
      this.maus = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this._22o1ir_events = null; } else {
      this._22o1ir_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this._2t2mp6_events = null; } else {
      this._2t2mp6_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this._87hoor_events = null; } else {
      this._87hoor_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this._96qx7c_events = null; } else {
      this._96qx7c_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.bcftn0_events = null; } else {
      this.bcftn0_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.dws9tj_events = null; } else {
      this.dws9tj_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ik4edh_revenue = null; } else {
      this.ik4edh_revenue = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ik4edh_events = null; } else {
      this.ik4edh_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ik4edh_revenue_per_event = null; } else {
      this.ik4edh_revenue_per_event = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.kfg79g_events = null; } else {
      this.kfg79g_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.lhfwnt_events = null; } else {
      this.lhfwnt_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.o9e3bk_events = null; } else {
      this.o9e3bk_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.smho8i_revenue = null; } else {
      this.smho8i_revenue = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.smho8i_events = null; } else {
      this.smho8i_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.smho8i_revenue_per_event = null; } else {
      this.smho8i_revenue_per_event = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.snmvax_events = null; } else {
      this.snmvax_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.wpj4el_events = null; } else {
      this.wpj4el_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.x1g8p2_events = null; } else {
      this.x1g8p2_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ylmlgd_revenue = null; } else {
      this.ylmlgd_revenue = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ylmlgd_events = null; } else {
      this.ylmlgd_events = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ylmlgd_revenue_per_event = null; } else {
      this.ylmlgd_revenue_per_event = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    adjust_data o = (adjust_data) super.clone();
    return o;
  }

  public void clone0(adjust_data o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("tracker_token", this.tracker_token);
    __sqoop$field_map.put("network", this.network);
    __sqoop$field_map.put("campaign", this.campaign);
    __sqoop$field_map.put("adgroup", this.adgroup);
    __sqoop$field_map.put("creative", this.creative);
    __sqoop$field_map.put("date", this.date);
    __sqoop$field_map.put("clicks", this.clicks);
    __sqoop$field_map.put("impressions", this.impressions);
    __sqoop$field_map.put("installs", this.installs);
    __sqoop$field_map.put("click_conversion_rate", this.click_conversion_rate);
    __sqoop$field_map.put("ctr", this.ctr);
    __sqoop$field_map.put("impression_conversion_rate", this.impression_conversion_rate);
    __sqoop$field_map.put("reattributions", this.reattributions);
    __sqoop$field_map.put("sessions", this.sessions);
    __sqoop$field_map.put("revenue_events", this.revenue_events);
    __sqoop$field_map.put("revenue", this.revenue);
    __sqoop$field_map.put("daus", this.daus);
    __sqoop$field_map.put("waus", this.waus);
    __sqoop$field_map.put("maus", this.maus);
    __sqoop$field_map.put("22o1ir_events", this._22o1ir_events);
    __sqoop$field_map.put("2t2mp6_events", this._2t2mp6_events);
    __sqoop$field_map.put("87hoor_events", this._87hoor_events);
    __sqoop$field_map.put("96qx7c_events", this._96qx7c_events);
    __sqoop$field_map.put("bcftn0_events", this.bcftn0_events);
    __sqoop$field_map.put("dws9tj_events", this.dws9tj_events);
    __sqoop$field_map.put("ik4edh_revenue", this.ik4edh_revenue);
    __sqoop$field_map.put("ik4edh_events", this.ik4edh_events);
    __sqoop$field_map.put("ik4edh_revenue_per_event", this.ik4edh_revenue_per_event);
    __sqoop$field_map.put("kfg79g_events", this.kfg79g_events);
    __sqoop$field_map.put("lhfwnt_events", this.lhfwnt_events);
    __sqoop$field_map.put("o9e3bk_events", this.o9e3bk_events);
    __sqoop$field_map.put("smho8i_revenue", this.smho8i_revenue);
    __sqoop$field_map.put("smho8i_events", this.smho8i_events);
    __sqoop$field_map.put("smho8i_revenue_per_event", this.smho8i_revenue_per_event);
    __sqoop$field_map.put("snmvax_events", this.snmvax_events);
    __sqoop$field_map.put("wpj4el_events", this.wpj4el_events);
    __sqoop$field_map.put("x1g8p2_events", this.x1g8p2_events);
    __sqoop$field_map.put("ylmlgd_revenue", this.ylmlgd_revenue);
    __sqoop$field_map.put("ylmlgd_events", this.ylmlgd_events);
    __sqoop$field_map.put("ylmlgd_revenue_per_event", this.ylmlgd_revenue_per_event);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("tracker_token", this.tracker_token);
    __sqoop$field_map.put("network", this.network);
    __sqoop$field_map.put("campaign", this.campaign);
    __sqoop$field_map.put("adgroup", this.adgroup);
    __sqoop$field_map.put("creative", this.creative);
    __sqoop$field_map.put("date", this.date);
    __sqoop$field_map.put("clicks", this.clicks);
    __sqoop$field_map.put("impressions", this.impressions);
    __sqoop$field_map.put("installs", this.installs);
    __sqoop$field_map.put("click_conversion_rate", this.click_conversion_rate);
    __sqoop$field_map.put("ctr", this.ctr);
    __sqoop$field_map.put("impression_conversion_rate", this.impression_conversion_rate);
    __sqoop$field_map.put("reattributions", this.reattributions);
    __sqoop$field_map.put("sessions", this.sessions);
    __sqoop$field_map.put("revenue_events", this.revenue_events);
    __sqoop$field_map.put("revenue", this.revenue);
    __sqoop$field_map.put("daus", this.daus);
    __sqoop$field_map.put("waus", this.waus);
    __sqoop$field_map.put("maus", this.maus);
    __sqoop$field_map.put("22o1ir_events", this._22o1ir_events);
    __sqoop$field_map.put("2t2mp6_events", this._2t2mp6_events);
    __sqoop$field_map.put("87hoor_events", this._87hoor_events);
    __sqoop$field_map.put("96qx7c_events", this._96qx7c_events);
    __sqoop$field_map.put("bcftn0_events", this.bcftn0_events);
    __sqoop$field_map.put("dws9tj_events", this.dws9tj_events);
    __sqoop$field_map.put("ik4edh_revenue", this.ik4edh_revenue);
    __sqoop$field_map.put("ik4edh_events", this.ik4edh_events);
    __sqoop$field_map.put("ik4edh_revenue_per_event", this.ik4edh_revenue_per_event);
    __sqoop$field_map.put("kfg79g_events", this.kfg79g_events);
    __sqoop$field_map.put("lhfwnt_events", this.lhfwnt_events);
    __sqoop$field_map.put("o9e3bk_events", this.o9e3bk_events);
    __sqoop$field_map.put("smho8i_revenue", this.smho8i_revenue);
    __sqoop$field_map.put("smho8i_events", this.smho8i_events);
    __sqoop$field_map.put("smho8i_revenue_per_event", this.smho8i_revenue_per_event);
    __sqoop$field_map.put("snmvax_events", this.snmvax_events);
    __sqoop$field_map.put("wpj4el_events", this.wpj4el_events);
    __sqoop$field_map.put("x1g8p2_events", this.x1g8p2_events);
    __sqoop$field_map.put("ylmlgd_revenue", this.ylmlgd_revenue);
    __sqoop$field_map.put("ylmlgd_events", this.ylmlgd_events);
    __sqoop$field_map.put("ylmlgd_revenue_per_event", this.ylmlgd_revenue_per_event);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
