// ORM class for table 'dim_hotel_mdm'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Wed Jun 06 07:00:57 UTC 2018
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

public class dim_hotel_mdm extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  protected ResultSet __cur_result_set;
  private String dim_hotel_id;
  public String get_dim_hotel_id() {
    return dim_hotel_id;
  }
  public void set_dim_hotel_id(String dim_hotel_id) {
    this.dim_hotel_id = dim_hotel_id;
  }
  public dim_hotel_mdm with_dim_hotel_id(String dim_hotel_id) {
    this.dim_hotel_id = dim_hotel_id;
    return this;
  }
  private String name;
  public String get_name() {
    return name;
  }
  public void set_name(String name) {
    this.name = name;
  }
  public dim_hotel_mdm with_name(String name) {
    this.name = name;
    return this;
  }
  private String address;
  public String get_address() {
    return address;
  }
  public void set_address(String address) {
    this.address = address;
  }
  public dim_hotel_mdm with_address(String address) {
    this.address = address;
    return this;
  }
  private String city;
  public String get_city() {
    return city;
  }
  public void set_city(String city) {
    this.city = city;
  }
  public dim_hotel_mdm with_city(String city) {
    this.city = city;
    return this;
  }
  private String district;
  public String get_district() {
    return district;
  }
  public void set_district(String district) {
    this.district = district;
  }
  public dim_hotel_mdm with_district(String district) {
    this.district = district;
    return this;
  }
  private String country_code;
  public String get_country_code() {
    return country_code;
  }
  public void set_country_code(String country_code) {
    this.country_code = country_code;
  }
  public dim_hotel_mdm with_country_code(String country_code) {
    this.country_code = country_code;
    return this;
  }
  private String star_rating;
  public String get_star_rating() {
    return star_rating;
  }
  public void set_star_rating(String star_rating) {
    this.star_rating = star_rating;
  }
  public dim_hotel_mdm with_star_rating(String star_rating) {
    this.star_rating = star_rating;
    return this;
  }
  private String longitude;
  public String get_longitude() {
    return longitude;
  }
  public void set_longitude(String longitude) {
    this.longitude = longitude;
  }
  public dim_hotel_mdm with_longitude(String longitude) {
    this.longitude = longitude;
    return this;
  }
  private String latitude;
  public String get_latitude() {
    return latitude;
  }
  public void set_latitude(String latitude) {
    this.latitude = latitude;
  }
  public dim_hotel_mdm with_latitude(String latitude) {
    this.latitude = latitude;
    return this;
  }
  private String zipcode;
  public String get_zipcode() {
    return zipcode;
  }
  public void set_zipcode(String zipcode) {
    this.zipcode = zipcode;
  }
  public dim_hotel_mdm with_zipcode(String zipcode) {
    this.zipcode = zipcode;
    return this;
  }
  private String chain;
  public String get_chain() {
    return chain;
  }
  public void set_chain(String chain) {
    this.chain = chain;
  }
  public dim_hotel_mdm with_chain(String chain) {
    this.chain = chain;
    return this;
  }
  private String chain_name;
  public String get_chain_name() {
    return chain_name;
  }
  public void set_chain_name(String chain_name) {
    this.chain_name = chain_name;
  }
  public dim_hotel_mdm with_chain_name(String chain_name) {
    this.chain_name = chain_name;
    return this;
  }
  private String hotel_brand;
  public String get_hotel_brand() {
    return hotel_brand;
  }
  public void set_hotel_brand(String hotel_brand) {
    this.hotel_brand = hotel_brand;
  }
  public dim_hotel_mdm with_hotel_brand(String hotel_brand) {
    this.hotel_brand = hotel_brand;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof dim_hotel_mdm)) {
      return false;
    }
    dim_hotel_mdm that = (dim_hotel_mdm) o;
    boolean equal = true;
    equal = equal && (this.dim_hotel_id == null ? that.dim_hotel_id == null : this.dim_hotel_id.equals(that.dim_hotel_id));
    equal = equal && (this.name == null ? that.name == null : this.name.equals(that.name));
    equal = equal && (this.address == null ? that.address == null : this.address.equals(that.address));
    equal = equal && (this.city == null ? that.city == null : this.city.equals(that.city));
    equal = equal && (this.district == null ? that.district == null : this.district.equals(that.district));
    equal = equal && (this.country_code == null ? that.country_code == null : this.country_code.equals(that.country_code));
    equal = equal && (this.star_rating == null ? that.star_rating == null : this.star_rating.equals(that.star_rating));
    equal = equal && (this.longitude == null ? that.longitude == null : this.longitude.equals(that.longitude));
    equal = equal && (this.latitude == null ? that.latitude == null : this.latitude.equals(that.latitude));
    equal = equal && (this.zipcode == null ? that.zipcode == null : this.zipcode.equals(that.zipcode));
    equal = equal && (this.chain == null ? that.chain == null : this.chain.equals(that.chain));
    equal = equal && (this.chain_name == null ? that.chain_name == null : this.chain_name.equals(that.chain_name));
    equal = equal && (this.hotel_brand == null ? that.hotel_brand == null : this.hotel_brand.equals(that.hotel_brand));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof dim_hotel_mdm)) {
      return false;
    }
    dim_hotel_mdm that = (dim_hotel_mdm) o;
    boolean equal = true;
    equal = equal && (this.dim_hotel_id == null ? that.dim_hotel_id == null : this.dim_hotel_id.equals(that.dim_hotel_id));
    equal = equal && (this.name == null ? that.name == null : this.name.equals(that.name));
    equal = equal && (this.address == null ? that.address == null : this.address.equals(that.address));
    equal = equal && (this.city == null ? that.city == null : this.city.equals(that.city));
    equal = equal && (this.district == null ? that.district == null : this.district.equals(that.district));
    equal = equal && (this.country_code == null ? that.country_code == null : this.country_code.equals(that.country_code));
    equal = equal && (this.star_rating == null ? that.star_rating == null : this.star_rating.equals(that.star_rating));
    equal = equal && (this.longitude == null ? that.longitude == null : this.longitude.equals(that.longitude));
    equal = equal && (this.latitude == null ? that.latitude == null : this.latitude.equals(that.latitude));
    equal = equal && (this.zipcode == null ? that.zipcode == null : this.zipcode.equals(that.zipcode));
    equal = equal && (this.chain == null ? that.chain == null : this.chain.equals(that.chain));
    equal = equal && (this.chain_name == null ? that.chain_name == null : this.chain_name.equals(that.chain_name));
    equal = equal && (this.hotel_brand == null ? that.hotel_brand == null : this.hotel_brand.equals(that.hotel_brand));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.dim_hotel_id = JdbcWritableBridge.readString(1, __dbResults);
    this.name = JdbcWritableBridge.readString(2, __dbResults);
    this.address = JdbcWritableBridge.readString(3, __dbResults);
    this.city = JdbcWritableBridge.readString(4, __dbResults);
    this.district = JdbcWritableBridge.readString(5, __dbResults);
    this.country_code = JdbcWritableBridge.readString(6, __dbResults);
    this.star_rating = JdbcWritableBridge.readString(7, __dbResults);
    this.longitude = JdbcWritableBridge.readString(8, __dbResults);
    this.latitude = JdbcWritableBridge.readString(9, __dbResults);
    this.zipcode = JdbcWritableBridge.readString(10, __dbResults);
    this.chain = JdbcWritableBridge.readString(11, __dbResults);
    this.chain_name = JdbcWritableBridge.readString(12, __dbResults);
    this.hotel_brand = JdbcWritableBridge.readString(13, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.dim_hotel_id = JdbcWritableBridge.readString(1, __dbResults);
    this.name = JdbcWritableBridge.readString(2, __dbResults);
    this.address = JdbcWritableBridge.readString(3, __dbResults);
    this.city = JdbcWritableBridge.readString(4, __dbResults);
    this.district = JdbcWritableBridge.readString(5, __dbResults);
    this.country_code = JdbcWritableBridge.readString(6, __dbResults);
    this.star_rating = JdbcWritableBridge.readString(7, __dbResults);
    this.longitude = JdbcWritableBridge.readString(8, __dbResults);
    this.latitude = JdbcWritableBridge.readString(9, __dbResults);
    this.zipcode = JdbcWritableBridge.readString(10, __dbResults);
    this.chain = JdbcWritableBridge.readString(11, __dbResults);
    this.chain_name = JdbcWritableBridge.readString(12, __dbResults);
    this.hotel_brand = JdbcWritableBridge.readString(13, __dbResults);
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
    JdbcWritableBridge.writeString(dim_hotel_id, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(name, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(address, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(city, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(district, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(country_code, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(star_rating, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(longitude, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(latitude, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(zipcode, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(chain, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(chain_name, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(hotel_brand, 13 + __off, 12, __dbStmt);
    return 13;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(dim_hotel_id, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(name, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(address, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(city, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(district, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(country_code, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(star_rating, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(longitude, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(latitude, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(zipcode, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(chain, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(chain_name, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(hotel_brand, 13 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.dim_hotel_id = null;
    } else {
    this.dim_hotel_id = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.name = null;
    } else {
    this.name = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.address = null;
    } else {
    this.address = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.city = null;
    } else {
    this.city = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.district = null;
    } else {
    this.district = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.country_code = null;
    } else {
    this.country_code = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.star_rating = null;
    } else {
    this.star_rating = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.longitude = null;
    } else {
    this.longitude = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.latitude = null;
    } else {
    this.latitude = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.zipcode = null;
    } else {
    this.zipcode = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.chain = null;
    } else {
    this.chain = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.chain_name = null;
    } else {
    this.chain_name = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.hotel_brand = null;
    } else {
    this.hotel_brand = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.dim_hotel_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, dim_hotel_id);
    }
    if (null == this.name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, name);
    }
    if (null == this.address) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, address);
    }
    if (null == this.city) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, city);
    }
    if (null == this.district) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, district);
    }
    if (null == this.country_code) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, country_code);
    }
    if (null == this.star_rating) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, star_rating);
    }
    if (null == this.longitude) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, longitude);
    }
    if (null == this.latitude) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, latitude);
    }
    if (null == this.zipcode) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, zipcode);
    }
    if (null == this.chain) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, chain);
    }
    if (null == this.chain_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, chain_name);
    }
    if (null == this.hotel_brand) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, hotel_brand);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.dim_hotel_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, dim_hotel_id);
    }
    if (null == this.name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, name);
    }
    if (null == this.address) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, address);
    }
    if (null == this.city) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, city);
    }
    if (null == this.district) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, district);
    }
    if (null == this.country_code) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, country_code);
    }
    if (null == this.star_rating) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, star_rating);
    }
    if (null == this.longitude) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, longitude);
    }
    if (null == this.latitude) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, latitude);
    }
    if (null == this.zipcode) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, zipcode);
    }
    if (null == this.chain) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, chain);
    }
    if (null == this.chain_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, chain_name);
    }
    if (null == this.hotel_brand) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, hotel_brand);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 9, (char) 10, (char) 0, (char) 0, false);
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
    __sb.append(FieldFormatter.escapeAndEnclose(dim_hotel_id==null?"null":dim_hotel_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(name==null?"null":name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(address==null?"null":address, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(city==null?"null":city, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(district==null?"null":district, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(country_code==null?"null":country_code, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(star_rating==null?"null":star_rating, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(longitude==null?"null":longitude, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(latitude==null?"null":latitude, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(zipcode==null?"null":zipcode, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(chain==null?"null":chain, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(chain_name==null?"null":chain_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(hotel_brand==null?"null":hotel_brand, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(dim_hotel_id==null?"null":dim_hotel_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(name==null?"null":name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(address==null?"null":address, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(city==null?"null":city, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(district==null?"null":district, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(country_code==null?"null":country_code, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(star_rating==null?"null":star_rating, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(longitude==null?"null":longitude, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(latitude==null?"null":latitude, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(zipcode==null?"null":zipcode, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(chain==null?"null":chain, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(chain_name==null?"null":chain_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(hotel_brand==null?"null":hotel_brand, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 9, (char) 10, (char) 0, (char) 0, false);
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
    if (__cur_str.equals("null")) { this.dim_hotel_id = null; } else {
      this.dim_hotel_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.name = null; } else {
      this.name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.address = null; } else {
      this.address = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.city = null; } else {
      this.city = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.district = null; } else {
      this.district = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.country_code = null; } else {
      this.country_code = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.star_rating = null; } else {
      this.star_rating = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.longitude = null; } else {
      this.longitude = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.latitude = null; } else {
      this.latitude = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.zipcode = null; } else {
      this.zipcode = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.chain = null; } else {
      this.chain = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.chain_name = null; } else {
      this.chain_name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.hotel_brand = null; } else {
      this.hotel_brand = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.dim_hotel_id = null; } else {
      this.dim_hotel_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.name = null; } else {
      this.name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.address = null; } else {
      this.address = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.city = null; } else {
      this.city = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.district = null; } else {
      this.district = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.country_code = null; } else {
      this.country_code = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.star_rating = null; } else {
      this.star_rating = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.longitude = null; } else {
      this.longitude = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.latitude = null; } else {
      this.latitude = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.zipcode = null; } else {
      this.zipcode = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.chain = null; } else {
      this.chain = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.chain_name = null; } else {
      this.chain_name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.hotel_brand = null; } else {
      this.hotel_brand = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    dim_hotel_mdm o = (dim_hotel_mdm) super.clone();
    return o;
  }

  public void clone0(dim_hotel_mdm o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
    __sqoop$field_map.put("dim_hotel_id", this.dim_hotel_id);
    __sqoop$field_map.put("name", this.name);
    __sqoop$field_map.put("address", this.address);
    __sqoop$field_map.put("city", this.city);
    __sqoop$field_map.put("district", this.district);
    __sqoop$field_map.put("country_code", this.country_code);
    __sqoop$field_map.put("star_rating", this.star_rating);
    __sqoop$field_map.put("longitude", this.longitude);
    __sqoop$field_map.put("latitude", this.latitude);
    __sqoop$field_map.put("zipcode", this.zipcode);
    __sqoop$field_map.put("chain", this.chain);
    __sqoop$field_map.put("chain_name", this.chain_name);
    __sqoop$field_map.put("hotel_brand", this.hotel_brand);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("dim_hotel_id", this.dim_hotel_id);
    __sqoop$field_map.put("name", this.name);
    __sqoop$field_map.put("address", this.address);
    __sqoop$field_map.put("city", this.city);
    __sqoop$field_map.put("district", this.district);
    __sqoop$field_map.put("country_code", this.country_code);
    __sqoop$field_map.put("star_rating", this.star_rating);
    __sqoop$field_map.put("longitude", this.longitude);
    __sqoop$field_map.put("latitude", this.latitude);
    __sqoop$field_map.put("zipcode", this.zipcode);
    __sqoop$field_map.put("chain", this.chain);
    __sqoop$field_map.put("chain_name", this.chain_name);
    __sqoop$field_map.put("hotel_brand", this.hotel_brand);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if ("dim_hotel_id".equals(__fieldName)) {
      this.dim_hotel_id = (String) __fieldVal;
    }
    else    if ("name".equals(__fieldName)) {
      this.name = (String) __fieldVal;
    }
    else    if ("address".equals(__fieldName)) {
      this.address = (String) __fieldVal;
    }
    else    if ("city".equals(__fieldName)) {
      this.city = (String) __fieldVal;
    }
    else    if ("district".equals(__fieldName)) {
      this.district = (String) __fieldVal;
    }
    else    if ("country_code".equals(__fieldName)) {
      this.country_code = (String) __fieldVal;
    }
    else    if ("star_rating".equals(__fieldName)) {
      this.star_rating = (String) __fieldVal;
    }
    else    if ("longitude".equals(__fieldName)) {
      this.longitude = (String) __fieldVal;
    }
    else    if ("latitude".equals(__fieldName)) {
      this.latitude = (String) __fieldVal;
    }
    else    if ("zipcode".equals(__fieldName)) {
      this.zipcode = (String) __fieldVal;
    }
    else    if ("chain".equals(__fieldName)) {
      this.chain = (String) __fieldVal;
    }
    else    if ("chain_name".equals(__fieldName)) {
      this.chain_name = (String) __fieldVal;
    }
    else    if ("hotel_brand".equals(__fieldName)) {
      this.hotel_brand = (String) __fieldVal;
    }
    else {
      throw new RuntimeException("No such field: " + __fieldName);
    }
  }
  public boolean setField0(String __fieldName, Object __fieldVal) {
    if ("dim_hotel_id".equals(__fieldName)) {
      this.dim_hotel_id = (String) __fieldVal;
      return true;
    }
    else    if ("name".equals(__fieldName)) {
      this.name = (String) __fieldVal;
      return true;
    }
    else    if ("address".equals(__fieldName)) {
      this.address = (String) __fieldVal;
      return true;
    }
    else    if ("city".equals(__fieldName)) {
      this.city = (String) __fieldVal;
      return true;
    }
    else    if ("district".equals(__fieldName)) {
      this.district = (String) __fieldVal;
      return true;
    }
    else    if ("country_code".equals(__fieldName)) {
      this.country_code = (String) __fieldVal;
      return true;
    }
    else    if ("star_rating".equals(__fieldName)) {
      this.star_rating = (String) __fieldVal;
      return true;
    }
    else    if ("longitude".equals(__fieldName)) {
      this.longitude = (String) __fieldVal;
      return true;
    }
    else    if ("latitude".equals(__fieldName)) {
      this.latitude = (String) __fieldVal;
      return true;
    }
    else    if ("zipcode".equals(__fieldName)) {
      this.zipcode = (String) __fieldVal;
      return true;
    }
    else    if ("chain".equals(__fieldName)) {
      this.chain = (String) __fieldVal;
      return true;
    }
    else    if ("chain_name".equals(__fieldName)) {
      this.chain_name = (String) __fieldVal;
      return true;
    }
    else    if ("hotel_brand".equals(__fieldName)) {
      this.hotel_brand = (String) __fieldVal;
      return true;
    }
    else {
      return false;    }
  }
}
