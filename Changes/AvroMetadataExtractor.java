package gobblin.source.extractor.hadoop;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.filebased.FileBasedExtractor;



public class AvroMetadataExtractor extends FileBasedExtractor<Schema, GenericRecord> {

  public AvroMetadataExtractor(WorkUnitState workUnitState) {
    super(workUnitState, new AvroFsHelper(workUnitState));
  }
  
  @Override
  public Iterator<GenericRecord> downloadFile(String file) throws IOException {
    
    Path path = new Path(file);
    FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:8020"), new Configuration());
    FileStatus[] statuses = fs.listStatus(path);
    
    String schemaString = "{\"namespace\": \"example.avro\", "
        + "\"type\": \"record\","
        + "\"name\": \"User\", "
       + "\"fields\": [ "
            + "{\"name\": \"name\", \"type\": [\"string\", \"null\"]}, "
            + "{\"name\": \"timeCreated\", \"type\": [\"long\", \"null\"]},"
            + "{\"name\": \"length\", \"type\": [\"long\", \"null\"]},"
            + "{\"name\": \"modificationTime\", \"type\": [\"long\", \"null\"]},"
            + "{\"name\": \"owner\", \"type\": [\"string\", \"null\"]} "
         
       + "]"
       + "}";
    
    Schema outputSchema = new Schema.Parser().parse(schemaString);
    GenericRecord record = new GenericData.Record(outputSchema);
    record.put("name", statuses[0].getPath().toString());
    record.put("timeCreated", statuses[0].getAccessTime());
    record.put("length", statuses[0].getLen());
    record.put("modificationTime", statuses[0].getModificationTime());
    record.put("owner", statuses[0].getOwner().toString());
    
    
    
    return ImmutableList.of(record).iterator();
  }
  
  @Override
  public Schema getSchema() {
     String SCHEMA_STRING = "{\"namespace\": \"example.avro\", "
         + "\"type\": \"record\","
         + "\"name\": \"User\", "
        + "\"fields\": [ "
             + "{\"name\": \"name\", \"type\": [\"string\", \"null\"]}, "
             + "{\"name\": \"timeCreated\", \"type\": [\"long\", \"null\"]},"
             + "{\"name\": \"length\", \"type\": [\"long\", \"null\"]},"
             + "{\"name\": \"modificationTime\", \"type\": [\"long\", \"null\"]},"
             + "{\"name\": \"owner\", \"type\": [\"string\", \"null\"]} "
          
        + "]"
        + "}";
        Schema OUTPUT_SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

       return OUTPUT_SCHEMA;
  }

}
