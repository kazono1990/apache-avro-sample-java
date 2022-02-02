package jp.co.kazono;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class GenericRecordSample {

    private GenericRecord user;
    private File avroSchema = new File("src/main/resources/avro/sample.avsc");

    public GenericRecordSample (String name, int number, String color) {
        try {
            Schema schema = new Schema.Parser().parse(this.avroSchema);
            user = new GenericData.Record(schema);
            user.put("name", name);
            user.put("favorite_number", number);
            user.put("favorite_color", color);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void serialize() throws IOException {
        Schema schema = new Schema.Parser().parse(this.avroSchema);
        File file = new File("users.avro");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(this.user);
        dataFileWriter.close();
    }

    public void deserialize() throws IOException {
        File avro = new File("users.avro");
        Schema schema = new Schema.Parser().parse(this.avroSchema);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(avro, datumReader);
        GenericRecord _user = null;
        while (dataFileReader.hasNext()) {
            _user = dataFileReader.next(_user);
            System.out.println("GenericRecord : " + _user);
        }
    }
}
