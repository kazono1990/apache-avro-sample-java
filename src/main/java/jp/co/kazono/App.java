package jp.co.kazono;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class App {
    public static void main(String[] args) throws IOException {

        // create user using default constructor and method.
        User user1 = new User();
        user1.setName("kazono");
        user1.setFavoriteNumber(128);
        user1.setFavoriteColor("Red");

        // create user using another constructor.
        User user2 = new User("kazono", 128, "Red");

        // create user using builder.
        User user3 = User.newBuilder()
            .setName("kazono")
            .setFavoriteNumber(128)
            .setFavoriteColor("Red")
            .build();

        System.out.println(user1);
        System.out.println(user2);
        System.out.println(user3);


        // serialize user1 ~ 3 to disk.
        // DatumWriter : converts Java objects into an in-memory serialized format.
        // SpecificDatumWriter : used with generated classes and extracts the schema from the specified generated type.
        File schema = new File("users.avro");
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<>();
        DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userDatumWriter);
        dataFileWriter.create(user1.getSchema(), schema);
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();

        // deserialize Users from disk.
        DatumReader<User> userDatumReader = new SpecificDatumReader<>();
        DataFileReader<User> dataFileReader = new DataFileReader<User>(schema, userDatumReader);
        User user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }


        // GenericRecord sample
        GenericRecordSample genericRecordSample = new GenericRecordSample("kazono", 256, "Black");
        genericRecordSample.serialize();
        genericRecordSample.deserialize();
    }
}
