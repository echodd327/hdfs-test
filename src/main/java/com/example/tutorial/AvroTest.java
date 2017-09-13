package com.example.tutorial;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class AvroTest {
	 
	public static void main(String[] args) {
		PhoneNumber phoneNumber1 = PhoneNumber.newBuilder()
				.setNumber("15110241024")
				.setType(0).build();
		PhoneNumber phoneNumber2 = PhoneNumber.newBuilder()
				.setNumber("01025689654")
				.setType(1).build();
		List<PhoneNumber> phoneNumbers = new ArrayList<PhoneNumber>();
		phoneNumbers.add(phoneNumber1);
		phoneNumbers.add(phoneNumber2);
		
		Person person = Person.newBuilder()
				.setName("Dong Xicheng")
				.setEmail("dongxicheng@yahoo.com")
				.setId(111)
				.setPhone(phoneNumbers).build();
		
		File file = new File("person.txt");
		try{
			DatumWriter<Person> personDatumWriter = new SpecificDatumWriter<Person>(Person.class);
			DataFileWriter<Person> dataFileWriter = new DataFileWriter<Person>(personDatumWriter);
			dataFileWriter.create(person.getSchema(), file);
			dataFileWriter.append(person);
			dataFileWriter.close();
		}catch(Exception e){
			System.out.println("Write Error:"+e);
		}
		
		try{
			DatumReader<Person> userDatumReader = new SpecificDatumReader<Person>(Person.class);
			DataFileReader<Person> dataFileReader = new DataFileReader<Person>(file, userDatumReader);
			person = null;
			while(dataFileReader.hasNext()){
				person = dataFileReader.next(person);
				System.out.println(person);
			}
		}catch(Exception e){
			System.out.println("reader Error:"+e);
		}
	}
}
