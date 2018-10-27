package com.dhoomil.kafkabasic.avro;

import com.dhoomil.kafkabasic.avro.model.Employee;
import com.dhoomil.kafkabasic.avro.model.Project;
import com.github.javafaker.Faker;

import java.util.Arrays;

public class DataGenerator {
    private static Faker faker;

    static {
        faker = new Faker();
    }

    public static Employee generateEmployee() {
        return Employee.newBuilder()
                .setId(faker.random().nextInt(1, 10000))
                .setFname(faker.name().firstName())
                .setLname(faker.name().lastName())
                .setDateOfJoining(faker.date().birthday().getTime())
                .setConfirmationStatus(faker.bool().bool())
                .setPhoneNums(Arrays.asList(faker.phoneNumber().cellPhone(), faker.phoneNumber().cellPhone()))
                .setProject(generateProject())
                .build();
    }

    public static Project generateProject() {
        return Project.newBuilder()
                .setId(faker.random().nextInt(100))
                .setName(faker.stock().nsdqSymbol())
                .build();
    }
}
