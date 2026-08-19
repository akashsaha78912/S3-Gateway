package com.mediator.s3gateway.auth;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Small JSON-file replacement for a user-credential database.
 *
 * All write operations are synchronized so two requests cannot update the users
 * file simultaneously inside this application instance.
 */
@Service
public class JsonUserCredentialStore {

    private final ObjectMapper objectMapper;
    private final AuthProperties properties;
  //  private final AccessKeyGenerator accessKeyGenerator;

    private final Object lock = new Object();

    public JsonUserCredentialStore(
            ObjectMapper objectMapper,
            AuthProperties properties
           // AccessKeyGenerator accessKeyGenerator
    ) {
        this.objectMapper = objectMapper;
        this.properties = properties;
       // this.accessKeyGenerator = accessKeyGenerator;
    }

    /**
     * Finds an enabled or disabled user using their access key.
     */
    public Optional<UserCredential> findByAccessKey(String accessKey) {
        if (accessKey == null || accessKey.isBlank()) {
            return Optional.empty();
        }

        synchronized (lock) {
            return readDatabase().users().stream()
                    .filter(user
                            -> user.accessKeyId().equals(accessKey.trim()))
                    .findFirst();
        }
    }

    public Optional<UserCredential> findByUserId(String userId) {
        if (userId == null || userId.isBlank()) {
            return Optional.empty();
        }

        synchronized (lock) {
            return readDatabase().users().stream()
                    .filter(user -> user.userId().equals(userId.trim()))
                    .findFirst();
        }
    }

  

    // private UserDatabase readDatabase() {
    //     try {
    //         Path usersFile = properties.usersFile();

    //         if (Files.notExists(usersFile)
    //                 || Files.size(usersFile) == 0) {
    //             return new UserDatabase(new ArrayList<>());
    //         }

    //         UserDatabase database = objectMapper.readValue(
    //                 usersFile.toFile(),
    //                 UserDatabase.class
    //         );

    //         if (database.users() == null) {
    //             return new UserDatabase(new ArrayList<>());
    //         }

    //         return database;
    //     } catch (IOException exception) {
    //         throw new IllegalStateException(
    //                 "Could not read authentication database",
    //                 exception
    //         );
    //     }
    // }

 private UserDatabase readDatabase() {
    try (InputStream inputStream =
            getClass().getClassLoader()
                    .getResourceAsStream("users.json")) {

        if (inputStream == null) {
            throw new IllegalStateException(
                    "users.json was not found in the application classpath"
            );
        }

        UserDatabase database = objectMapper.readValue(
                inputStream,
                UserDatabase.class
        );

        if (database.users() == null) {
            return new UserDatabase(new ArrayList<>());
        }

        return database;
    } catch (IOException exception) {
        throw new IllegalStateException(
                "Could not read authentication database",
                exception
        );
    }
}

    /**
     * Root JSON object. A root object makes future schema additions easier.
     */
    public record UserDatabase(List<UserCredential> users) {

    }
}
