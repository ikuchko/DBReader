package com.illiakins;

import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;

import static com.illiakins.PropertiesReader.getStringSetting;
import static org.junit.Assert.*;

/**
 * Created by Illiak on 4/12/2017.
 */
public class DBTest {
    @Test
    public void executeStoredProcedure() throws SQLException {
        new DB(getStringSetting("DB_URL_TEST"), getStringSetting("DB_TEST_USERNAME"), getStringSetting("DB_TEST_PASSWORD"), 5, 40, 2500, 5000, 900_000, 28_440_000);
        DB.execStoredProcedure("___CLEAR_TEST_DB___()", new ArrayList<Object>());
    }
}