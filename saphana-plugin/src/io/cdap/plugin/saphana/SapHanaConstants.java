package io.cdap.plugin.saphana;

/**
 * SAP HANA constants.
 */
public class SapHanaConstants {
    public static final String SAPHANA_CONNECTION_STRING_FORMAT = "jdbc:sap://%s:%s/"; //TODO check schema

    private SapHanaConstants() {
        throw new AssertionError("Should not instantiate static utility class.");
    }

    public static final String PLUGIN_NAME = "SAP HANA";


}
