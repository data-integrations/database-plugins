package io.cdap.plugin.saphana;


import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.batch.config.DBSpecificSourceConfig;
import io.cdap.plugin.db.batch.source.AbstractDBSource;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * Batch source to read from PostgreSQL.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SapHanaConstants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
        " Outputs one record for each row returned by the query.")
public class SapHanaSource extends AbstractDBSource {

    private final SapHanaSourceConfig sapHanaSourceConfig;

    public SapHanaSource(SapHanaSourceConfig sapHanaSourceConfig) {
        super(sapHanaSourceConfig);
        this.sapHanaSourceConfig = sapHanaSourceConfig;
    }

    @Override
    protected String createConnectionString() {
        return String.format(SapHanaConstants.SAPHANA_CONNECTION_STRING_FORMAT, sapHanaSourceConfig.host,
                sapHanaSourceConfig.port); //TODO maybe pass Database here too
    }

    @Override
    protected SchemaReader getSchemaReader() {
        return new SapHanaSchemaReader();
    }

    @Override
    protected Class<? extends DBWritable> getDBRecordType() {
        return SapHanaDBRecord.class;
    }

    public static class SapHanaSourceConfig extends DBSpecificSourceConfig {

        @Override
        public String getConnectionString() {
            return String.format(SapHanaConstants.SAPHANA_CONNECTION_STRING_FORMAT, host, port); //TODO maybe pass Database here too
        }


    }


}

