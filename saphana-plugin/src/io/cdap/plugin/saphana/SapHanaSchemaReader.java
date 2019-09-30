package io.cdap.plugin.saphana;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.CommonSchemaReader;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Set;


public class SapHanaSchemaReader extends CommonSchemaReader {


    public static final Set<Integer> STRING_MAPPED_SAPHANA_TYPES = ImmutableSet.of(
            //TODO fill this
    );

    public static final Set<String> STRING_MAPPED_SAPHANA_TYPES_NAMES = ImmutableSet.of(
            //TODO fill this
    );

    @Override
    public Schema getSchema(ResultSetMetaData metadata, int index) throws SQLException {
        String typeName = metadata.getColumnTypeName(index);
        int columnType = metadata.getColumnType(index);

        if (STRING_MAPPED_SAPHANA_TYPES_NAMES.contains(typeName) || STRING_MAPPED_SAPHANA_TYPES.contains(columnType)) {
            return Schema.of(Schema.Type.STRING);
        }

        return super.getSchema(metadata, index);
    }
}
