package com.dataphion.hermes.icebergIngest;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.data.GenericRecord;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;

public class Utile {

	public static PartitionData buildPartitionData(PartitionSpec spec, GenericRecord record) {
	    if (!spec.isPartitioned()) return null;
	    PartitionData pd = new PartitionData(spec.partitionType());
	    List<PartitionField> fields = spec.fields();

	    for (int i = 0; i < fields.size(); i++) {
	        PartitionField field = fields.get(i);
	        String sourceName = spec.schema().findField(field.sourceId()).name();
	        Object value = null;
	        try {
	            value = record.getField(sourceName);
	        } catch (ArrayIndexOutOfBoundsException e) {
	            // Field missing in record
	            pd.set(i, null);
	            continue;
	        }

	        if (value == null) {
	            pd.set(i, null);
	            continue;
	        }

	        switch (field.transform().toString()) {
	            case "day":
	                pd.set(i, (int) ((LocalDate) value).toEpochDay());
	                break;
	            case "month":
	                LocalDate d = (LocalDate) value;
	                pd.set(i, d.getYear() * 12 + d.getMonthValue() - 1);
	                break;
	            case "year":
	                pd.set(i, ((LocalDate) value).getYear());
	                break;
	            case "hour":
	                pd.set(i, ((LocalTime) value).getHour());
	                break;
	            case "identity":
	                pd.set(i, value);
	                break;

	            default:
	            	String transformStr = field.transform().toString();
	                if (transformStr.startsWith("bucket")) {
	                    int numBuckets = 16; 
	                    int start = transformStr.indexOf('[');
	                    int end = transformStr.indexOf(']');
	                    if (start > 0 && end > start) {
	                        numBuckets = Integer.parseInt(transformStr.substring(start + 1, end));
	                    }
	                    pd.set(i, (value.hashCode() & Integer.MAX_VALUE) % numBuckets);
	                } else {
	                    throw new IllegalArgumentException("Unsupported partition transform: " + field.transform());
	                }
	        }
	    }

	    return pd;
	}
}