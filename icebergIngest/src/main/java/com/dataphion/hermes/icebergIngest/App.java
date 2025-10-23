package com.dataphion.hermes.icebergIngest;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.Arrays;

import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.blob.models.BlobItem;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types.StructType;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class App {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        String azureAccountKey = System.getenv("AZURE_ACCOUNT_KEY");
        String azureAccountName = System.getenv("AZURE_ACCOUNT_NAME");
        String azureContainerName = System.getenv("AZURE_CONTAINER_NAME");

        String jdbcUrl = System.getenv("JDBC_URL");
        String jdbcUser = System.getenv("JDBC_USER");
        String jdbcPassword = System.getenv("JDBC_PASSWORD");

        String catalogName = System.getenv("ICEBERG_CATALOG_NAME");
        String namespace = System.getenv("ICEBERG_NAMESPACE");
        String tableName = System.getenv("ICEBERG_TABLE_NAME");
        String componentID = System.getenv("COMPONENT_ID");

        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.azure.account.key." + azureAccountName + ".dfs.core.windows.net", azureAccountKey);

        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "abfss://" + azureContainerName + "@" + azureAccountName
                + ".dfs.core.windows.net/warehouse/hermesdemo");
        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.jdbc.JdbcCatalog");
        properties.put("uri", jdbcUrl);
        properties.put("jdbc.user", jdbcUser);
        properties.put("jdbc.password", jdbcPassword);

        JdbcCatalog catalog = new JdbcCatalog();
        Configuration conf = new Configuration();
        conf.addResource(hadoopConf);
        catalog.setConf(conf);
        catalog.initialize(catalogName, properties);
        TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);
        org.apache.iceberg.Table table = catalog.loadTable(tableIdentifier);
        LocationProvider lp = table.locationProvider();
        System.out.println("Table location -> " + table.location());
        PartitionSpec ps = table.spec();
        System.out.println(ps.fields());
        Schema schema = table.schema();
        // Get ABFS Connection
        BlobContainerClient containerClient = getABFSConnection(azureContainerName, azureAccountName, componentID,
                azureAccountKey);
        List<String> newFiles = getFilesForComponent(containerClient, componentID);
        System.out.println("Number of files: " + newFiles.size());

        try {
            for (String fileName : newFiles) {
                List<GenericRecord> records = readFromAzureBlob(containerClient, fileName, schema, true);
                if (records.isEmpty()) {
                    continue;
                }
             // Group records by partition
                Map<PartitionData, List<GenericRecord>> partitionedRecords = new HashMap<>();
                for (GenericRecord record : records) {
                    PartitionData pd = Utile.buildPartitionData(ps, record);
                    partitionedRecords.computeIfAbsent(pd, k -> new ArrayList<>()).add(record);
                }
                for (Map.Entry<PartitionData, List<GenericRecord>> entry : partitionedRecords.entrySet()) {
                    PartitionData partitionData = entry.getKey();
                    List<GenericRecord> recs = entry.getValue();
//                    System.out.println("=====================================================================================================");
//                    System.out.println("partitionData: "+partitionData);
//                    System.out.println("recs: "+ recs);
                    String partitionPath = "";
                    if (partitionData != null && ps.isPartitioned()) {
                        StringBuilder sb = new StringBuilder();
                        List<PartitionField> fields = ps.fields();
                        for (int i = 0; i < fields.size(); i++) {
                            PartitionField f = fields.get(i);
                            Object val = partitionData.get(i);
                            String transform = f.transform().toString();
                            if ("day".equals(transform)) {
                                val = LocalDate.ofEpochDay((Integer) val);
                            }else if ("month".equals(transform)) {
                                int monthVal = (Integer) val;
                                int year = monthVal / 12;
                                int month = monthVal % 12 + 1;
                                val = String.format("%04d-%02d", year, month);
                            }
                            sb.append(f.name()).append("=").append(val).append("/");
                        }
                        partitionPath = sb.toString();
                    }
                    String filepath = lp.newDataLocation(partitionPath + UUID.randomUUID() + ".parquet");
                    OutputFile file = table.io().newOutputFile(filepath);
                    System.out.println("filepath: "+filepath);
	                DataWriter<GenericRecord> dataWriter;
	                dataWriter = Parquet.writeData(file)
	                		.schema(schema)
	                        .createWriterFunc(GenericParquetWriter::buildWriter)
	                        .withSpec(ps)
	                        .withPartition(partitionData)
	                        .build();

	                for (GenericRecord rec : recs) {
                        dataWriter.write(rec);
                    }
	                dataWriter.close();
	                DataFile dataFile = dataWriter.toDataFile();
	                table.newAppend().appendFile(dataFile).commit();
	                table.refresh();
                }
                System.out.println("Record written to Iceberg table");
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catalog.close();
    }

    public static List<String> getFilesForComponent(BlobContainerClient containerClient, String componentID) {

        List<String> files = new ArrayList<String>();
        for (BlobItem blobItem : containerClient.listBlobs()) {
            String blobName = blobItem.getName();
            // Apply your pattern matching logic here
            if (blobName.startsWith("events/" + componentID + "/")) {
                files.add(blobName);
            }
        }
        return files;

    }

    public static BlobContainerClient getABFSConnection(String containerName, String accountName, String componentID,
            String accountKey) {
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
                .endpoint("https://" + accountName + ".blob.core.windows.net")
                .credential(credential);

        BlobContainerClient containerClient = builder.buildClient().getBlobContainerClient(containerName);
        return containerClient;
    }

    public static List<GenericRecord> readFromAzureBlob(BlobContainerClient containerClient, String fileName,
            Schema schema,
            boolean removeFile) {
        List<GenericRecord> records = new ArrayList<>();
        BlobClient blobClient = containerClient.getBlobClient(fileName);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            blobClient.downloadStream(outputStream);
            String jsonContent = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
            List<String> jsonRecords = Arrays.asList(jsonContent.split("\n")); // Adjust delimiter as needed

            for (String record : jsonRecords) {
                GenericRecord genericRecord = parseJsonToRecord(record, schema);
                if (genericRecord != null) {
                    records.add(genericRecord);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        if (removeFile) {
            blobClient.delete();
        }
        return records;
    }

    private static GenericRecord parseJsonToRecord(String jsonLine, Schema schema) {
        try {
            JsonNode jsonNode = objectMapper.readTree(jsonLine);

            // Create a record using the Iceberg schema
            GenericRecord record = GenericRecord.create(schema);

            // Populate the record fields based on the JSON data
            for (Types.NestedField field : schema.asStruct().fields()) {
                String fieldName = field.name();

                // Assuming that the JSON field names match the Iceberg schema field names
                if (jsonNode.has(fieldName)) {
                    // Set the value directly without explicit conversion
                    Object fieldValue = extractJsonValue(jsonNode.get(fieldName), field.type());
                    if (fieldName.equals("event_date")) {
                        System.out.println(fieldName);
                        System.out.println(fieldValue);
                    }
                    record.setField(fieldName, fieldValue);
                }
            }

            return record;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static Object extractJsonValue(JsonNode jsonNode, Type type) {
        switch (type.typeId()) {
            case STRING:
                return jsonNode.asText();
            case INTEGER:
                return jsonNode.asInt();
            case LONG:
                return jsonNode.asLong();
            case FLOAT:
                return (float) jsonNode.asDouble();
            case DOUBLE:
                return jsonNode.asDouble();
            case BOOLEAN:
                return jsonNode.asBoolean();
            case DECIMAL:
                return new BigDecimal(jsonNode.asText());
            case DATE:
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                return LocalDate.parse(jsonNode.asText(), formatter);
            case TIME:
                return LocalTime.parse(jsonNode.asText());
            case TIMESTAMP:
                return jsonNode.asDouble();
            case STRUCT:
                return extractStruct(jsonNode, (StructType) type);
            case LIST:
                return extractList(jsonNode, (Types.ListType) type);
            case MAP:
                return extractMap(jsonNode, (Types.MapType) type);
            // Add cases for other types as needed
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    private static Record extractStruct(JsonNode jsonNode, StructType structType) {
        Record structRecord = GenericRecord.create(structType);
        for (Types.NestedField field : structType.fields()) {
            String fieldName = field.name();
            if (jsonNode.has(fieldName)) {
                Object fieldValue = extractJsonValue(jsonNode.get(fieldName), field.type());
                structRecord.setField(fieldName, fieldValue);
            }
        }
        return structRecord;
    }

    private static List<?> extractList(JsonNode jsonNode, Types.ListType listType) {
        List<Object> list = new ArrayList<>();
        Type elementType = listType.elementType();

        jsonNode.elements().forEachRemaining(element -> {
            Object elementValue = extractJsonValue(element, elementType);
            list.add(elementValue);
        });

        return list;
    }
    
    private static Map<Object, Object> extractMap(JsonNode jsonNode, Types.MapType mapType) {
        Map<Object, Object> map = new HashMap<>();
        Type keyType = mapType.keyType();
        Type valueType = mapType.valueType();

        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();

            // keys in JSON maps are always strings
            Object key = entry.getKey();
            if (keyType.typeId() != Type.TypeID.STRING) {
                throw new IllegalArgumentException("Only string keys supported in JSON maps");
            }

            Object value = extractJsonValue(entry.getValue(), valueType);
            map.put(key, value);
        }
        return map;
    }

}
