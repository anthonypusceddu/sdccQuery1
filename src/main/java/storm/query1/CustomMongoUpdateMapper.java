package storm.query1;

import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.shade.org.eclipse.jetty.util.ajax.JSON;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.tuple.ITuple;
import org.bson.Document;


public class CustomMongoUpdateMapper implements MongoMapper {
    private String[] fields;

    @Override
    public Document toDocument(ITuple tuple) {
        Document document = new Document();
        for(String field : fields){
            document.append(field, tuple.getValueByField(field));
        }
        return new Document("$set", document);
    }

    public CustomMongoUpdateMapper withFields(String... fields) {
        this.fields = fields;
        return this;
    }
}
