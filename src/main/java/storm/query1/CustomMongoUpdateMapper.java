package storm.query1;

import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.tuple.ITuple;
import org.bson.Document;
import storm.costant.Costant;
import storm.entity.Intersection;

import java.util.ArrayList;
import java.util.List;


public class CustomMongoUpdateMapper implements MongoMapper {
    private String[] fields;

    @Override
    public Document toDocument(ITuple tuple) {
        Document document = new Document();
        String id = (String) tuple.getValueByField(Costant.ID);
        document.append( "id" , id  );
        List<Intersection> listToSave= (List<Intersection>) tuple.getValueByField(Costant.RANK_TOPK);
        for ( int j = 0 ; j < listToSave.size() ; j++){
            Intersection intersection = listToSave.get(j);
            Document documentToAnnidate = new Document();
            documentToAnnidate.append( "classifica", j+1 );
            documentToAnnidate.append( "id", intersection.getId());
            documentToAnnidate.append( "velocitaMedia", intersection.getVelocitaMedia() );
            document.append( "valore "+ (j+1) , documentToAnnidate);
        }
        return new Document("$set", document);
    }

    public CustomMongoUpdateMapper withFields(String... fields) {
        this.fields = fields;
        return this;
    }
}
