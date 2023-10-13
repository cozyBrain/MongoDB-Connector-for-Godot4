using System.Collections.Generic;

using Godot;

using MongoDB.Driver;
using MongoDB.Bson;
using System;

[GlobalClass]
public partial class MongoDBConnector : Node
{
    private IMongoDatabase database;
	private MongoClient mongo_client;
    public void Connect(string connection_uri, string database_name)
    {
        try{
            // Connect MongoDB
            mongo_client = new MongoClient(connection_uri);
            database = mongo_client.GetDatabase(database_name);
        }
        catch(Exception ex){
            GD.PushError(ex.Message);
        }
    }

    // C.R.U.D.
    // Create
	public void InsertData(string collection_name, Godot.Collections.Dictionary<string, Variant> data)
    {
        var bsonDocument = ConvertDictionaryToBsonDocument(data);
        var collection = database.GetCollection<BsonDocument>(collection_name);
        collection.InsertOne(bsonDocument);
    }

    // Read
    public Godot.Collections.Array<string> GetCollectionNameList()
    {
        var collectionNames = database.ListCollectionNames().ToList();
        var nameList = new Godot.Collections.Array<string>();
        foreach (var collectionName in collectionNames){
            nameList.Add(collectionName);
        }
        return nameList;
    }
    public Godot.Collections.Array<Godot.Collections.Dictionary> GetDocument_ANDFilter(string collection_name, Godot.Collections.Dictionary<string, Variant> filter)
    {
        var collection = database.GetCollection<BsonDocument>(collection_name);
       
        // No filter, return every docs from the collection
        if(filter.Count == 0){ 
            return BsonDocsToDictionaries(collection.Find(Builders<BsonDocument>.Filter.Empty).ToList());
        }

        // Build combined_filter
        var filterBuilder = Builders<BsonDocument>.Filter;
        var combined_filter = Builders<BsonDocument>.Filter.Empty;
        foreach (KeyValuePair<string, Variant> entry in filter)
        {
            var key = entry.Key;
            var val = ConvertToBsonValue(entry.Value);
            var filter_definition = filterBuilder.Eq(key, val);
            combined_filter = filterBuilder.And(combined_filter, filter_definition);
        }

        // Use the combined filter in your MongoDB query
        // Convert bson to json and return
        return BsonDocsToDictionaries(collection.Find(combined_filter).ToList());
    }

    // Update
    // yet.. no functions for Update.

    // Delete
    public void DropCollection(string collection_name)
    {
        database.DropCollection(collection_name);
    }

    // Private functions
    private BsonDocument ConvertDictionaryToBsonDocument(Godot.Collections.Dictionary<string, Variant> dictionary)
    {
        var bsonDocument = new BsonDocument();
        foreach (KeyValuePair<string, Variant> entry in dictionary)
        {
            var key = entry.Key;
            var val = ConvertToBsonValue(entry.Value);
            bsonDocument.Add(key, val);
        }
        return bsonDocument;
    }

    // Convert BsonDocs to readable from GDScript
    private static Godot.Collections.Array<Godot.Collections.Dictionary> BsonDocsToDictionaries(List<BsonDocument> bson_docs){
        var dictionaries = new Godot.Collections.Array<Godot.Collections.Dictionary>();
        foreach (BsonDocument doc in bson_docs){
            doc["_id"] = doc["_id"].ToString(); // Need to convert ObjectId("blabla") to "ObjectId"
            dictionaries.Add((Godot.Collections.Dictionary)Json.ParseString(doc.ToJson()));
        }
        return dictionaries;
    }
    private BsonValue ConvertToBsonValue(Variant value)
    {
        if (value.VariantType is Variant.Type.Int){
            return new BsonInt64((long)value); // (Godot stores 64-bit integers in Variant) ref:https://docs.godotengine.org/en/stable/tutorials/scripting/c_sharp/c_sharp_variant.html
        }
        else if (value.VariantType is Variant.Type.Float){
            return new BsonDouble((double)value); // (Godot stores 64-bit floats in Variant)
        }
        else if (value.VariantType is Variant.Type.String){
            return new BsonString((string)value);
        }

        // Default: Convert to BsonString
        return new BsonString(value.ToString());
    }
}
