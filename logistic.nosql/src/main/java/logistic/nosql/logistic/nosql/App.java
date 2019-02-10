package logistic.nosql.logistic.nosql;


import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;
import org.bson.types.ObjectId;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
//import org.neo4j.helpers.collection.MapUtil;
import redis.clients.jedis.Jedis;

public class App {

	public static void main(String[] args) {		
		 Scanner sc = new Scanner(System.in);  
		 
		 //Redis Connection
		 Jedis jedis = new Jedis("localhost");
		 int scCount=1;
		 
		 //MongoDB connection
		 MongoClient mongoClient = null;
			try {
				mongoClient = new MongoClient("localhost",27017);
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 DB database = mongoClient.getDB("Logistics");
		 DBCollection customerCollection = database.getCollection("Customers");
		 DBCollection pkgCollection = database.getCollection("PackageMaterial");
		 int i=1;
		 
		 System.out.println("Select Option");
		 
		 do {
		 System.out.println("1.Create Order 2.Display Order 3.Give Feedback 4.Display Service Center 5.Exit");
		 int option =sc.nextInt();
		 
		 switch(option)
		 {
		 case 1:
			 createOrder(jedis,customerCollection,pkgCollection);
			 break;
		 case 2:
			 System.out.println("Enter Order ID");
			 String orderid=sc.next();
			 displayOrder(orderid,jedis);
			 break;
		 case 3:
			 giveFeedback(sc,jedis);	
			 break;
		 case 4:
			 System.out.println("Enter name of service center");
			 String servicecenter=sc.next(); 			 
			 getAllOrdersinCenter(jedis,servicecenter,sc,scCount);
			 break;
		 case 5:			 
			 i=0;
			 break;
		 }	
		 }while(i!=0);
		 
	}
	
	public static void createOrder(Jedis jedis, DBCollection customerCollection,DBCollection pkgCollection)
	{
		try {
			Date date = new Date();
			String datetime = String.format("Current Date/Time : %tc", date );
			UUID rand = UUID.randomUUID();
			//System.out.println(rand);
			 
			Scanner sc = new Scanner(System.in);
			System.out.println("Enetr Customer First and Last name");
			String name = sc.next();
			System.out.println("Address");
			String address = sc.next();
			System.out.println("Zipcode");
			String zip = sc.next();
			System.out.println("Contact");
			String contact = sc.next();
			System.out.println("Email");
			String email = sc.next();
			
			System.out.println("End user detail");
			String enduser=sc.next();
			System.out.print("Source");
			String source = sc.next();
			
			System.out.println("Order material type");
			String material = sc.next();
			
			System.out.print("destination");
			String dest = sc.next();
			
			System.out.print("path");
			//String source = sc.next();
			
			getPath(source,dest,jedis);
			
			DBObject customer = new BasicDBObject()
			                            .append("name", name)
			                            .append("address", new BasicDBObject("City",address)    		                                                         
			                                                         .append("zip", zip)
			                                                         .append("street",""))
			                                                         .append("contact", contact)
			                                                         .append("Email", email);
			                            
			customerCollection.insert(customer);
			ObjectId id = (ObjectId)customer.get( "_id" );
			String pkgid="";
			//get package id from MongoDB
			
			//DBObject pkgmaterial = new BasicDBObject();
			for( DBObject pkgmaterial : pkgCollection.find() )
			{
				if(material.equals((String) pkgmaterial.get( "MaterialType")))
				{
					pkgid= (String) pkgmaterial.get( "PackagingID" );
					System.out.println(pkgid);
					
				}		
			}
			
			Map<String, String> Order = new HashMap<String, String>();
			Order.put("OrderID",rand.toString());
			Order.put("customerid", id.toHexString());
			Order.put("PackageID", pkgid);
			Order.put("End User Details", enduser);
			Order.put("Source", source);
			Order.put("Destination",dest);			
			Order.put("Status","InTransit");
			Order.put("Date", datetime);
			Order.put("CurrentLocation", source);
			Order.put("LocationPosition", "1");
			Order.put("Cost","");
			Order.put("Priority","Normal");
			jedis.hmset(rand.toString(), Order);	
			System.out.println(jedis.hgetAll(rand.toString()));

			jedis.lpush("Orders", rand.toString());
			jedis.lpush(source+"orders", rand.toString());

		}
		catch(Exception e)
		{
			System.out.println(e.getStackTrace());
		}
			}
	
	public static void getPath(String source,String destination,Jedis jedis)
	{
		String hashname=source+destination;
		int i=1;
		
		Map<String, String> avalue = new HashMap<String, String>();
		
		

		// Connect
		Connection con = null;
		try {
			con = DriverManager.getConnection("jdbc:neo4j:bolt://localhost:7687?user=neo4j,password=ajith123,scheme=basic");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Querying
		
		try (Statement stmt = con.createStatement()) {
			ResultSet rs = stmt.executeQuery("MATCH (start:place{name:\"mumbai\"}),(end:place{name:\"frankfurt\"})\r\n" + 
					"CALL algo.shortestPath.stream(start, end, 'distance',{relationshipQuery:'connectedby',direction:'OUT'})\r\n" + 
					"YIELD nodeId, cost\r\n" + 
					"RETURN algo.getNodeById(nodeId).name as hub");
			while (rs.next()) {

				avalue.put("sc"+i,rs.getString("hub"));
				jedis.hmset(hashname,avalue);
				i++;				
				System.out.println(rs);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
		System.out.println(jedis.hgetAll(hashname));
	}	
	
	public static void getAllOrdersinCenter(Jedis jedis,String source,Scanner sc,int scCount)
	{	
		
		List<String> list = jedis.lrange(source+"orders", 0, 10);

		for(int i=0; i< list.size();i++)
		{
			System.out.println(list.get(i));
		}
		
		System.out.println("/nDo you want to send the package to next Service Centre /n1) Y /n2) N ");
		
		String selection=sc.next();
		if(selection.equals("Y"))
		{
			System.out.println("Enter Order ID for Delivery");
			String orderid = sc.next();
			
			String ordsource = jedis.hget(orderid, "Source");
			String orddest = jedis.hget(orderid, "Destination");
			int currloc = Integer.parseInt( jedis.hget(orderid, "LocationPosition"));
			String hashname =ordsource+orddest;
			
			//find next service center
			currloc++;
			currloc++;
			System.out.println(scCount);
			String servcenter = jedis.hget(hashname, "sc"+currloc);
			
			//add order
			updateServiceCenter(jedis,servcenter,orderid);
			updateOrder(servcenter,orddest,jedis,orderid,currloc);
		}
				
	}
	
	public static void updateOrder(String servcenter,String dest,Jedis jedis,String orderid,int currloc)
	{
		Map<String, String> avalue = new HashMap<String, String>();
		
		
			if(servcenter.equals(dest))
			{
				avalue.put("Status", "Delivered");
				avalue.put("CurrentLocation", servcenter);
				avalue.put("LocationPosition",Integer.toString(currloc) );
				jedis.hmset(orderid,avalue);
				
			}
			else
			{
				avalue.put("CurrentLocation", servcenter);
				avalue.put("LocationPosition",Integer.toString(currloc) );
				jedis.hmset(orderid,avalue);
				
			}
	}
	
	public static void updateServiceCenter(Jedis jedis,String servcenter,String orderid)
	{	
				jedis.lpush(servcenter+"orders",orderid);			
	}
	
	public static void giveFeedback(Scanner sc,Jedis jedis)
	{
		Map<String, String> avalue = new HashMap<String, String>();
		System.out.println("Give order id for feedback");
		String ordid= sc.next();
		
		System.out.println("Enter Comment");
		String comment=sc.next();
		
		avalue.put("Comments", comment);
		jedis.hmset(ordid,avalue);
		
	}
	
	public static void displayOrder(String orderid,Jedis jedis)
	{
		System.out.print(jedis.hgetAll(orderid));
	}
}



