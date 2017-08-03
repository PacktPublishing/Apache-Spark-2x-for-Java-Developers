package com.packt.sfjd.ch9;

import java.io.IOException;
import java.io.Serializable;

import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
public class TweetText implements Function<String, String> ,Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String call(String tweet) throws Exception {

   	 ObjectMapper mapper = new ObjectMapper();
       try
       {
           JsonNode root = mapper.readValue(tweet, JsonNode.class);
           if (root.get("lang") != null &&
               "en".equals(root.get("lang").textValue()))
           {
               if (root.get("id") != null && root.get("text") != null)
               {   System.out.println("the text is ::"+root.get("text").textValue());                            
                   return root.get("text").textValue();
               }
               return null;
           }
           return null;
       }
       catch (IOException ex)
       {
          ex.printStackTrace();
       }
       return null;
	}

}
