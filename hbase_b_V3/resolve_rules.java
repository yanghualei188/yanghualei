package hbase_b_V3;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class resolve_rules {
	public static Set<String> resolve_to_sets(String str){
		Set<String> sets = new HashSet<String>();
		
		if(str.contains("||")){
			String[] strs = str.split("||");
			for(int i = 0 ; i < strs.length ; i++){
				String element  = strs[i];
				sets.add(element);
			}
		}else{
			sets.add(str);
		}
		
		
		return sets;
		
	}
	
	
	
	
	public static boolean resolve_main(Set<String> sets,Table table,Get get) throws Exception{
		
		
		 
	
		
		
		Iterator<String> iterator = sets.iterator();

		while(iterator.hasNext()){
			Set<String> flag = new HashSet<String>();
			Set<String> sets1 = new HashSet<String>();
			String ele = iterator.next();
			if(ele.contains("&")){
				String[] strs = ele.split("&");
				System.out.println(strs[0]);
				for(int i = 0 ; i < strs.length ; i++){
					String element  = strs[i];
					sets1.add(element);
				}
				
			}else{
				sets1.add(ele);
			}
			
			
				Iterator<String> iterator1 = sets1.iterator();
				while(iterator1.hasNext()){
					String ele1 = iterator1.next();
					System.out.println("-------------"+ele1);
					if(ele1.contains(">")){
						String[] es = ele1.split(">");
						get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(es[0]));
						 Result result = table.get(get);
						if(Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(es[0])))) > Integer.parseInt(es[1])){
							flag.add("1");
						}else{
							flag.add("2");
						}
					}
					

					if(ele1.contains("<")){
						
						String[] es = ele1.split("<");
						get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(es[0]));
						 Result result = table.get(get);
						if(Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(es[0])))) < Integer.parseInt(es[1])){
							flag.add("1");
						}else{
							flag.add("2");
						}
					}
					

					if(ele1.contains("=")){
						String[] es = ele1.split("=");
						get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(es[0]));
						 Result result = table.get(get);
						if(Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(es[0]))).equals(es[1])){
							flag.add("1");
						}else{
							flag.add("2");
						}
					}

					if(ele1.contains(">=")){
						String[] es = ele1.split(">=");
						get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(es[0]));
						 Result result = table.get(get);
						if(Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(es[0])))) >= Integer.parseInt(es[1])){
							flag.add("1");
						}else{
							flag.add("2");
						}
					}

					if(ele1.contains("<=")){
						String[] es = ele1.split("<=");
						get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(es[0]));
						 Result result = table.get(get);
						if(Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(es[0])))) <= Integer.parseInt(es[1])){
							flag.add("1");
						}else{
							flag.add("2");
						}
					}

					if(ele1.contains("!=")){
						String[] es = ele1.split(">");
						get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(es[0]));
						 Result result = table.get(get);
						if(!Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(es[0]))).equals(es[1])){
							flag.add("1");
						}else{
							flag.add("2");
						}
					}
					
				}
			   if (!flag.contains("2")) {	
				   return true;
			   }
		}
		return false;
	}
}
