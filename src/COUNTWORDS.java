import java.util.TreeMap;


public class COUNTWORDS {
	public static void main(String[] args) {
		String str = "Car Car River";
		String[] words = str.split(" ");
	}
	
	public static TreeMap<String, Integer> addToMap(String key) {
		int value=0;
		TreeMap<String, Integer> myMap = new TreeMap<String, Integer>();
		if(myMap.containsKey(key)) {
			myMap.put(key,myMap.get(key)+1);
		}
		else {
			value = 1;
			myMap.put(key,value);
		}
		return myMap;
	}
}
