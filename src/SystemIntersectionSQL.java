import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.ListUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import com.google.common.collect.Lists;
import java.util.Map.Entry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.HashSet;
import me.xdrop.fuzzywuzzy.FuzzySearch;

public class SystemIntersectionSQL {
	// public static String path1 =
	// "C:\\Users\\Negar\\eclipse-workspace\\niousha1\\ML2\\TupleEstimation\\";
	// public static String path2 =
	// "C:\\Users\\Negar\\eclipse-workspace\\niousha1\\";
	public static String path1 = "C:\\Users\\LENOVO\\workspace\\Histogram\\ML2\\";
	public static String path2 = "C:\\Users\\LENOVO\\workspace\\Histogram\\";
	public static String[] tableList = { "attribute", "business", "category", "hours" };
	public static String[] primKeys = { "business_id", "business_id", "business_id", "business_id" };
	public static String[][] attributeMatrix = { { "name", "value", null, null, null },
			{ "name", "neighborhood", "address", "city", "state" }, { "category", null, null, null, null },
			{ "hours", null, null, null, null } };
	public static Set<String> allIRs = new HashSet<String>();
	public static int no_Of_Atts = 5;
	public static Graph graph = new Graph();
	public static LinkedList<String> subgraphPath = new LinkedList<String>();
	public static LinkedList<String> visited = new LinkedList<String>();
	public static LinkedHashMap<Integer, ArrayList<String>> matchListInfo = new LinkedHashMap<Integer, ArrayList<String>>();
	public static LinkedHashMap<Integer, ArrayList<String>> key_bids = new LinkedHashMap<Integer, ArrayList<String>>();
	public static LinkedHashMap<String, Double> IR_tupleEstimation = new LinkedHashMap<String, Double>();
	public static int nextRow = 1;

	// ---------------------------------------------------------------------------
	public static void cc() {
		graph = new Graph();
		subgraphPath.clear();
		visited.clear();
		matchListInfo.clear();
		key_bids.clear();
	}

	// ---------------------------------------------------------------------------

	public static void createGraph() throws IOException, SQLException {
		// for each table in the schema checks if they have common attributes
		// if they do, then creates an edge in the graph for those tables which
		// contains the same attribute

		double start = System.currentTimeMillis();
		Connection conn = null;
		conn = ConnectionToMySql.getConnection();
		Statement stmt = null;
		ResultSet rs = null;

		for (int q = 0; q < tableList.length; q++) {
			for (int h = 0; h < tableList.length; h++) {
				if (tableList[q] != tableList[h]) {
					stmt = conn.createStatement();
					rs = stmt.executeQuery(
							"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='" + tableList[q]
									+ "' AND table_schema = 'yelp' AND COLUMN_NAME in(SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='"
									+ tableList[h] + "' AND table_schema='yelp')");
					if (rs.next()) {
						graph.addEdge(tableList[q], tableList[h]);
					}
				}
			}
		}
		double end = System.currentTimeMillis();
		System.out.println("Execution Time in createGraph Method: " + (end - start) / 1000 + " s");
		System.out.println("");
	}

	// --------------------------------------------------------------------------

	@SuppressWarnings({ "unused", "null" })
	public static void createIndex() throws IOException, SQLException {
		double start = System.currentTimeMillis();
		Connection conn = null;
		conn = ConnectionToMySql.getConnection();
		Statement stmt = null;
		ResultSet rs = null;

		for (int i = 0; i < tableList.length; i++) {
			if (!tableList[i].equals("business")) {
				stmt = conn.createStatement();
				stmt.execute("ALTER TABLE " + tableList[i] + " ADD id int NOT NULL AUTO_INCREMENT PRIMARY KEY");
			}
		}

		for (int q = 0; q < tableList.length; q++) {
			for (int h = 0; h < no_Of_Atts; h++) {
				if (attributeMatrix[q][h] != null) {
					stmt = conn.createStatement();
					stmt.execute("CREATE FULLTEXT INDEX idx" + q + h + " ON yelp." + tableList[q] + "("
							+ attributeMatrix[q][h] + ")");
				}
			}
		}

		double end = System.currentTimeMillis();
		System.out.println("Execution Time in createIndex Method: " + (end - start) / 1000 + " s");
		System.out.println("");
	}

	// ---------------------------------------------------------------------------

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static String findInitialRelations(String query) throws Exception {
		System.out.println("User's Query: " + query);

		// finds initial relations and all combinations of them which contain
		// all the query keywords
		double start = System.currentTimeMillis();
		int key = 0;
		Connection conn = null;
		conn = ConnectionToMySql.getConnection();
		Statement stmt = null;
		ResultSet rs = null;
		String newQuery = "";
		// finds tables and attributes which contain query keywords combinations
		for (String kw : query.split(" ")) {
			int oldSize = matchListInfo.size();
			for (int i = 0; i < tableList.length; i++) {
				for (int j = 0; j < no_Of_Atts; j++) {
					if (attributeMatrix[i][j] != null) {
						HashSet<String> ids = new HashSet<String>();
						stmt = conn.createStatement();
						rs = stmt.executeQuery("SELECT " + attributeMatrix[i][j] + "," + primKeys[i] + " FROM yelp."
								+ tableList[i] + " WHERE MATCH (" + attributeMatrix[i][j] + ")AGAINST ('*" + kw
								+ "*' IN BOOLEAN MODE)");
						while (rs.next()) {
							int tik = 0;
							for (String t : rs.getString(1).split(" ")) {
								if (FuzzySearch.ratio(kw.toLowerCase(),
										t.replaceAll("[^a-zA-Z]", "").toLowerCase()) > 70) {
									tik = 1;
									break;
								}
							}
							if (tik == 1) {
								ids.add(rs.getString(2));
							}
						}

						if (ids.size() > 0) {
							allIRs.add(tableList[i]);
							matchListInfo.put(key, new ArrayList<String>(Arrays.asList(kw, tableList[i],
									attributeMatrix[i][j], Integer.toString(ids.size()))));
							key_bids.put(key, Lists.newArrayList(ids));
							ids.clear();
							key++;
						}
					}
				}
			}
			if (matchListInfo.size() > oldSize) {
				if (newQuery.equals(""))
					newQuery = kw;
				else
					newQuery = newQuery + " " + kw;
			}
		}

		double end = System.currentTimeMillis();
		System.out.println("");
		System.out.println("Execution Time in findInitialRelation Method: " + (end - start) / 1000 + " s");
		System.out.println("");

		// query = newquery;
		System.out.println("");
		System.out.println("Query Keywords which were found in Indexes: " + newQuery);

		System.out.println("");
		System.out
				.println("----------------------- Keyword, Table, Attribute, Term Frequency -------------------------");
		for (Map.Entry<Integer, ArrayList<String>> entry : matchListInfo.entrySet()) {
			System.out.println(entry.getKey() + " >> " + entry.getValue());
		}
		System.out.println("");

		return newQuery;
	}
	// ---------------------------------------------------------------------------

	public static LinkedHashMap<String, Integer> TFinDB() throws IOException, SQLException {
		LinkedHashMap<String, Integer> tfDB = new LinkedHashMap<String, Integer>();
		for (Entry<Integer, ArrayList<String>> entry : matchListInfo.entrySet()) {
			String kw = entry.getValue().get(0);
			int tf = Integer.parseInt(entry.getValue().get(3));
			if (!tfDB.keySet().contains(kw)) {
				tfDB.put(kw, tf);
			} else {
				int temp = tfDB.get(kw);
				tfDB.replace(kw, temp + tf);
			}
		}
		return tfDB;
	}
	// --------------------------------------------------

	public static LinkedHashMap<String, Double> exclusivityInDB(String query) throws IOException {
		LinkedHashMap<String, Double> attribute = new LinkedHashMap<String, Double>();
		LinkedHashMap<String, Double> business = new LinkedHashMap<String, Double>();
		LinkedHashMap<String, Double> category = new LinkedHashMap<String, Double>();
		LinkedHashMap<String, Double> hours = new LinkedHashMap<String, Double>();
		LinkedHashMap<String, Double> kw_exInDB = new LinkedHashMap<String, Double>();

		for (int q = 0; q < tableList.length; q++) {
			for (int h = 0; h < 5; h++) {
				if (attributeMatrix[q][h] != null) {
					String fileName2 = "HistogramWordsCountIn" + tableList[q] + attributeMatrix[q][h];
					BufferedReader br2 = new BufferedReader(new FileReader(path1 + fileName2 + ".txt"));

					if (tableList[q].equals("attribute")) {
						String contentLine1;
						while ((contentLine1 = br2.readLine()) != null) {
							attribute.put(contentLine1.split("@")[0] + "@" + attributeMatrix[q][h],
									Double.parseDouble(contentLine1.split("@")[1]));
						}
					}

					if (tableList[q].equals("business")) {
						String contentLine1;
						while ((contentLine1 = br2.readLine()) != null) {
							business.put(contentLine1.split("@")[0] + "@" + attributeMatrix[q][h],
									Double.parseDouble(contentLine1.split("@")[1]));
						}
					}

					if (tableList[q].equals("category")) {
						String contentLine1;
						while ((contentLine1 = br2.readLine()) != null) {
							category.put(contentLine1.split("@")[0] + "@" + attributeMatrix[q][h],
									Double.parseDouble(contentLine1.split("@")[1]));
						}
					}

					if (tableList[q].equals("hours")) {
						String contentLine1;
						while ((contentLine1 = br2.readLine()) != null) {
							hours.put(contentLine1.split("@")[0] + "@" + attributeMatrix[q][h],
									Double.parseDouble(contentLine1.split("@")[1]));
						}
					}
					br2.close();
				}
			}
		}

		double att_name = 0;
		double att_value = 0;
		double business_name = 0;
		double business_neighborhood = 0;
		double business_address = 0;
		double business_city = 0;
		double business_state = 0;
		double category_category = 0;
		double hours_hours = 0;
		double exclusivityInDB = 0;

		for (String kw : query.split(" ")) {
			for (Entry<String, Double> entry : attribute.entrySet()) {
				if (entry.getKey().equals("" + kw + "" + "@name")) {
					att_name = attribute.get(kw + "@name");
				}
				if (entry.getKey().equals("" + kw + "" + "@value")) {
					att_value = attribute.get(kw + "@value");
				}
			}

			for (Entry<String, Double> entry : business.entrySet()) {
				if (entry.getKey().equals("" + kw + "" + "@name")) {
					business_name = business.get(kw + "@name");
				}
				if (entry.getKey().equals("" + kw + "" + "@neighborhood")) {
					business_neighborhood = business.get(kw + "@neighborhood");
				}
				if (entry.getKey().equals("" + kw + "" + "@address")) {
					business_address = business.get(kw + "@address");
				}
				if (entry.getKey().equals("" + kw + "" + "@city")) {
					business_city = business.get(kw + "@city");
				}
				if (entry.getKey().equals("" + kw + "" + "@state")) {
					business_state = business.get(kw + "@state");
				}
			}

			for (Entry<String, Double> entry : category.entrySet()) {
				if (entry.getKey().equals("" + kw + "" + "@category")) {
					category_category = category.get("" + kw + "" + "@category");
				} else if (entry.getKey().equals("" + kw + "s" + "@category")) {
					category_category = category.get("" + kw + "s" + "@category");
				}
			}

			for (Entry<String, Double> entry : hours.entrySet()) {
				if (entry.getKey().equals("" + kw + "" + "@hours")) {
					hours_hours = hours.get(kw + "@hours");
				}
			}

			double sum = att_name + att_value + business_name + business_neighborhood + business_address + business_city
					+ business_state + category_category + hours_hours;
			exclusivityInDB = (Math.pow(att_name / sum, 2)) + (Math.pow(att_value / sum, 2))
					+ (Math.pow(business_name / sum, 2)) + (Math.pow(business_neighborhood / sum, 2))
					+ (Math.pow(business_address / sum, 2)) + (Math.pow(business_city / sum, 2))
					+ (Math.pow(category_category / sum, 2)) + (Math.pow(business_state / sum, 2))
					+ (Math.pow(hours_hours / sum, 2));

			kw_exInDB.put(kw, exclusivityInDB);
		}

		return kw_exInDB;
	}
	// ---------------------------------------------

	public static void histogram() throws IOException {
		for (int q = 0; q < tableList.length; q++) {
			for (int h = 0; h < 5; h++) {
				if (attributeMatrix[q][h] != null) {
					if (!attributeMatrix[q][h].equals("hours")) {
						String fileName2 = "WordsCountIn" + tableList[q] + attributeMatrix[q][h];
						BufferedReader br2 = new BufferedReader(
								new FileReader("C:\\Users\\LENOVO\\workspace\\Histogram\\ExclusivityTextFiles\\"
										+ fileName2 + ".txt"));

						LinkedHashMap<String, Double> topList = new LinkedHashMap<String, Double>();
						LinkedHashMap<String, Double> others = new LinkedHashMap<String, Double>();
						BufferedWriter writer1 = new BufferedWriter(new FileWriter(
								"C:\\Users\\LENOVO\\workspace\\Histogram\\ExclusivityTextFiles\\Histogram" + fileName2
										+ ".txt",
								true));
						int count1 = 0;
						double sum1 = 0;

						String contentLine;
						while ((contentLine = br2.readLine()) != null) {
							if (count1 < 20) {
								topList.put(contentLine.split("=")[0], Double.parseDouble(contentLine.split("=")[1]));
							}
							if (count1 >= 20) {
								sum1 = sum1 + Integer.parseInt(contentLine.split("=")[1]);
								others.put(contentLine.split("=")[0], sum1);
							}
							count1++;
						}
						br2.close();
						int size = others.size();
						List<Double> values = new ArrayList<Double>(others.values());

						double sum = values.get(size - 1);
						for (Entry<String, Double> entry : topList.entrySet()) {
							writer1.write(entry.getKey() + "@" + entry.getValue());
							writer1.newLine();
						}
						for (Entry<String, Double> entry : others.entrySet()) {
							writer1.write(entry.getKey() + "@" + sum / size);
							writer1.newLine();
						}
						writer1.close();
					}
				}
			}
		}
	}

	// ---------------------------------------------------------------------------

	public static int compareTwoStrings(String str1, String str2) {
		Set<String> set1 = new HashSet<String>();
		Set<String> set2 = new HashSet<String>();
		str1 = str1.replace("[", " ").replace("]", " ").replace(",", " ").replace("<", " ").replace(">", " ");
		str2 = str2.replace("[", " ").replace("]", " ").replace(",", " ").replace("<", " ").replace(">", " ");

		for (String s : str1.split(" ")) {
			if (!s.equals("") || !s.equals(" "))
				set1.add(s);
		}

		for (String s : str2.split(" ")) {
			if (!s.equals("") || !s.equals(" ")) {
				set2.add(s);
			}
		}

		if (set1.containsAll(set2) && set2.containsAll(set1)) {
			return 1;
		} else {
			return 0;
		}
	}

	// ---------------------------------------------------------------------------
	public static String ISCreation(LinkedList<String> node_0, LinkedList<String> otherNodes) {
		LinkedList<String> candidNodes = new LinkedList<String>();
		for (int i = 0; i < node_0.size(); i++) {
			String n_0 = node_0.get(i);
			for (String d : graph.adjacentNodes(n_0)) {
				if (!visited.contains(d) && otherNodes.contains(d)) {
					subgraphPath.add("<" + n_0 + " " + d + ">");
					visited.add(d);
					candidNodes.add(d);
					otherNodes.remove(d);
				}
			}
			if (otherNodes.size() == 0)
				break;
		}

		if (otherNodes.size() > 0) {
			LinkedList<String> tempAdjacentNodes = new LinkedList<String>();
			for (String d : visited) {
				for (String g : graph.adjacentNodes(d))
					tempAdjacentNodes.add(g);
			}
			for (int i = 0; i < node_0.size(); i++) {
				String n_0 = node_0.get(i);
				for (String d : graph.adjacentNodes(n_0)) {
					if (!visited.contains(d)) {
						for (String g : graph.adjacentNodes(d)) {
							if (!visited.contains(g) && otherNodes.contains(g) && !tempAdjacentNodes.contains(g)) {
								subgraphPath.add("<" + n_0 + " " + d + ">");
								visited.add(d);
								candidNodes.add(d);
							}
						}
					}
				}
			}
			ISCreation(candidNodes, otherNodes);
		}
		return subgraphPath.toString();
	}

	// ---------------------------------------------------------------------------

	public static LinkedHashMap<String, Double> probabilityCalculation(String query) throws IOException, SQLException {
		double start = System.currentTimeMillis();
		LinkedHashMap<String, Integer> attr_wordCount = new LinkedHashMap<String, Integer>();
		// once calculated by wordCount() method
		attr_wordCount.put("name@attribute", 1153838);
		attr_wordCount.put("value@attribute", 525769);
		attr_wordCount.put("name@business", 416270);
		attr_wordCount.put("neighborhood@business", 82361);
		attr_wordCount.put("address@business", 470075);
		attr_wordCount.put("city@business", 192760);
		attr_wordCount.put("state@business", 130106);
		attr_wordCount.put("category@category", 936087);
		attr_wordCount.put("hours@hours", 734421);
		LinkedHashMap<String, Double> kw_probability = new LinkedHashMap<String, Double>();
		LinkedHashMap<String, Double> tempkw_probability = new LinkedHashMap<String, Double>();

		// for each word in query, finds the word's probability in each IA
		for (Map.Entry<Integer, ArrayList<String>> entry : matchListInfo.entrySet()) {
			String[] tempArray = new String[entry.getValue().size()];
			entry.getValue().toArray(tempArray);
			// ([key, kw, attribute, table, tf, allWords], probability)
			kw_probability.put(
					Integer.toString(entry.getKey()) + " @@ " + tempArray[0] + " @@ " + tempArray[2] + " @@ "
							+ tempArray[1] + " @@ " + tempArray[3] + " @@ "
							+ attr_wordCount.get(tempArray[2] + "@" + tempArray[1]) + " ",
					Double.parseDouble(tempArray[3]) / attr_wordCount.get(tempArray[2] + "@" + tempArray[1]));
		}

		// sort the list of column name/table name/Probability in descending
		// order of
		// their probability
		for (String kw : query.split(" ")) {
			List<Double> sortedProbabilities = new ArrayList<Double>();
			for (Map.Entry<String, Double> entry : kw_probability.entrySet()) {
				if (entry.getKey().split(" @@ ")[1].equals(kw)) {
					sortedProbabilities.add(entry.getValue());
				}
			}
			Collections.sort(sortedProbabilities, Collections.reverseOrder());
			for (Double d : sortedProbabilities) {
				kw_probability.forEach((k, v) -> {
					if (d == v) {
						tempkw_probability.put(k, v);
					}
				});
			}
		}

		kw_probability.clear();
		kw_probability = tempkw_probability;

		System.out.println("");
		System.out.println("-------------------- Keyword, Attribute, Probability ------------------------------");
		for (Map.Entry<String, Double> entry2 : kw_probability.entrySet()) {
			System.out.println(entry2.getKey() + "=" + entry2.getValue());
		}

		double end = System.currentTimeMillis();
		System.out.println("");
		System.out.println("Execution Time in probabilityCalculation Method: " + (end - start) / 1000 + " s");
		System.out.println("");
		return kw_probability;
	}

	// ---------------------------------------------------------------

	public static void attributeDisambiguation(String query, LinkedHashMap<String, Double> kw_probability)
			throws Exception {
		DecimalFormat formatter = new DecimalFormat("#0.000000");
		LinkedHashMap<String, Integer> attr_wordCount = new LinkedHashMap<String, Integer>();
		// once calculated by wordCount() method
		attr_wordCount.put("name@attribute", 1153838);
		attr_wordCount.put("value@attribute", 525769);
		attr_wordCount.put("name@business", 416270);
		attr_wordCount.put("neighborhood@business", 82361);
		attr_wordCount.put("address@business", 470075);
		attr_wordCount.put("city@business", 192760);
		attr_wordCount.put("state@business", 130106);
		attr_wordCount.put("category@category", 936087);
		attr_wordCount.put("hours@hours", 734421);
		LinkedHashMap<String, Integer> tfDB = TFinDB();
		LinkedHashMap<String, Double> exInDB = exclusivityInDB(query);
		LinkedHashMap<String, String> temp = new LinkedHashMap<String, String>();

		// creating query file (testFile)
		Workbook wb = new HSSFWorkbook();
		Sheet sheet = wb.createSheet("new sheet");
		Row row0 = sheet.createRow((short) 0);
		row0.createCell(0).setCellValue("order");
		row0.createCell(1).setCellValue("probability");
		row0.createCell(2).setCellValue("ColumnWordsCount");
		row0.createCell(3).setCellValue("TfinDB");
		row0.createCell(4).setCellValue("exclusivityInDB");
		row0.createCell(5).setCellValue("Output");
		int nextRow = 1;
		for (String kw : query.split(" ")) {
			int order = 1;
			for (Map.Entry<String, Double> entry : kw_probability.entrySet()) {
				String col = entry.getKey().split(" @@ ")[2];
				String table = entry.getKey().split(" @@ ")[3];
				if (entry.getKey().split(" @@ ")[1].equals(kw)) {
					Row rowi = sheet.createRow((short) nextRow);
					if (nextRow == 1) {
						rowi.createCell(0).setCellValue(order);
						rowi.createCell(1).setCellValue(formatter.format(entry.getValue()));
						rowi.createCell(2).setCellValue(attr_wordCount.get("" + col + "@" + table + ""));
						rowi.createCell(3).setCellValue(tfDB.get(kw));
						rowi.createCell(4).setCellValue(exInDB.get(kw));
						rowi.createCell(5).setCellValue("keep");
						temp.put(
								order + "," + formatter.format(entry.getValue()) + ","
										+ attr_wordCount.get("" + col + "@" + table + "") + "," + tfDB.get(kw),
								entry.getKey().split(" @@ ")[0]);
						order++;
						nextRow++;
					} else {
						rowi.createCell(0).setCellValue(order);
						rowi.createCell(1).setCellValue(formatter.format(entry.getValue()));
						rowi.createCell(2).setCellValue(attr_wordCount.get("" + col + "@" + table + ""));
						rowi.createCell(3).setCellValue(tfDB.get(kw));
						rowi.createCell(4).setCellValue(exInDB.get(kw));
						rowi.createCell(5).setCellValue("remove");
						temp.put(
								order + "," + formatter.format(entry.getValue()) + ","
										+ attr_wordCount.get("" + col + "@" + table + "") + "," + tfDB.get(kw),
								entry.getKey().split(" @@ ")[0]);
						order++;
						nextRow++;
					}
				}
			}
		}

		System.out.println("------------------------------------------------------");
		System.out.println("temp: ");
		for (Entry<String, String> entry : temp.entrySet()) {
			System.out.println(entry);
		}

		// Write the output to a file
		FileOutputStream fileOut = new FileOutputStream(path2 + "query.xls");
		wb.write(fileOut);
		fileOut.close();

		// classifier
		AttributeDisambiguationClassifierModel cl = new AttributeDisambiguationClassifierModel();
		cl.xlsToCsv(nextRow);
		cl.csvToArff();
		System.out.println("---------------------------- Attribute Disambiguation --------------------------------");
		cl.pruningClassifier();

		BufferedReader br = new BufferedReader(new FileReader(path2 + "PredictedResults.txt"));

		System.out.println();
		ArrayList<Integer> removeElements = new ArrayList<Integer>();
		String contentLine;
		while ((contentLine = br.readLine()) != null) {
			if (contentLine.split(" : ")[1].equals("remove")) {
				String str = contentLine.split(" : ")[0];
				ArrayList<Double> predictedArr = new ArrayList<Double>();
				for (int i = 0; i < 4; i++) {
					predictedArr.add(Double.parseDouble(str.split(",")[i]));
				}
				for (Entry<String, String> entry1 : temp.entrySet()) {
					ArrayList<Double> tempArr = new ArrayList<Double>();
					for (int j = 0; j < 4; j++) {
						tempArr.add(Double.parseDouble(entry1.getKey().split(",")[j]));
					}
					if (tempArr.equals(predictedArr)) {
						int key = Integer.parseInt(entry1.getValue());
						for (Map.Entry<Integer, ArrayList<String>> entry : matchListInfo.entrySet()) {
							if (entry.getKey() == key) {
								removeElements.add(entry.getKey());
								System.out.println("Removed Attributes by Attribute Disambiguation: "
										+ matchListInfo.get(entry.getKey()));
								matchListInfo.remove(entry.getKey());
								break;
							}
						}
					}
				}
			}
		}
		br.close();

		System.out.println();
		System.out.println("-------- Elements to be Removed by Attribute Disambiguation: -----------");
		for (int rem : removeElements) {
			System.out.println(rem);
			for (Entry<String, Double> entry : kw_probability.entrySet()) {
				if (Integer.parseInt(entry.getKey().split(" @@ ")[0]) == rem) {
					kw_probability.remove(entry.getKey());
					break;
				}
			}
		}

		System.out.println();
		System.out.println("------------------------------------------------------------");
		System.out.println("new kw_pro: ");
		for (Entry<String, Double> entry : kw_probability.entrySet()) {
			System.out.println(entry);
		}
	}
	// ---------------------------------------------------------------

	public static LinkedHashMap<String, Integer> wordCount() throws IOException, SQLException {
		LinkedHashMap<String, Integer> wordCount_Attr = new LinkedHashMap<String, Integer>();
		ArrayList<String> stopwords = new ArrayList<String>(
				new ArrayList<String>(Arrays.asList("a", "about", "an", "are", "as", "at", "be", "by", "com", "de",
						"en", "for", "from", "how", "i", "in", "is", "it", "la", "of", "on", "or", "that", "the",
						"this", "to", "was", "what", "when", "where", "who", "will", "with", "and", "the", "www")));
		Connection conn = null;
		conn = ConnectionToMySql.getConnection();
		Statement stmt = null;
		ResultSet rs = null;

		for (int q = 0; q < tableList.length; q++) {
			for (int h = 0; h < no_Of_Atts; h++) {
				if (attributeMatrix[q][h] != null) {
					int wordCount = 0;
					stmt = conn.createStatement();
					rs = stmt.executeQuery("SELECT " + attributeMatrix[q][h] + " FROM " + "yelp." + tableList[q]);
					while (rs.next()) {
						for (String s : rs.getString(attributeMatrix[q][h]).split(" ")) {
							if (!s.replaceAll("[^a-zA-Z\\t]", "").toLowerCase().equals("")
									&& !stopwords.contains(s.replaceAll("[^a-zA-Z\\t]", "").toLowerCase()))
								wordCount++;
						}
					}
					wordCount_Attr.put(attributeMatrix[q][h] + "@" + tableList[q], wordCount);
				}
			}
		}
		return wordCount_Attr;
	}
	// ---------------------------------------------------------------

	public static double hristidis(String query, String attrComb) throws SQLException, IOException {
		double score = 0.0;
		LinkedHashMap<String, Integer> colSize = new LinkedHashMap<String, Integer>();
		colSize.put("name@attribute", 18270957);
		colSize.put("value@attribute", 3890909);
		colSize.put("name@business", 2570632);
		colSize.put("neighborhood@business", 651548);
		colSize.put("address@business", 2631536);
		colSize.put("city@business", 1277822);
		colSize.put("state@business", 317392);
		colSize.put("category@category", 6480708);
		colSize.put("hours@hours", 13594483);
		LinkedHashMap<String, Integer> table_rowCount = new LinkedHashMap<String, Integer>();
		table_rowCount.put("attribute", 1153838);
		table_rowCount.put("category", 590290);
		table_rowCount.put("business", 156639);
		table_rowCount.put("hours", 734421);
		LinkedHashMap<String, Double> colTable_avgSize = new LinkedHashMap<String, Double>();
		colTable_avgSize.put("name@attribute", (double) (18270957 / 1153838));
		colTable_avgSize.put("value@attribute", (double) 3890909 / 1153838);
		colTable_avgSize.put("name@business", (double) 2570632 / 156639);
		colTable_avgSize.put("neighborhood@business", (double) 651548 / 156639);
		colTable_avgSize.put("address@business", (double) 2631536 / 156639);
		colTable_avgSize.put("city@business", (double) 1277822 / 156639);
		colTable_avgSize.put("state@business", (double) 317392 / 156639);
		colTable_avgSize.put("category@category", (double) 6480708 / 590290);
		colTable_avgSize.put("hours@hours", (double) 13594483 / 734421);
		LinkedHashMap<String, Double> kwTableCol_tf = new LinkedHashMap<String, Double>();
		LinkedHashMap<String, Integer> kwTable_df = new LinkedHashMap<String, Integer>();
		LinkedHashMap<String, Double> colTable_score = new LinkedHashMap<String, Double>();

		for (String kw : query.split(" ")) {
			for (int q = 0; q < tableList.length; q++) {
				for (int h = 0; h < no_Of_Atts; h++) {
					if (attributeMatrix[q][h] != null) {
						BufferedReader br = new BufferedReader(new FileReader(
								path1 + "HistogramWordsCountIn" + tableList[q] + attributeMatrix[q][h] + ".txt"));
						String contentLine;
						while ((contentLine = br.readLine()) != null) {
							if (contentLine.split("@")[0].equals(kw)) {
								kwTableCol_tf.put(kw + " @ " + tableList[q] + " @ " + attributeMatrix[q][h],
										Double.parseDouble(contentLine.split("@")[1]));
							}
						}
						br.close();
					}
				}
			}
		}

		// System.out.println("kwTableCol_tf: ");
		// for (Entry<String, Double> entry1 : kwTableCol_tf.entrySet()) {
		// System.out.println(entry1);
		// }

		for (String kw : query.split(" ")) {
			for (Entry<Integer, ArrayList<String>> entry : matchListInfo.entrySet()) {
				if (entry.getValue().get(0).equals(kw)) {
					String table = entry.getValue().get(1);
					Integer count = Integer.parseInt(entry.getValue().get(3));
					if (!kwTable_df.keySet().contains("" + kw + " @ " + table + "")) {
						kwTable_df.put("" + kw + " @ " + table + "", count);
					} else {
						int temp = kwTable_df.get("" + kw + " @ " + table + "");
						kwTable_df.replace("" + kw + " @ " + table + "", temp + count);
					}
				}
			}
		}

		// System.out.println();
		// System.out.println("kwTable_df: ");
		// for (Entry<String, Integer> entry1 : kwTable_df.entrySet()) {
		// System.out.println(entry1);
		// }

		for (int q = 0; q < tableList.length; q++) {
			for (int h = 0; h < no_Of_Atts; h++) {
				if (attributeMatrix[q][h] != null) {
					score = 0.0;
					for (String kw : query.split(" ")) {
						if (kwTableCol_tf.keySet()
								.contains("" + kw + " @ " + tableList[q] + " @ " + attributeMatrix[q][h] + "")
								&& colTable_avgSize.keySet()
										.contains("" + attributeMatrix[q][h] + "@" + tableList[q] + "")
								&& kwTable_df.keySet().contains("" + kw + " @ " + tableList[q] + "")) {
							score = score + ((1 + Math.log((1 + Math.log(kwTableCol_tf
									.get("" + kw + " @ " + tableList[q] + " @ " + attributeMatrix[q][h] + "")))))
									/ (0.8 + (0.2 * (colSize.get("" + attributeMatrix[q][h] + "@" + tableList[q] + "")
											/ colTable_avgSize
													.get("" + attributeMatrix[q][h] + "@" + tableList[q] + ""))))
									* (Math.log((table_rowCount.get("" + tableList[q] + "") + 1)
											/ kwTable_df.get("" + kw + " @ " + tableList[q] + ""))));
						}
					}
					colTable_score.put("" + attributeMatrix[q][h] + " @ " + tableList[q] + "", score);
				}
			}
		}

		// System.out.println();
		// System.out.println("colTable_score: ");
		// for (Entry<String, Double> entry1 : colTable_score.entrySet()) {
		// System.out.println(entry1);
		// }

		double hristisSum = 0.0;
		for (String a : attrComb.split(",")) {
			String str = matchListInfo.get(Integer.parseInt(a)).get(2) + " @ "
					+ matchListInfo.get(Integer.parseInt(a)).get(1);
			for (Entry<String, Double> entry1 : colTable_score.entrySet()) {
				if (entry1.getKey().equals(str)) {
					hristisSum = hristisSum + entry1.getValue();
				}
			}
		}
		return hristisSum;
	}
	// --------------------------------------------------

	public static double prScore(String attComb, LinkedHashMap<String, Double> kw_probability)
			throws IOException, SQLException {
		double score = 1.0;
		for (String s : attComb.split(",")) {
			for (Map.Entry<String, Double> entry : kw_probability.entrySet()) {
				if (entry.getKey().split(" @@ ")[0].equals(s)) {
					score = score * entry.getValue();
				}
			}
		}
		return score;
	}
	// --------------------------------------------------------------

	public static LinkedHashMap<String, ArrayList<String>> attrCombCreator(List<Integer> kwKeys, int querySize)
			throws IOException, SQLException {
		double start = System.nanoTime();
		LinkedHashMap<String, ArrayList<String>> ir_attKeysComb = new LinkedHashMap<String, ArrayList<String>>();
		ArrayList<Set<Integer>> allAttCombs = SubsetCreation.getSubsets(kwKeys, querySize);
		System.out.println("");
		System.out.println("-----------------------------------------------------------");
		System.out.println("allAttCombs: ");
		System.out.println(allAttCombs);
		System.out.println();

		// check if combination contains all the query keywords
		for (Set<Integer> ac : allAttCombs) {
			Set<String> wordsOfComb = new HashSet<String>();
			Set<String> tablesOfComb = new HashSet<String>();
			int i = 0;
			StringBuilder attrCombKeys = new StringBuilder();
			for (int a : ac) {
				wordsOfComb.add(matchListInfo.get(a).get(0));
				tablesOfComb.add(matchListInfo.get(a).get(1));
				if (i++ == ac.size() - 1) {
					attrCombKeys.append(Integer.toString(a)).toString();
				} else {
					attrCombKeys.append(Integer.toString(a)).append(",");
				}
			}
			/////////////////////
			String tables = tablesOfComb.toString().replace("[", "").replace("]", "").replaceAll(" ", "");
			if (wordsOfComb.size() == querySize) {
				if (ir_attKeysComb.keySet().contains(tables)) {
					ArrayList<String> val = new ArrayList<String>();
					val = ir_attKeysComb.get(tables);
					val.add(attrCombKeys.toString());
					ir_attKeysComb.replace(tables, val);
				} else {
					ir_attKeysComb.put(tables, new ArrayList<String>(Arrays.asList(attrCombKeys.toString())));
				}
			}
		}

		System.out.println("");
		System.out.println("-----------------------------------------------------------");
		System.out.println("ir_attKeysComb: ");
		for (Entry<String, ArrayList<String>> entry : ir_attKeysComb.entrySet()) {
			System.out.println(entry);
		}
		System.out.println("");
		double end = System.nanoTime();
		System.out.println("Execution Time in attrCombCreator Method: " + (end - start) / 1000000000 + " s");
		System.out.println("");
		return ir_attKeysComb;

	}
	// -------------------------------------------------------------------------------------------

	public static LinkedHashMap<String, Double> tupleEstimation(LinkedHashMap<String, ArrayList<String>> IS_AttKeyCombs)
			throws IOException, SQLException {
		LinkedHashMap<String, Integer> table_allTuplesCount = new LinkedHashMap<String, Integer>();
		table_allTuplesCount.put("business", 156639);
		table_allTuplesCount.put("attribute", 1153838);
		table_allTuplesCount.put("category", 590290);
		table_allTuplesCount.put("hours", 734421);
		LinkedHashMap<String, Integer> table_distinctBIdCount = new LinkedHashMap<String, Integer>();
		table_distinctBIdCount.put("business", 156639);
		table_distinctBIdCount.put("attribute", 138321);
		table_distinctBIdCount.put("category", 156261);
		table_distinctBIdCount.put("hours", 114989);
		LinkedHashMap<String, Double> ac_tupleEstimation = new LinkedHashMap<String, Double>();

		System.out.println(" ");
		for (Entry<String, ArrayList<String>> entry1 : IS_AttKeyCombs.entrySet()) {
			String IS = entry1.getKey().replace("[", "").replace("]", "").replace(",", "").replace("<", "").replace(">",
					"");
			HashSet<String> allIRsofIsSet = new HashSet<String>();
			for (String s : IS.split(" ")) {
				if (allIRs.contains(s)) {
					allIRsofIsSet.add(s);
				}
			}

			for (String attComb : entry1.getValue()) {
				// attComb = attComb.replace("[", "").replace("]", "");
				LinkedHashMap<String, Double> table_kwCounts = new LinkedHashMap<String, Double>();
				for (String a : attComb.split(",")) {
					String table = matchListInfo.get(Integer.parseInt(a)).get(1);
					if (!table_kwCounts.containsKey(table)) {
						table_kwCounts.put(table, Double.parseDouble(matchListInfo.get(Integer.parseInt(a)).get(3)));
					} else if (table_kwCounts.containsKey(table)) {
						double temp = table_kwCounts.get(table)
								+ Double.parseDouble(matchListInfo.get(Integer.parseInt(a)).get(3));
						table_kwCounts.replace(table, temp);
					}
				}
				double tupleCounts = Collections.min(table_kwCounts.values());
				ac_tupleEstimation.put(attComb, tupleCounts);
			}
		}
		return ac_tupleEstimation;
	}

	// --------------------------------------------------------------

	public static void acDisambiguation(String query, LinkedHashMap<String, ArrayList<String>> is_attKeyCombs,
			LinkedHashMap<String, Double> kw_probability) throws Exception {
		LinkedHashMap<String, String> temp2 = new LinkedHashMap<String, String>();
		DecimalFormat formatter = new DecimalFormat("#0.000000");
		System.out.println();
		System.out.println("---------------- AC Disambiguation -----------------------------");

		// creating query file (testFile)
		Workbook wb = new HSSFWorkbook();
		Sheet sheet = wb.createSheet("new sheet");
		Row row0 = sheet.createRow((short) 0);
		row0.createCell(0).setCellValue("MinPr");
		row0.createCell(1).setCellValue("NumberofTables");
		row0.createCell(2).setCellValue("NumberofColumns");
		row0.createCell(3).setCellValue("Hristidis");
		row0.createCell(4).setCellValue("Score");

		for (Entry<String, ArrayList<String>> entry1 : is_attKeyCombs.entrySet()) {
			for (String attComb : entry1.getValue()) {
				ArrayList<Double> pr = new ArrayList<Double>();
				HashSet<String> tables = new HashSet<String>();
				HashSet<String> tableCol = new HashSet<String>();
				for (String s : attComb.split(",")) {
					for (Entry<String, Double> entry2 : kw_probability.entrySet()) {
						if (entry2.getKey().split(" @@ ")[0].equals(s)) {
							tableCol.add(entry2.getKey().split(" @@ ")[3] + entry2.getKey().split(" @@ ")[2]);
							tables.add(entry2.getKey().split(" @@ ")[3]);
							pr.add(entry2.getValue());
							break;
						}
					}
				}
				double hrisValue = hristidis(query, attComb) / (tables.size() * tableCol.size());
				Row rowi = sheet.createRow((short) nextRow);
				rowi.createCell(0).setCellValue(Collections.min(pr));
				rowi.createCell(1).setCellValue(tables.size());
				rowi.createCell(2).setCellValue(tableCol.size());
				rowi.createCell(3).setCellValue(hrisValue);
				rowi.createCell(4).setCellValue(5);
				nextRow++;
				temp2.put(attComb, formatter.format(Collections.min(pr)) + "," + tables.size() + "," + tableCol.size()
						+ "," + formatter.format(hrisValue));
			}
		}

		// Write the output to a file
		FileOutputStream fileOut = new FileOutputStream("ACdata.xls");
		wb.write(fileOut);
		fileOut.close();

		AC_DisambiguationClassifierModel ccm = new AC_DisambiguationClassifierModel();
		ccm.ACdataxlsToCsv();
		ccm.ACdataCsvToArff();
		ccm.cut_offClassifier();

		BufferedReader br = new BufferedReader(new FileReader(path2 + "PredictedACResultsML2.txt"));
		System.out.println();
		ArrayList<String> removeElements = new ArrayList<String>();
		String contentLine;
		while ((contentLine = br.readLine()) != null) {
			if (Double.parseDouble(contentLine.split(" : ")[1]) <= 0.6) {
				String str = contentLine.split(" : ")[0];
				ArrayList<Double> predictedArr = new ArrayList<Double>();
				for (int i = 0; i < 4; i++) {
					predictedArr.add(Double.parseDouble(str.split(",")[i]));
				}
				for (Entry<String, String> entry1 : temp2.entrySet()) {
					ArrayList<Double> tempArr = new ArrayList<Double>();
					for (int j = 0; j < 4; j++) {
						tempArr.add(Double.parseDouble(entry1.getValue().split(",")[j]));
					}
					if (tempArr.equals(predictedArr)) {
						String attrcomb = entry1.getKey();
						for (Entry<String, ArrayList<String>> entry : is_attKeyCombs.entrySet()) {
							for (String s : entry.getValue()) {
								if (s.equals(attrcomb)) {
									removeElements.add(s);
									System.out.println("Removed ACs by AC Disambiguation: " + s);
									is_attKeyCombs.get(entry.getKey()).remove(s);
									break;
								}
							}
						}
					}
				}
			}
		}
		br.close();
	}
	// --------------------------------------------------------------

	public static int SQLBuilder(String query, String attrComb, String IS) throws IOException, SQLException {
		HashSet<String> bidIntersection = new HashSet<String>();
		System.out.println("--------------------In SQLBuilder----------------------");
		System.out.println("attrComb: " + attrComb);
		System.out.println("IS: " + IS);
		ArrayList<String[]> kw_table_att = new ArrayList<String[]>();
		BufferedWriter writer1 = new BufferedWriter(new FileWriter(path2 + "SQL.txt", true));

		int counter = 0;
		///////////////////////// changed part with intersection
		for (String a : attrComb.split(",")) {
			if (counter == 0) {
				bidIntersection = new HashSet(key_bids.get(Integer.parseInt(a)));
			} else {
				HashSet<String> tempIds = new HashSet<String>(key_bids.get(Integer.parseInt(a)));
				bidIntersection.retainAll(tempIds);
			}
			counter++;
		}
		//////////////////////////////

		String[] temp;
		for (String a : attrComb.split(",")) {
			temp = new String[3];
			temp[0] = matchListInfo.get(Integer.parseInt(a)).get(0); // kw
			temp[1] = matchListInfo.get(Integer.parseInt(a)).get(1); // table
			temp[2] = matchListInfo.get(Integer.parseInt(a)).get(2); // column
			kw_table_att.add(temp);
			System.out.println(temp[1] + "(" + temp[2] + ")=" + temp[0]);
			writer1.write(temp[1] + "=" + temp[2] + "(" + temp[0] + ")");
			writer1.newLine();
		}

		if (bidIntersection.size() == 0) {
			System.out.println("No Tuple!");
		}

		else if (bidIntersection.size() != 0) {
			IS = IS.replaceAll("<", " ").replaceAll(">", " ").replaceAll("\\[", " ").replaceAll("\\]", " ")
					.replaceAll(",", " ");
			ArrayList<String> isTables = new ArrayList<String>();
			for (String table : Arrays.asList(IS.split(" "))) {
				if (!table.equals("") && !table.equals(" ")) {
					isTables.add(table);
				}
			}

			String[][] edges = new String[20][2];
			if (isTables.size() > 1) {
				int row = 0;
				// edges is an array which keeps edges in the subgraph
				for (int i = 0; i < isTables.size(); i++) {
					if (isTables.get(i) != null && isTables.get(i + 1) != null) {
						edges[row][0] = isTables.get(i);
						edges[row][1] = isTables.get(i + 1);
						row++;
						i++;
					}
				}
			}

			///////////////
			String sqlSelect = "";
			for (String[] kta : kw_table_att) {
				if (!sqlSelect.contains(kta[1] + "." + kta[2])) {
					// business
					if (kta[1].equals("business")) {
						if (sqlSelect.equals(""))
							sqlSelect = kta[1] + "." + kta[2];
						else
							sqlSelect = sqlSelect + "," + kta[1] + "." + kta[2];
					}
					// other tables
					else {
						if (sqlSelect.equals(""))
							sqlSelect = "GROUP_CONCAT(distinct " + kta[1] + "." + kta[2] + " SEPARATOR ', ')";
						else
							sqlSelect = sqlSelect + "," + "GROUP_CONCAT(distinct " + kta[1] + "." + kta[2]
									+ " SEPARATOR ', ')";
					}
				}
			}

			/////////
			String sqlTableNames = "";
			if (isTables.size() == 1) {
				sqlTableNames = isTables.toString().replace("]", "").replace("[", "").replace("(", "").replace(")", "");
			} else if (isTables.size() > 1) {
				for (int i = 0; i < edges.length; i++) {
					if (edges[i][0] != null) {
						if (sqlTableNames.equals("") || i == 0) {
							sqlTableNames = "yelp." + edges[i][0] + " INNER JOIN yelp." + edges[i][1] + " on "
									+ edges[i][0] + ".business_id=" + edges[i][1] + ".business_id";
						} else {
							sqlTableNames = sqlTableNames + " INNER JOIN yelp." + edges[i][1] + " on " + edges[i][0]
									+ ".business_id=" + edges[i][1] + ".business_id";
						}
					}
				}
			}

			/////////////////

			String sqlConditions = isTables.get(0) + ".business_id in("
					+ bidIntersection.toString().replace("[", "'").replace("]", "'").replaceAll(", ", "','") + ")";

			////////
			if (!sqlConditions.equals("")) {
				String sqlStr = "SELECT " + isTables.get(0) + ".business_id, " + sqlSelect + " FROM " + sqlTableNames
						+ " where " + sqlConditions + " GROUP BY " + isTables.get(0) + ".business_id";
				String sqlStrPrint = "SELECT " + isTables.get(0) + ".business_id, " + sqlSelect + " FROM "
						+ sqlTableNames + " where sqlCondition GROUP BY business.business_id";

				System.out.println(sqlStrPrint);
				System.out.println("");
				writer1.write(sqlStr);
				writer1.newLine();

				Connection conn = null;
				conn = ConnectionToMySql.getConnection();
				Statement stmt = null;
				ResultSet rs = null;
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sqlStr);
				ResultSetMetaData rsmd = rs.getMetaData();
				int columnsNumber = rsmd.getColumnCount();
				int tupleCount = 0;
				while (rs.next()) {
					tupleCount++;
					String rowStr = "";
					for (int i = 1; i <= columnsNumber; i++) {
						if (rowStr.equals(""))
							rowStr = rs.getString(i);
						else
							rowStr = rowStr + " @ " + rs.getString(i);
					}
					System.out.println(rowStr.replaceAll("[\\t\\n\\r]+", " ") + " %% ");
					writer1.write(rowStr.replaceAll("[\\t\\n\\r]+", " ") + " %% ");
					writer1.newLine();
				}

				System.out.println();
				System.out.println("Number of Tuples: " + tupleCount);
				writer1.write("tupleCount: " + tupleCount + "");
				writer1.newLine();
				writer1.newLine();

				System.out.println();
				System.out.println("---------------------------------------------------------------");
				System.out.println();
			}

			writer1.close();
		}
		return bidIntersection.size();
	}
	
	// --------------------------------------------------------------
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) throws Exception {
		LinkedHashMap<String, Double> ac_tupleEstimation = new LinkedHashMap<String, Double>();
		LinkedHashMap<String, ArrayList<String>> IS_AttKeyCombs = new LinkedHashMap<String, ArrayList<String>>();
		List<Integer> kwKeys = new ArrayList<Integer>();

		double start = System.currentTimeMillis();
		cc();
		createGraph();
		// createIndex();
		// String query = "shoe footwear";
		// String query = "italian restaurant music maghdadddddddd";
		String query = "italian restaurant music";
		// String query = "auto insurance";
		// String query = "chinese food vegas";
		// String query = "auto insurance Tuesday";
		// String query="greek food";
		// String query="monday cafe Scottsdale";
		// String query ="restaurants karaoke old town";
		String newQuery = findInitialRelations(query);
		LinkedHashMap<String, Double> kw_probability = probabilityCalculation(newQuery);

		double start4 = System.currentTimeMillis();
		attributeDisambiguation(newQuery, kw_probability);
		double end4 = System.currentTimeMillis();
		System.out.println("");
		System.out.println("Execution Time in Attribute Disambiguation Method: " + (end4 - start4) / 1000 + " s");
		System.out.println("");
		System.out.println("---------------------------------------------------------------");
		System.out.println("");

		// finds Unique keys of KWs
		for (String kw : newQuery.split(" ")) {
			for (Entry<String, Double> entry : kw_probability.entrySet()) {
				if (entry.getKey().split(" @@ ")[1].equals(kw)) {
					kwKeys.add(Integer.parseInt(entry.getKey().split(" @@ ")[0]));
				}
			}
		}

		// In Case of Having Zero_tuples at the End
		for (int i = newQuery.split(" ").length; i > 0; i--) {
			int zero_ac_counter = 0;
			System.out.println("i: " + i);
			System.out.println("Checking Tuples for Query Size " + i + ": ");
			LinkedHashMap<String, ArrayList<String>> ir_attKeysComb = attrCombCreator(kwKeys, i);
			System.out.println("");
			System.out.println("------------------------------- Initial Subgraphs -------------------------------");
			System.out.println("");
			double start2 = System.nanoTime();
			for (Entry<String, ArrayList<String>> entry : ir_attKeysComb.entrySet()) {
				if (entry.getKey().split(",").length == 1) {
					System.out.println("[<" + entry.getKey() + ">]");
					IS_AttKeyCombs.put("<" + entry.getKey() + ">", entry.getValue());
				} else {
					LinkedList<String> otherNodes = new LinkedList<String>();
					for (String ir : entry.getKey().split(",")) {
						if (!ir.equals(entry.getKey().split(",")[0])) {
							otherNodes.add(ir);
						}
					}
					subgraphPath.clear();
					visited.clear();
					visited.add(entry.getKey().split(",")[0]);
					String initialSubgraph = ISCreation(visited, otherNodes);
					System.out.println(initialSubgraph);
					IS_AttKeyCombs.put(initialSubgraph, entry.getValue());
				}
			}

			System.out.println("");
			double end2 = System.nanoTime();
			System.out.println("Execution Time in ISCreation Method: " + (end2 - start2) / 1000000000 + " s");
			System.out.println("");

			if (i != 1) {
				acDisambiguation(newQuery, IS_AttKeyCombs, kw_probability);
			}

			System.out.println();
			System.out.println("--------    NEW IS:   -----------");
			for (Entry<String, ArrayList<String>> entry : IS_AttKeyCombs.entrySet()) {
				System.out.println(entry);
			}

			// Final Number of ACs
			int ac_count = 0;
			for (Entry<String, ArrayList<String>> entry1 : IS_AttKeyCombs.entrySet()) {
				ac_count += entry1.getValue().size();
			}
			System.out.println("ac_count: " + ac_count);

			double start6 = System.nanoTime();
			ac_tupleEstimation = tupleEstimation(IS_AttKeyCombs);
			double end6 = System.nanoTime();
			System.out.println("");
			System.out.println("-----------------------------------------------------");
			System.out.println("ac_tupleEstimation: ");
			for (Entry<String, Double> entry1 : ac_tupleEstimation.entrySet()) {
				System.out.println(entry1);
			}
			System.out.println("");
			System.out.println("Execution Time in tupleEstimation Method: " + (end6 - start6) / 1000 + " s");
			System.out.println("");

			double start5 = System.currentTimeMillis();
			for (Entry<String, ArrayList<String>> entry1 : IS_AttKeyCombs.entrySet()) {
				for (String attComb : entry1.getValue()) {
					int bidIntersection = SQLBuilder(newQuery, attComb, entry1.getKey());
					if (bidIntersection == 0) {
						zero_ac_counter++;
					}
				}
			}
			System.out.println("zero_ac_counter: " + zero_ac_counter);

			double end5 = System.currentTimeMillis();
			System.out.println("Execution Time in SQLBuilder Method: " + (end5 - start5) / 1000 + " s");
			System.out.println("");

			IS_AttKeyCombs.clear();
			if (ac_count != zero_ac_counter) {
				break;
			}
		}

		double end = System.currentTimeMillis();
		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("Execution Time: " + (end - start) / 1000 + " s");
	}
}
