import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.usermodel.HSSFDataFormat;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import java.util.Iterator;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import au.com.bytecode.opencsv.CSVWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import weka.classifiers.Classifier;
import weka.classifiers.trees.LMT;
import weka.core.Instances;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
import java.io.File;

public class AttributeDisambiguationClassifierModel {

	public static void majVotedDataToArff() throws Exception {
		// load CSV
		CSVLoader loader = new CSVLoader();
		loader.setSource(new File("C:\\Users\\LENOVO\\workspace\\Histogram\\MajorityVotedExclusivity.csv"));
		Instances data = loader.getDataSet();
		// save ARFF
		ArffSaver saver = new ArffSaver();
		saver.setInstances(data);
//		File f = new File("C:\\Users\\LENOVO\\workspace\\Histogram\\MajorityVotedExclusivity.arff");
//		if (f.exists()) {
//			f.delete();
//		}
		saver.setFile(new File("C:\\Users\\LENOVO\\workspace\\Histogram\\MajorityVotedExclusivity.arff"));
		saver.setDestination(new File("C:\\Users\\LENOVO\\workspace\\Histogram\\MajorityVotedExclusivity.arff"));
		saver.writeBatch();

	}
	// --------------------------------------------------------------------------------------------

	private static void createModel() throws Exception {
		Classifier cls = new LMT();
		Instances inst = new Instances(new BufferedReader(
				new FileReader("C:\\Users\\LENOVO\\workspace\\Histogram\\MajorityVotedExclusivity.arff")));
		inst.setClassIndex(inst.numAttributes() - 1);
		cls.buildClassifier(inst);
		weka.core.SerializationHelper
				.write("C:\\Users\\LENOVO\\workspace\\Histogram\\MajorityVotedExclusivity_LMT.model", cls);
	}

	// --------------------------------------------------------------

	public static void pruningClassifier() throws Exception {
		BufferedWriter bw = null;
		try {
			File file = new File("C:\\Users\\LENOVO\\workspace\\Histogram\\PredictedResults.txt");
			// file gets created if it is not present
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file);
			bw = new BufferedWriter(fw);

			Classifier cls = (Classifier) weka.core.SerializationHelper
					.read("C:\\Users\\LENOVO\\workspace\\Histogram\\MajorityVotedExclusivity_LMT.model");
			// load unlabeled data
			Instances unlabeled = new Instances(
					new BufferedReader(new FileReader("C:\\Users\\LENOVO\\workspace\\Histogram\\query.arff")));

			// set class attribute
			unlabeled.setClassIndex(unlabeled.numAttributes() - 1);
			for (int i1 = 0; i1 < unlabeled.numInstances(); i1++) {
				double value = cls.classifyInstance(unlabeled.instance(i1));
				String prediction1 = unlabeled.classAttribute().value((int) value);
				System.out.println("Predicted value for -> " + unlabeled.instance(i1) + " : " + prediction1);
				bw.write(unlabeled.instance(i1) + " : " + prediction1);
				bw.newLine();
			}
			System.out.println("File written Successfully");
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			try {
				if (bw != null)
					bw.close();
			} catch (Exception ex) {
				System.out.println("Error in closing the BufferedWriter" + ex);
			}
		}

	}

	// --------------------------------------------------------------
	public void xlsToCsv(int rowCount) throws IOException {
		// First we read the Excel file in binary format into FileInputStream
		FileInputStream input_document = new FileInputStream(
				new File("C:\\Users\\LENOVO\\workspace\\Histogram\\query.xls"));
		// Read workbook into HSSFWorkbook
		HSSFWorkbook my_xls_workbook = new HSSFWorkbook(input_document);
		// Read worksheet into HSSFSheet
		HSSFSheet my_worksheet = my_xls_workbook.getSheetAt(0);
		// To iterate over the rows
		Iterator<Row> rowIterator = my_worksheet.iterator();
		// OpenCSV writer object to create CSV file
		PrintWriter pw = new PrintWriter(new File("query.csv"));
		// Loop through rows.
		while (rowIterator.hasNext()) {
			Row row = rowIterator.next();
			StringBuilder sb = new StringBuilder();
			Iterator<Cell> cellIterator = row.cellIterator();
			while (cellIterator.hasNext()) {
				Cell cell = cellIterator.next(); // Fetch CELL
				if (cell.getColumnIndex() != 5) {
					switch (cell.getCellType()) { // Identify CELL type
					case Cell.CELL_TYPE_STRING:
						sb.append(cell.getStringCellValue());
						sb.append(',');
						break;
					case Cell.CELL_TYPE_NUMERIC:
						sb.append(cell.getNumericCellValue());
						sb.append(',');
						break;
					}
				} else if (cell.getColumnIndex() == 5) {
					switch (cell.getCellType()) { // Identify CELL type
					case Cell.CELL_TYPE_STRING:
						sb.append(cell.getStringCellValue());
						break;
					case Cell.CELL_TYPE_NUMERIC:
						sb.append(cell.getNumericCellValue());
						break;
					}
					sb.append('\n');
					pw.write(sb.toString());
				}
			}
		}

		pw.close();
		System.out.println("done!");
	}

	// ------------------------------------
	public void csvToArff() throws Exception {

		// load CSV
		CSVLoader loader = new CSVLoader();
		loader.setSource(new File("C:\\Users\\LENOVO\\workspace\\Histogram\\query.csv"));
		Instances data = loader.getDataSet();
		// save ARFF
		ArffSaver saver = new ArffSaver();
		saver.setInstances(data);
		File f = new File("C:\\Users\\LENOVO\\workspace\\Histogram\\query.arff");
		if (f.exists()) {
			f.delete();
		}
		saver.setFile(f);
		saver.writeBatch();

	}

	// --------------------------------------------------------------

	public static void main(String[] args) throws Exception {
		double start = System.currentTimeMillis();
//		majVotedDataToArff();
		createModel();
		// pruningClassifier();
		// End of program
		double end = System.currentTimeMillis();
		System.out.println("");
		System.out.println("********************  END  ******************** ");

		System.out.println("Execution Time : " + (end - start) / 1000 + " s");

	}

}