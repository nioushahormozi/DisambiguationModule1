
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.usermodel.HSSFSheet;

import java.util.Arrays;
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
import weka.classifiers.evaluation.Evaluation;
import weka.classifiers.evaluation.output.prediction.PlainText;
import weka.classifiers.meta.RandomCommittee;
import weka.classifiers.trees.LMT;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;
import weka.core.Range;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
import weka.core.converters.ConverterUtils.DataSource;

import java.io.File;

public class AC_DisambiguationClassifierModel {

	private static void createModel() throws Exception {
		Classifier cls = new RandomCommittee();

		Instances inst = new Instances(
				new BufferedReader(new FileReader("C:\\Users\\LENOVO\\workspace\\Histogram\\AcCutoffAllData.arff")));
		inst.setClassIndex(inst.numAttributes() - 1);
		cls.buildClassifier(inst);
		weka.core.SerializationHelper.write("C:\\Users\\LENOVO\\workspace\\Histogram\\ML2_RandomCommittee.model", cls);
	}

	// --------------------------------------------------------------

	public static void cut_offClassifier() throws Exception {
		BufferedWriter bw = null;
		try {
			File file = new File("C:\\Users\\LENOVO\\workspace\\Histogram\\PredictedACResultsML2.txt");
			// file gets created if it is not present
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file);
			bw = new BufferedWriter(fw);

			Classifier cls = (Classifier) weka.core.SerializationHelper
					.read("C:\\Users\\LENOVO\\workspace\\Histogram\\ML2_RandomForest.model");
			// load unlabeled data
			Instances unlabeled = new Instances(
					new BufferedReader(new FileReader("C:\\Users\\LENOVO\\workspace\\Histogram\\ACdata.arff")));
			// set class attribute
			unlabeled.setClassIndex(unlabeled.numAttributes() - 1);
			System.out.println("unlabeled.numInstances(): " + unlabeled.numInstances());
			for (int i = 0; i < unlabeled.numInstances(); i++) {
				double value = cls.classifyInstance(unlabeled.instance(i));
				System.out.println("Predicted value for -> " + unlabeled.instance(i) + " : " + value);
				bw.write(unlabeled.instance(i) + " : " + value);
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

	// ------------------------------------
	public static void ACdataCsvToArff() throws Exception {
		// load CSV
		CSVLoader loader = new CSVLoader();
		loader.setSource(new File("C:\\Users\\LENOVO\\workspace\\Histogram\\ACdata.csv"));
		Instances data = loader.getDataSet();
		// save ARFF
		ArffSaver saver = new ArffSaver();
		saver.setInstances(data);
		File f = new File("C:\\Users\\LENOVO\\workspace\\Histogram\\ACdata.arff");
		if (f.exists()) {
			f.delete();
		}
		saver.setFile(new File("C:\\Users\\LENOVO\\workspace\\Histogram\\ACdata.arff"));
		saver.writeBatch();
	}

	// --------------------------------------------------------------
	public static void ACdataxlsToCsv() throws IOException {
		// First we read the Excel file in binary format into FileInputStream
		FileInputStream input_document = new FileInputStream(
				new File("C:\\Users\\LENOVO\\workspace\\Histogram\\ACdata.xls"));
		// Read workbook into HSSFWorkbook
		HSSFWorkbook my_xls_workbook = new HSSFWorkbook(input_document);
		// Read worksheet into HSSFSheet
		HSSFSheet my_worksheet = my_xls_workbook.getSheetAt(0);
		// To iterate over the rows
		Iterator<Row> rowIterator = my_worksheet.iterator();
		// OpenCSV writer object to create CSV file
		PrintWriter pw = new PrintWriter(new File("ACdata.csv"));
		// Loop through rows.
		while (rowIterator.hasNext()) {
			Row row = rowIterator.next();
			StringBuilder sb = new StringBuilder();
			Iterator<Cell> cellIterator = row.cellIterator();
			while (cellIterator.hasNext()) {
				Cell cell = cellIterator.next(); // Fetch CELL
				if (cell.getColumnIndex() != 4) {
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
				} else if (cell.getColumnIndex() == 4) {
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
	public static void AcCutoffCsvToArff() throws Exception {
		// load CSV
		CSVLoader loader = new CSVLoader();
		loader.setSource(new File("C:\\Users\\LENOVO\\workspace\\Histogram\\AcCutoffAllData.csv"));
		Instances data = loader.getDataSet();
		// save ARFF
		ArffSaver saver = new ArffSaver();
		saver.setInstances(data);
		File f = new File("C:\\Users\\LENOVO\\workspace\\Histogram\\AcCutoffAllData.arff");
		if (f.exists()) {
			f.delete();
		}
		saver.setFile(f);
		saver.writeBatch();
	}

	// --------------------------------------------------------------

	public static void main(String[] args) throws Exception {
		double start = System.currentTimeMillis();
		AcCutoffCsvToArff();
		ACdataxlsToCsv();
		ACdataCsvToArff();
		createModel();
		cut_offClassifier();
		// End of program
		double end = System.currentTimeMillis();
		System.out.println("");
		System.out.println("********************  END  ******************** ");

		System.out.println("Execution Time : " + (end - start) / 1000 + " s");

	}

}
