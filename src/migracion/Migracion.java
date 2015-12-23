package migracion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Migracion {

	static File dir; // carpeta temporal para las pruebas
	static final String ARCHIVO_PERSONAS = "datos/personas10.txt";
	static final String ARCHIVO_RESUMEN = "datos/resumen.txt";
	static int numeroInsercciones = 0;
	static int numeroErrores = 0;
	
	public static void main(String[] args) {

		dir = new File(ARCHIVO_PERSONAS);

		FileReader fr = null;
		BufferedReader br = null;

		try {

			fr = new FileReader(dir);

			// optimizar lectura con buffer
			br = new BufferedReader(fr);

			String[] aCampos;
			String linea = null;
			DbConnection conn = new DbConnection();
			PreparedStatement pst;
			//Leer el fichero e insertar en la base de datos
			while ((linea = br.readLine()) != null) {
				aCampos = linea.split(",");
			
				if (aCampos.length < 7) {
					numeroErrores++;
				} else {
					// sentencia sql
					String sql = "INSERT INTO `persona` (`nombre`,`email`,`observaciones`) VALUES (?,?,?);";
					// creamos consulta
					pst = conn.getConnection().prepareStatement(sql,PreparedStatement.RETURN_GENERATED_KEYS);
					pst.setString(1, aCampos[0] + " " + aCampos[1] + " "+ aCampos[2]);
					pst.setString(2, aCampos[4]);
					pst.setString(3, aCampos[6]);

					// ejecutar la consulta

					if (pst.executeUpdate() == 1) {

						ResultSet generatedKeys = pst.getGeneratedKeys();
						if (generatedKeys.next()) {
							generatedKeys.getInt(1);
							numeroInsercciones++;
						} else {
							throw new SQLException("Creating user failed, no ID obtained.");
						}

					}
					pst.close();

				}

			}
			// Cerramos conexión

			conn.desconectar();

		} catch (FileNotFoundException e) {
			System.out.println("No se ha encontrado el fichero " + dir);

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Excepcion generiaca" + e.getMessage());

		} finally {
			try {
				// cerrar buffer y reader
				if (br != null) {
					br.close();
				}
				if (fr != null) {
					fr.close();
				}

			} catch (Exception e) {
				System.out.println("Error cerrando buffer o reader");
			}
		}

		// Creación de fichero resumen

		FileWriter fw = null;
		PrintWriter pw = null;
		dir = new File(ARCHIVO_RESUMEN);
		try {
			fw = new FileWriter(dir);
			pw = new PrintWriter(fw, true);

			pw.println("Resumen de la migración: ");
			pw.println("------------------------");
			pw.println();
			pw.println("Número de insercciones realizadas: "+ numeroInsercciones);
			pw.println("Número de errores: "+ numeroErrores);
		} catch (Exception e) {

			System.out.println("Excepcion: " + e.getMessage());

		} finally {

			try {
				if (pw != null) {
					pw.close();
				}
				if (fw != null) {
					fw.close();
				}
			} catch (IOException e) {

				System.out.println("Excepcion cerrando recursos: " + e.getMessage());
				e.printStackTrace();
			}

		}

	}

}
