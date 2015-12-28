package migracion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Clase que inserta en una base de datos desde un fichero de texto. 
 * Lee una fila del fichero de texto, separa sus campos e inserta los campos expecificados en la base de datos. 
 * Al terminar crea un archivo txt resumen del proceso.
 * 
 * @author Oscar
 *
 */

public class Migracion {

	// Variables globales

	// Medicion de tiempos de ejecucion

	static long tInicio;
	static long tFin;

	static final String ARCHIVO_PERSONAS = "datos/personas.txt";
	static final String ARCHIVO_RESUMEN = "datos/resumen.txt";
	static int numeroInsercciones = 0;
	static int numeroErrores = 0;

	public static void main(String[] args) {

		System.out.println("Comenzando migracion......");

		tInicio = System.currentTimeMillis();

		FileReader fr = null;
		BufferedReader br = null;

		try {
			// Abrir y leer fichero

			fr = new FileReader(ARCHIVO_PERSONAS);

			// Optimizar lectura con buffer
			br = new BufferedReader(fr);

			String[] aCampos;
			String linea = null;

			// Abrir conexion base de datos

			// Clase externa para gestionar la conexión a base de datos
			DbConnection db = new DbConnection();
			Connection conexion = db.getConnection();
			PreparedStatement pst;

			// Leer el fichero e insertar en la base de datos

			while ((linea = br.readLine()) != null) {

				aCampos = linea.split(",");

				if (aCampos.length != 7) {
					numeroErrores++;
				} else {

					// Sentencia sql
					String sql = "INSERT INTO `persona` (`nombre`,`email`,`observaciones`) VALUES (?,?,?);";

					// Creamos consulta

					/*
					 * aCampos contiene 7 campos: 
					 * [0] Nombre 
					 * [1] Apellido1 
					 * [2] Apellido2
					 * [3] Edad 
					 * [4] Email 
					 * [5] Dni 
					 * [6] Rol
					 */

					pst = conexion.prepareStatement(sql);
					pst.setString(1, aCampos[0] + " " + aCampos[1] + " " + aCampos[2]);
					pst.setString(2, aCampos[4]);
					pst.setString(3, aCampos[6]);

					// Ejecutar la consulta

					if (pst.executeUpdate() == 1) {

						numeroInsercciones++;
						System.out.println(numeroInsercciones + " - " + linea);

					} else {
						System.out.println("ERROR no ha insertado ");
						numeroErrores++;
					}
					pst.close();

				}

			} // end while

			// Cerramos conexión
			db.desconectar();
			tFin = System.currentTimeMillis();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("Fichero no encontrado: " + ARCHIVO_PERSONAS);

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Excepcion genérica" + e.getMessage());

		} finally {
			try {
				// Cerrar buffer y reader
				if (br != null) {
					br.close();
				}
				if (fr != null) {
					fr.close();
				}

			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Error cerrando buffer o reader");
			}
		}

		// Creación de fichero resumen

		FileWriter fw = null;
		PrintWriter pw = null;

		try {
			fw = new FileWriter(ARCHIVO_RESUMEN);
			pw = new PrintWriter(fw, true);

			pw.println("Resumen de la migración: ");
			pw.println("------------------------");
			pw.println("Proceso terminado en : " + ((tFin - tInicio) / 1000) + " segundos");
			pw.println();
			pw.println("Número de insercciones realizadas: " + numeroInsercciones);
			pw.println("Número de errores: " + numeroErrores);

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
