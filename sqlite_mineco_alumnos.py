import sqlite3
import os
import csv
import pandas as pd

class SQLiteMineco:
    """Clase para gestionar la base de datos de indicadores económicos en SQLite."""

    def __init__(self, db_name="indicadores.db"):
        """Inicializa la conexión con la base de datos y crea la tabla si no existe."""
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        self._crear_tabla()
    
    def _crear_tabla(self):
        """Crea la tabla de indicadores económicos si no existe."""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS indicadores_economicos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha TEXT,
                valor REAL,
                nombre_indicador TEXT,
                fecha_insercion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()

    def insertar_indicadores(self, output_dir="data/indicadores"):
        """Inserta los datos de los CSV en la base de datos SQLite."""
        for csv_file in os.listdir(output_dir):
            if csv_file.endswith(".csv"):
                file_path = os.path.join(output_dir, csv_file)
                nombre_indicador = csv_file.replace("indicadores_economicos_", "").replace(".csv", "")

                with open(file_path, mode="r", encoding="utf-8") as file:
                    reader = csv.reader(file, delimiter="\t")
                    next(reader)  # Omitir encabezados

                    self.cursor.executemany(
                        "INSERT INTO indicadores_economicos (fecha, valor, nombre_indicador) VALUES (?, ?, ?)",
                        [(row[0], float(row[1]), nombre_indicador) for row in reader]
                    )

        self.conn.commit()
        print(f"✅ Datos insertados en la base de datos desde {output_dir}.")

    def mostrar_datos(self, limit=10):
        """Muestra los últimos `limit` registros de indicadores económicos."""
        self.cursor.execute(
            "SELECT fecha, valor, nombre_indicador, fecha_insercion FROM indicadores_economicos ORDER BY fecha_insercion DESC LIMIT ?",
            (limit,)
        )
        rows = self.cursor.fetchall()

        print("\nÚltimos indicadores económicos insertados:")
        print("-" * 60)
        for row in rows:
            print(f"Fecha: {row[0]} | Valor: {row[1]:.2f} | Indicador: {row[2]} | Insertado: {row[3]}")
        print("-" * 60)

    def close(self):
        """Cierra la conexión con la base de datos."""
        self.conn.close()

    def opcional_pandas(self, archivo):
        df = pd.read_sql_query("SELECT * FROM indicadores_economicos", self.conn)
        df_meses = df.sort_values(by="fecha")
        df_meses = df_meses[['fecha', 'valor']]
        df_meses = df_meses.groupby('fecha').agg({'valor': 'mean'}).reset_index()
        df_meses['varicacion_mensual'] = ((df_meses['valor'] - df_meses['valor'].shift(1)) / df_meses['valor'].shift(1)) * 100
        df_meses['varicacion_mensual'] = df_meses['varicacion_mensual'].apply(lambda x: f"{x:.2f}%")
        print("\nDatos de la base de datos en un DataFrame de Pandas:")
        print(df_meses)
    
# Script de ejecución
if __name__ == "__main__":
    print("Iniciando procesamiento de indicadores económicos...")
    db = SQLiteMineco("indicadores.db")
    db.insertar_indicadores()
    db.mostrar_datos(10)
    print("✅ Proceso finalizado.")
    db.opcional_pandas("indicadores.db")
    db.close()
