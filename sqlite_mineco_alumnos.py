import sqlite3
import os
import csv

class SQLiteMineco:
    """Clase para gestionar la base de datos de indicadores económicos en SQLite."""

    def __init__(self, db_name="indicadores.db"):
        """Inicializa la conexión con la base de datos y crea la tabla si no existe."""
        self.db_name = db_name
        self.conn = sqlite3.con(self.db_name)
        self.cursor = 
        self._crear_tabla()
    
    def _crear_tabla(self):
        """Crea la tabla de indicadores económicos si no existe."""
        self.cursor.ex("""
            CREATE TABLE IF NOT EXISTS indicadores_economicos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha TEXT,
                valor REAL,
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

                    self.cursor.sqlExecutor(
                        "INSERT INTO indicadores_economicos (fecha, valor, nombre_indicador) VALUES (?, ?, ?)",
                        [(row[0], row[1], nombre_indicador) for row in reader]
                    )

        self.conn.commit()
        print(f"✅ Datos insertados en la base de datos desde {output_dir}.")

    def mostrar_datos(self, limit=10):
        """Muestra los últimos `limit` registros de indicadores económicos."""
        self.cursor.execute(
            "SELECT fecha, valor, nombre_indicador, fecha_insercion FROM ORDER BY DESC LIMIT ?",
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
        pass

# Script de ejecución
if __name__ == "__main__":
    print("Iniciando procesamiento de indicadores económicos...")
    db = SQLiteMineco("indicadores.db")
    db.insertar_indicadores()
    db.mostrar_datos(10)
    db.close()
    print("✅ Proceso finalizado.")
