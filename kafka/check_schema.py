import psycopg2

DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "happiness_db"
DB_USER = "admin"
DB_PASS = "adminpassword"

def inspect_schema():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    cursor = conn.cursor()

    # Listar tablas creadas
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public';
    """)
    tables = cursor.fetchall()
    
    print("=== TABLAS EXISTENTES EN POSTGRESQL ===")
    for t in tables:
        print(f"📌 Tabla: {t[0]}")
    
    print("\n=== DETALLE DE ESTRUCTURA Y COLUMNAS ===")
    for t in tables:
        table_name = t[0]
        print(f"\n--- Estrutura de '{table_name}' ---")
        cursor.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table_name}';
        """)
        columns = cursor.fetchall()
        for col in columns:
            print(f"  • Columna: {col[0]:<25} | Tipo: {col[1]:<15} | Nullable: {col[2]}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    inspect_schema()