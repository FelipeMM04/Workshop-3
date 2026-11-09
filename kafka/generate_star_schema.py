import matplotlib.pyplot as plt
import matplotlib.patches as patches

def draw_star_schema():
    print("🎨 Generando el Diagrama en Estrella (Star Schema) del Step 7...")
    
    fig, ax = plt.subplots(figsize=(14, 9))
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 9)
    ax.axis('off')

    # Configuración de colores
    FACT_COLOR = '#1e3a8a'      # Azul Oscuro (Hechos)
    DIM_COLOR = '#0d9488'       # Verde Azulado (Dimensiones)
    RAW_COLOR = '#475569'       # Gris (Raw/Staging)
    TEXT_BG = '#f8fafc'

    def draw_table(x, y, width, height, title, columns, header_color):
        # Dibujar cuerpo de la tabla
        rect = patches.FancyBboxPatch(
            (x, y), width, height,
            boxstyle="round,pad=0.02,rounding_size=0.1",
            linewidth=1.5, edgecolor=header_color, facecolor=TEXT_BG
        )
        ax.add_patch(rect)

        # Dibujar encabezado
        header_height = 0.6
        header_rect = patches.FancyBboxPatch(
            (x, y + height - header_height), width, header_height,
            boxstyle="round,pad=0.02,rounding_size=0.1",
            linewidth=0, facecolor=header_color
        )
        ax.add_patch(header_rect)

        # Texto del encabezado
        ax.text(
            x + width / 2, y + height - (header_height / 2), title,
            color='white', weight='bold', fontsize=10, ha='center', va='center'
        )

        # Dibujar columnas
        curr_y = y + height - header_height - 0.3
        for col, is_pk_fk in columns:
            col_text = f"• {col}"
            weight = 'bold' if is_pk_fk else 'normal'
            color = '#b91c1c' if 'PK' in is_pk_fk else ('#0369a1' if 'FK' in is_pk_fk else '#334155')
            
            ax.text(x + 0.3, curr_y, col_text, fontsize=9, weight=weight, color='#0f172a', ha='left', va='center')
            if is_pk_fk:
                ax.text(x + width - 0.3, curr_y, is_pk_fk, fontsize=8, weight='bold', color=color, ha='right', va='center')
            curr_y -= 0.32

    # 1. Tabla de Hechos Central (FACT_PREDICTIONS)
    fact_cols = [
        ("prediction_id", "PK"),
        ("raw_event_id", ""),
        ("raw_id", "FK"),
        ("country_id", "FK"),
        ("date_id", "FK"),
        ("actual_score", ""),
        ("predicted_score", ""),
        ("prediction_error", ""),
        ("prediction_timestamp", "")
    ]
    draw_table(5.0, 2.5, 4.0, 3.8, "fact_predictions (Fact Table)", fact_cols, FACT_COLOR)

    # 2. Dimensión País (DIM_COUNTRY) - Izquierda
    country_cols = [
        ("country_id", "PK"),
        ("country_name", "UNIQUE"),
        ("created_at", "")
    ]
    draw_table(0.5, 3.2, 3.2, 2.0, "dim_country (Dimension)", country_cols, DIM_COLOR)

    # 3. Dimensión Fecha (DIM_DATE) - Derecha
    date_cols = [
        ("date_id", "PK"),
        ("full_date", ""),
        ("year", ""),
        ("month", ""),
        ("day", ""),
        ("day_of_week", "")
    ]
    draw_table(10.3, 2.8, 3.2, 2.8, "dim_date (Dimension)", date_cols, DIM_COLOR)

    # 4. Capa RAW / Staging (RAW_HAPPINESS_EVENTS) - Arriba
    raw_cols = [
        ("raw_id", "PK"),
        ("event_id", ""),
        ("payload (JSONB)", ""),
        ("processing_status", "")
    ]
    draw_table(5.0, 7.0, 4.0, 1.8, "raw_happiness_events (Staging)", raw_cols, RAW_COLOR)

    # Conexiones / Relaciones (Flechas)
    arrow_props = dict(arrowstyle="->", color="#1e293b", lw=2, mutation_scale=15)

    # Conexión dim_country -> fact_predictions
    ax.annotate("", xy=(5.0, 4.4), xytext=(3.7, 4.4), arrowprops=arrow_props)
    ax.text(4.35, 4.6, "1:N\n(country_id)", fontsize=8, weight='bold', color='#0369a1', ha='center')

    # Conexión dim_date -> fact_predictions
    ax.annotate("", xy=(9.0, 4.2), xytext=(10.3, 4.2), arrowprops=arrow_props)
    ax.text(9.65, 4.4, "1:N\n(date_id)", fontsize=8, weight='bold', color='#0369a1', ha='center')

    # Conexión raw_happiness_events -> fact_predictions
    ax.annotate("", xy=(7.0, 6.3), xytext=(7.0, 7.0), arrowprops=arrow_props)
    ax.text(7.4, 6.6, "1:1\n(raw_id)", fontsize=8, weight='bold', color='#0369a1', ha='center')

    # Título Principal
    plt.title("Modelo Dimensional Analítico (Esquema en Estrella) — Step 7", fontsize=14, weight='bold', pad=15, color='#0f172a')

    output_path = "star_schema_diagram.png"
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ ¡Diagrama Estrella generado con éxito! Guardado como: '{output_path}'")

if __name__ == "__main__":
    draw_star_schema()