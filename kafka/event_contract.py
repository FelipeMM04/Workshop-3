from datetime import datetime

def validate_event_contract(event: dict) -> tuple[bool, str]:
    """
    Valida que un evento recibido desde Apache Kafka cumpla estrictamente
    con el esquema y las reglas del Event Contract.

    Retorna:
        (True, "VALID") si cumple todas las reglas.
        (False, "motivo_del_error") si falla alguna validación.
    """
    # 1. Verificar que el evento sea un diccionario/JSON válido
    if not isinstance(event, dict):
        return False, "INVALID_FORMAT: El evento no es un objeto JSON valido."

    # 2. Requeridos: event_id (Cadena no vacía)
    event_id = event.get("event_id")
    if not event_id or not isinstance(event_id, str) or not str(event_id).strip():
        return False, "INVALID_SCHEMA: 'event_id' debe ser una cadena no vacia."

    # 3. Requeridos: country (Cadena no vacía)
    country = event.get("country")
    if not country or not isinstance(country, str) or not str(country).strip():
        return False, "INVALID_SCHEMA: 'country' debe ser una cadena no vacia."

    # 4. Requeridos: year (Entero)
    year = event.get("year")
    if year is None or not isinstance(year, int):
        return False, "INVALID_TYPES: 'year' debe ser un numero entero."

    # 5. Requeridos: Features numéricas de ML
    features = ["gdp", "family", "health", "freedom", "generosity", "corruption"]
    for feature in features:
        val = event.get(feature)
        if val is None or not isinstance(val, (int, float)):
            return False, f"INVALID_TYPES: La feature '{feature}' debe ser un valor numerico."

    # 6. Requerido: actual_happiness_score (Numérico para evaluación)
    actual_score = event.get("actual_happiness_score")
    if actual_score is None or not isinstance(actual_score, (int, float)):
        return False, "INVALID_TYPES: 'actual_happiness_score' debe ser un valor numerico."

    # 7. Requerido: event_time (Timestamp ISO-8601 válido)
    event_time = event.get("event_time")
    if not event_time or not isinstance(event_time, str):
        return False, "INVALID_SCHEMA: 'event_time' debe ser un texto en formato ISO-8601."
    
    try:
        datetime.fromisoformat(event_time)
    except ValueError:
        return False, "INVALID_FORMAT: 'event_time' no cumple con la estructura ISO-8601 (YYYY-MM-DDTHH:MM:SS)."

    # Si pasa todas las validaciones
    return True, "VALID"