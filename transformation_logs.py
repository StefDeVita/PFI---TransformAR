"""
Módulo para gestionar logs de transformaciones de documentos en Firestore.

Este módulo permite:
- Crear logs cuando inicia una transformación
- Actualizar el progreso y estado de transformaciones
- Marcar transformaciones como completadas o fallidas
- Obtener historial de transformaciones por usuario
- Mantener límite de logs por usuario

Estructura en Firestore:
users/{userId}/transformation_logs/{logId}
"""

from __future__ import annotations
import os
import uuid
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
from google.cloud import firestore

# Inicializar cliente de Firestore
db = firestore.Client()


async def create_transformation_log(
    user_id: str,
    file_name: str,
    file_type: str,
    template_id: Optional[str] = None,
    template_name: Optional[str] = None,
    total_fields: int = 0
) -> str:
    """
    Crea un nuevo log de transformación cuando inicia el procesamiento.

    Args:
        user_id: ID del usuario que realiza la transformación
        file_name: Nombre del archivo siendo procesado
        file_type: Tipo de archivo (document, pdf, spreadsheet, image, etc.)
        template_id: ID de la plantilla usada (opcional)
        template_name: Nombre de la plantilla usada (opcional)
        total_fields: Número total de campos a extraer (opcional)

    Returns:
        ID del log creado
    """
    try:
        # Generar ID único para el log
        log_id = str(uuid.uuid4())

        # Timestamp actual
        now = datetime.now()
        start_time = now.isoformat()

        # Crear documento de log
        log_data = {
            "id": log_id,
            "fileName": file_name,
            "fileType": file_type,
            "status": "processing",
            "progress": 0,
            "startTime": start_time,
            "extractedFields": 0,
            "totalFields": total_fields,
            "created_at": firestore.SERVER_TIMESTAMP,
            "updated_at": firestore.SERVER_TIMESTAMP
        }

        # Agregar template si está disponible
        if template_id:
            log_data["templateId"] = template_id
        if template_name:
            log_data["template"] = template_name

        # Guardar en Firestore
        doc_ref = db.collection("users").document(user_id).collection("transformation_logs").document(log_id)
        doc_ref.set(log_data)

        print(f"[TransformationLog] Log creado: {log_id} para usuario {user_id}")

        # Limpiar logs antiguos para mantener límite
        await cleanup_old_logs(user_id, max_logs=100)

        return log_id

    except Exception as e:
        print(f"[TransformationLog] Error creando log: {e}")
        raise


async def update_transformation_log(
    user_id: str,
    log_id: str,
    progress: Optional[int] = None,
    extracted_fields: Optional[int] = None,
    status: Optional[str] = None
) -> bool:
    """
    Actualiza el progreso de una transformación en curso.

    Args:
        user_id: ID del usuario
        log_id: ID del log a actualizar
        progress: Progreso actual (0-100)
        extracted_fields: Número de campos extraídos hasta ahora
        status: Nuevo estado (processing, queued, etc.)

    Returns:
        True si se actualizó correctamente
    """
    try:
        doc_ref = db.collection("users").document(user_id).collection("transformation_logs").document(log_id)

        update_data = {
            "updated_at": firestore.SERVER_TIMESTAMP
        }

        if progress is not None:
            update_data["progress"] = progress
        if extracted_fields is not None:
            update_data["extractedFields"] = extracted_fields
        if status is not None:
            update_data["status"] = status

        doc_ref.update(update_data)
        print(f"[TransformationLog] Log actualizado: {log_id}")
        return True

    except Exception as e:
        print(f"[TransformationLog] Error actualizando log {log_id}: {e}")
        return False


async def complete_transformation_log(
    user_id: str,
    log_id: str,
    extracted_fields: int,
    extracted_data: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Marca una transformación como completada exitosamente.

    Args:
        user_id: ID del usuario
        log_id: ID del log
        extracted_fields: Número final de campos extraídos
        extracted_data: Datos extraídos (opcional, para referencia)

    Returns:
        True si se actualizó correctamente
    """
    try:
        doc_ref = db.collection("users").document(user_id).collection("transformation_logs").document(log_id)

        # Obtener datos actuales para calcular duración
        doc = doc_ref.get()
        if not doc.exists:
            print(f"[TransformationLog] Log {log_id} no encontrado")
            return False

        data = doc.to_dict()
        start_time_str = data.get("startTime")

        # Calcular duración
        now = datetime.now()
        end_time = now.isoformat()

        duration = None
        if start_time_str:
            try:
                start_time = datetime.fromisoformat(start_time_str)
                duration_seconds = (now - start_time).total_seconds()

                # Formatear duración como "Xm Ys"
                minutes = int(duration_seconds // 60)
                seconds = int(duration_seconds % 60)

                if minutes > 0:
                    duration = f"{minutes}m {seconds}s"
                else:
                    duration = f"{seconds}s"

            except Exception as e:
                print(f"[TransformationLog] Error calculando duración: {e}")

        update_data = {
            "status": "completed",
            "progress": 100,
            "extractedFields": extracted_fields,
            "endTime": end_time,
            "updated_at": firestore.SERVER_TIMESTAMP
        }

        if duration:
            update_data["duration"] = duration

        if extracted_data:
            update_data["extractedData"] = extracted_data

        doc_ref.update(update_data)
        print(f"[TransformationLog] Transformación completada: {log_id} - Duración: {duration}")
        return True

    except Exception as e:
        print(f"[TransformationLog] Error completando log {log_id}: {e}")
        return False


async def fail_transformation_log(
    user_id: str,
    log_id: str,
    error_message: str
) -> bool:
    """
    Marca una transformación como fallida.

    Args:
        user_id: ID del usuario
        log_id: ID del log
        error_message: Mensaje de error descriptivo

    Returns:
        True si se actualizó correctamente
    """
    try:
        doc_ref = db.collection("users").document(user_id).collection("transformation_logs").document(log_id)

        # Obtener datos actuales para calcular duración
        doc = doc_ref.get()
        if not doc.exists:
            print(f"[TransformationLog] Log {log_id} no encontrado")
            return False

        data = doc.to_dict()
        start_time_str = data.get("startTime")

        # Calcular duración
        now = datetime.now()
        end_time = now.isoformat()

        duration = None
        if start_time_str:
            try:
                start_time = datetime.fromisoformat(start_time_str)
                duration_seconds = (now - start_time).total_seconds()

                minutes = int(duration_seconds // 60)
                seconds = int(duration_seconds % 60)

                if minutes > 0:
                    duration = f"{minutes}m {seconds}s"
                else:
                    duration = f"{seconds}s"

            except Exception as e:
                print(f"[TransformationLog] Error calculando duración: {e}")

        update_data = {
            "status": "failed",
            "errorMessage": error_message,
            "endTime": end_time,
            "updated_at": firestore.SERVER_TIMESTAMP
        }

        if duration:
            update_data["duration"] = duration

        doc_ref.update(update_data)
        print(f"[TransformationLog] Transformación fallida: {log_id} - Error: {error_message}")
        return True

    except Exception as e:
        print(f"[TransformationLog] Error marcando log como fallido {log_id}: {e}")
        return False


async def get_transformation_logs(
    user_id: str,
    limit: int = 50,
    status_filter: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Obtiene el historial de transformaciones de un usuario.

    Args:
        user_id: ID del usuario
        limit: Número máximo de logs a retornar
        status_filter: Filtrar por estado específico (completed, failed, processing, etc.)

    Returns:
        Lista de logs ordenados por fecha (más recientes primero)
    """
    try:
        query = (
            db.collection("users")
            .document(user_id)
            .collection("transformation_logs")
            .order_by("created_at", direction=firestore.Query.DESCENDING)
            .limit(limit)
        )

        # Aplicar filtro de estado si se especifica
        if status_filter:
            query = query.where("status", "==", status_filter)

        docs = query.stream()
        logs = []

        for doc in docs:
            log_data = doc.to_dict()
            logs.append(log_data)

        print(f"[TransformationLog] Obtenidos {len(logs)} logs para usuario {user_id}")
        return logs

    except Exception as e:
        print(f"[TransformationLog] Error obteniendo logs: {e}")
        return []


async def cleanup_old_logs(user_id: str, max_logs: int = 100) -> bool:
    """
    Elimina logs antiguos para mantener un límite por usuario.

    Args:
        user_id: ID del usuario
        max_logs: Número máximo de logs a mantener

    Returns:
        True si se eliminaron logs
    """
    try:
        # Obtener todos los logs ordenados por fecha
        query = (
            db.collection("users")
            .document(user_id)
            .collection("transformation_logs")
            .order_by("created_at", direction=firestore.Query.DESCENDING)
        )

        docs = list(query.stream())

        # Si hay más logs que el límite, eliminar los más antiguos
        if len(docs) > max_logs:
            logs_to_delete = docs[max_logs:]

            batch = db.batch()
            for doc in logs_to_delete:
                batch.delete(doc.reference)

            batch.commit()
            print(f"[TransformationLog] Eliminados {len(logs_to_delete)} logs antiguos para usuario {user_id}")
            return True

        return False

    except Exception as e:
        print(f"[TransformationLog] Error limpiando logs antiguos: {e}")
        return False


async def get_transformation_stats(user_id: str) -> Dict[str, Any]:
    """
    Obtiene estadísticas de transformaciones de un usuario.

    Args:
        user_id: ID del usuario

    Returns:
        Diccionario con estadísticas:
        - total: Total de transformaciones
        - completed: Transformaciones completadas
        - failed: Transformaciones fallidas
        - processing: Transformaciones en proceso
        - successRate: Tasa de éxito (%)
    """
    try:
        query = db.collection("users").document(user_id).collection("transformation_logs")

        all_docs = list(query.stream())

        stats = {
            "total": len(all_docs),
            "completed": 0,
            "failed": 0,
            "processing": 0,
            "queued": 0,
            "successRate": 0
        }

        for doc in all_docs:
            data = doc.to_dict()
            status = data.get("status", "unknown")

            if status == "completed":
                stats["completed"] += 1
            elif status == "failed":
                stats["failed"] += 1
            elif status == "processing":
                stats["processing"] += 1
            elif status == "queued":
                stats["queued"] += 1

        # Calcular tasa de éxito
        total_finished = stats["completed"] + stats["failed"]
        if total_finished > 0:
            stats["successRate"] = round((stats["completed"] / total_finished) * 100)

        return stats

    except Exception as e:
        print(f"[TransformationLog] Error obteniendo estadísticas: {e}")
        return {
            "total": 0,
            "completed": 0,
            "failed": 0,
            "processing": 0,
            "queued": 0,
            "successRate": 0
        }
