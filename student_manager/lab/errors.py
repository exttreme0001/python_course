# lab/errors.py
"""Модуль для определения пользовательских исключений приложения."""

class StudentAppError(Exception):
    """Базовый класс для всех исключений в этом приложении."""
    pass

class DataValidationError(StudentAppError):
    """Исключение, связанное с некорректными данными (в файле или вводе)."""
    pass

class FileProcessingError(StudentAppError):
    """Исключение, связанное с ошибками файловых операций."""
    pass

class StudentNotFoundError(StudentAppError):
    """Исключение, когда студент с заданным ID не найден."""
    pass

class DuplicateStudentIdError(StudentAppError):
    """Исключение при попытке добавить студента с уже существующим ID."""
    pass
