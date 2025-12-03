# lab/processing.py
"""Модуль для обработки данных: сортировка, статистика, управление студентами."""
from typing import List, Dict, Any, Optional

try:
    # 1. Относительный импорт (для pytest)
    from .models import Student
    from .errors import StudentNotFoundError, DuplicateStudentIdError
except (ImportError, ValueError):
    # 2. Прямой импорт (для EXE)
    from models import Student
    from errors import StudentNotFoundError, DuplicateStudentIdError
# --------------------------------------------------

def add_student(students: List[Student], student_id: int, name: str, grades: List[int]) -> List[Student]:
    """Добавляет нового студента в список, проверяя уникальность ID."""
    if any(s.id == student_id for s in students):
        raise DuplicateStudentIdError(f"Студент с ID {student_id} уже существует.")

    # Здесь вызовется __init__ класса Student, и если оценка < 0,
    # вылетит ValueError, который поймается в main.py
    new_student = Student(student_id, name, grades)
    students.append(new_student)
    return students

def remove_student_by_id(students: List[Student], student_id: int) -> List[Student]:
    """Удаляет студента из списка по его ID."""
    student_to_remove = next((s for s in students if s.id == student_id), None)
    if not student_to_remove:
        raise StudentNotFoundError(f"Студент с ID {student_id} не найден.")

    students.remove(student_to_remove)
    return students

def update_student_grades(students: List[Student], student_id: int, new_grades: List[int]) -> Student:
    """Обновляет оценки существующего студента."""
    student_to_update = next((s for s in students if s.id == student_id), None)
    if not student_to_update:
        raise StudentNotFoundError(f"Студент с ID {student_id} не найден.")

    # Валидация новых оценок произойдет здесь вручную, так как мы меняем поле напрямую,
    # но лучше пересоздать список через валидацию
    for grade in new_grades:
        if not isinstance(grade, int) or grade < 0 or grade > 100:
             raise ValueError(f"Оценка {grade} недопустима. Разрешен диапазон 0-100.")

    student_to_update.grades = new_grades
    return student_to_update

def sort_students(students: List[Student], by: str) -> List[Student]:
    """Сортирует список студентов по заданному критерию."""
    if by == 'id':
        return sorted(students, key=lambda s: s.id)
    elif by == 'name':
        return sorted(students, key=lambda s: s.name)
    elif by == 'avg':
        # Сортировка по убыванию среднего балла, затем по имени для стабильности
        return sorted(students, key=lambda s: (-s.average, s.name))
    else:
        raise ValueError("Неверный ключ для сортировки. Доступно: 'id', 'name', 'avg'.")

def get_group_statistics(students: List[Student]) -> Optional[Dict[str, Any]]:
    """Рассчитывает статистику по группе студентов."""
    if not students:
        return None

    total_students = len(students)
    all_grades = [grade for s in students for grade in s.grades]

    overall_avg = sum(all_grades) / len(all_grades) if all_grades else 0.0

    best_student = max(students, key=lambda s: s.average)
    worst_student = min(students, key=lambda s: s.average)

    return {
        "total_students": total_students,
        "overall_average": overall_avg,
        "best_student": best_student,
        "worst_student": worst_student,
    }

def get_top_n_students(students: List[Student], n: int) -> List[Student]:
    """Возвращает N лучших студентов по среднему баллу."""
    sorted_by_avg = sort_students(students, 'avg')
    return sorted_by_avg[:n]
