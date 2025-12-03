# lab/io_utils.py
"""Модуль для операций ввода/вывода, в основном для работы с CSV файлами."""
import csv
from typing import List

try:
    # Сначала относительный (для pytest)
    from .models import Student
    from .errors import FileProcessingError, DataValidationError
except (ImportError, ValueError):
    # Затем прямой (для EXE)
    from models import Student
    from errors import FileProcessingError, DataValidationError
# -------------------------

def read_students_from_csv(filepath: str) -> List[Student]:
    """Читает данные о студентах из CSV-файла."""
    students = []
    try:
        with open(filepath, mode='r', encoding='utf-8', newline='') as file:
            reader = csv.reader(file)

            try:
                first_row = next(reader)
            except StopIteration:
                return [] # Пустой файл

            has_header = False
            if first_row and 'id' in first_row[0].lower():
                has_header = True

            if not has_header:
                process_row(first_row, 1, students)

            for i, row in enumerate(reader, start=2 if has_header else 2):
                process_row(row, i, students)

    except FileNotFoundError:
        raise FileProcessingError(f"Файл не найден по пути: {filepath}")
    except Exception as e:
        raise FileProcessingError(f"Не удалось прочитать файл {filepath}: {e}")

    return students

def process_row(row: List[str], line_num: int, students: List[Student]):
    """Обрабатывает одну строку из CSV и добавляет студента в список."""
    if not row or not row[0].strip():
        return

    try:
        student_id = int(row[0])
        name = row[1]
        grades = [int(grade) for grade in row[2:] if grade.strip()]
        students.append(Student(student_id, name, grades))
    except (ValueError, IndexError) as e:
        raise DataValidationError(f"Ошибка в строке {line_num}: {row}. Детали: {e}")

def write_students_to_csv(filepath: str, students: List[Student]):
    """Записывает данные о студентах в CSV-файл с выравниванием колонок."""
    try:
        with open(filepath, mode='w', encoding='utf-8', newline='') as file:
            writer = csv.writer(file)

            max_grades = max((len(s.grades) for s in students), default=0)
            header = ['id', 'name'] + [f'grade{i+1}' for i in range(max_grades)]
            writer.writerow(header)

            for s in students:
                row = [s.id, s.name] + s.grades
                row.extend([''] * (max_grades - len(s.grades)))
                writer.writerow(row)
    except IOError as e:
        raise FileProcessingError(f"Ошибка записи в файл {filepath}: {e}")

def export_top_n_to_csv(filepath: str, students: List[Student]):
    """Экспортирует ТОП-N студентов в отдельный CSV-файл."""
    try:
        with open(filepath, mode='w', encoding='utf-8', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['id', 'name', 'average', 'grades'])

            for s in students:
                grades_str = " ".join(map(str, s.grades))
                writer.writerow([s.id, s.name, f"{s.average:.2f}", grades_str])
    except IOError as e:
        raise FileProcessingError(f"Ошибка экспорта в файл {filepath}: {e}")
