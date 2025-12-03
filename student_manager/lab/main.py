# lab/main.py
"""Главный модуль, реализующий консольный интерфейс (CLI) для управления студентами."""
import sys
import os
import traceback
from typing import List

if getattr(sys, 'frozen', False):
    base_path = sys._MEIPASS
else:
    base_path = os.path.dirname(os.path.abspath(__file__))

if base_path not in sys.path:
    sys.path.append(base_path)

try:
    # 1. Попытка относительного импорта (Для pytest и запуска через python -m lab.main)
    from . import io_utils, processing, errors
    from .models import Student
except (ImportError, ValueError):
    # 2. Попытка прямого импорта (Для EXE и запуска через python lab/main.py)
    import io_utils
    import processing
    import errors
    from models import Student
# -------------------------

students_data: List[Student] = []

def print_menu():
    """Выводит на экран главное меню."""
    print("\n" + "="*30)
    print("      МЕНЮ УПРАВЛЕНИЯ")
    print("="*30)
    print("1. Загрузить студентов из CSV")
    print("2. Сохранить студентов в CSV")
    print("3. Показать всех студентов")
    print("4. Добавить нового студента")
    print("5. Удалить студента по ID")
    print("6. Обновить оценки студента")
    print("7. Показать статистику по группе")
    print("8. Экспорт ТОП-N студентов")
    print("9. Сортировать и показать список")
    print("0. Выход")
    print("="*30)

def main_cli():
    """Основной цикл консольного приложения."""
    global students_data

    while True:
        print_menu()
        choice = input("Выберите пункт меню: ")

        try:
            if choice == '1':
                filepath = input("Введите путь к файлу для загрузки (e.g., data/students.csv): ")
                filepath = filepath.strip('"').strip("'")
                students_data = io_utils.read_students_from_csv(filepath)
                print(f"✅ Успешно загружено {len(students_data)} студентов.")

            elif choice == '2':
                if not students_data:
                    print("⚠️ Список студентов пуст. Нечего сохранять.")
                    continue
                filepath = input("Введите путь к файлу для сохранения: ")
                filepath = filepath.strip('"').strip("'")
                io_utils.write_students_to_csv(filepath, students_data)
                print(f"✅ Данные успешно сохранены в {filepath}.")

            elif choice == '3':
                if not students_data:
                    print("ℹ️ Список студентов пуст.")
                else:
                    print("\n--- Список всех студентов ---")
                    for s in students_data:
                        print(s)

            elif choice == '4':
                try:
                    stud_id = int(input("Введите ID нового студента: "))
                    name = input("Введите ФИО студента: ")
                    grades_str = input("Введите оценки через пробел: ")
                    grades = [int(g) for g in grades_str.split()] if grades_str else []
                    students_data = processing.add_student(students_data, stud_id, name, grades)
                    print(f"✅ Студент {name} успешно добавлен.")
                except ValueError as e:
                    print(f"❌ Ошибка данных: {e}")

            elif choice == '5':
                try:
                    stud_id = int(input("Введите ID студента для удаления: "))
                    students_data = processing.remove_student_by_id(students_data, stud_id)
                    print(f"✅ Студент с ID {stud_id} успешно удален.")
                except ValueError:
                    print("❌ Ошибка ввода: ID должен быть числом.")

            elif choice == '6':
                try:
                    stud_id = int(input("Введите ID студента для обновления оценок: "))
                    grades_str = input("Введите новые оценки через пробел: ")
                    grades = [int(g) for g in grades_str.split()] if grades_str else []
                    student = processing.update_student_grades(students_data, stud_id, grades)
                    print(f"✅ Оценки студента {student.name} обновлены.")
                except ValueError as e:
                    print(f"❌ Ошибка данных: {e}")

            elif choice == '7':
                stats = processing.get_group_statistics(students_data)
                if not stats:
                    print("ℹ️ Список студентов пуст, статистика недоступна.")
                else:
                    print("\n--- Статистика по группе ---")
                    print(f"Всего студентов: {stats['total_students']}")
                    print(f"Общий средний балл: {stats['overall_average']:.2f}")
                    print(f"Лучший студент: {stats['best_student'].name} (ср. балл: {stats['best_student'].average:.2f})")
                    print(f"Худший студент: {stats['worst_student'].name} (ср. балл: {stats['worst_student'].average:.2f})")

            elif choice == '8':
                try:
                    n = int(input("Введите количество студентов для экспорта (ТОП-N): "))
                    filepath = input("Введите путь к файлу для экспорта: ")
                    filepath = filepath.strip('"').strip("'")
                    top_students = processing.get_top_n_students(students_data, n)
                    io_utils.export_top_n_to_csv(filepath, top_students)
                    print(f"✅ ТОП-{n} студентов экспортирован в {filepath}.")
                except ValueError as e:
                    print(f"❌ Ошибка ввода: {e}")

            elif choice == '9':
                sort_key = input("Введите ключ сортировки (id, name, avg): ").lower()
                try:
                    sorted_list = processing.sort_students(students_data, sort_key)
                    print(f"\n--- Студенты, отсортированные по '{sort_key}' ---")
                    for s in sorted_list:
                        print(s)
                except ValueError as ve:
                    print(f"❌ Ошибка сортировки: {ve}")

            elif choice == '0':
                print("👋 До свидания!")
                break

            else:
                print("❌ Неверный выбор. Пожалуйста, введите число от 0 до 9.")

        except errors.StudentAppError as e:
            print(f"❌ Ошибка логики: {e}")
        except Exception as e:
            print(f"❌ Произошла непредвиденная ошибка: {e}")

if __name__ == '__main__':
    try:
        main_cli()
    except KeyboardInterrupt:
        print("\nПрограмма принудительно остановлена.")
    except Exception:
        print("\n!!! КРИТИЧЕСКАЯ ОШИБКА ЗАПУСКА !!!")
        traceback.print_exc()
    finally:
        input("\nНажмите Enter, чтобы выйти...")
