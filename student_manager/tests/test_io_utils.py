# tests/test_io_utils.py
from lab.io_utils import write_students_to_csv, read_students_from_csv

def test_csv_roundtrip(sample_students, tmp_path):
    """Тестирует полный цикл: запись в CSV и чтение обратно."""
    filepath = tmp_path / "test.csv"

    # 1. Записать данные в файл
    write_students_to_csv(filepath, sample_students)

    # 2. Прочитать данные из файла
    read_students = read_students_from_csv(filepath)

    # 3. Сравнить исходные и прочитанные данные
    assert len(read_students) == len(sample_students)

    # Сортируем оба списка для стабильного сравнения
    sample_students.sort(key=lambda s: s.id)
    read_students.sort(key=lambda s: s.id)

    for original, read in zip(sample_students, read_students):
        assert original.id == read.id
        assert original.name == read.name
        assert original.grades == read.grades
