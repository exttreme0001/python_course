import pytest
# ВАЖНО: Импортируем StudentNotFoundError из lab.processing,
# чтобы класс ошибки гарантированно совпадал с тем, который выбрасывает функция.
from lab.processing import sort_students, get_group_statistics, remove_student_by_id, StudentNotFoundError

def test_sort_students_by_id(sample_students):
    sorted_list = sort_students(sample_students, 'id')
    assert [s.id for s in sorted_list] == [1, 2, 3]

def test_sort_students_by_name(sample_students):
    sorted_list = sort_students(sample_students, 'name')
    assert [s.name for s in sorted_list] == ["Иванов Иван", "Петров Петр", "Сидорова Анна"]

def test_sort_students_by_avg(sample_students):
    sorted_list = sort_students(sample_students, 'avg')
    assert [s.id for s in sorted_list] == [3, 1, 2] # 91.6, 84.3, 67.5

def test_get_group_statistics(sample_students):
    stats = get_group_statistics(sample_students)
    assert stats["total_students"] == 3
    assert stats["best_student"].id == 3
    assert stats["worst_student"].id == 2
    assert pytest.approx(stats["overall_average"], 0.01) == 83.12

def test_remove_student_not_found(sample_students):
    # Тест ожидает, что будет выброшено исключение StudentNotFoundError
    with pytest.raises(StudentNotFoundError):
        remove_student_by_id(sample_students, 999)
