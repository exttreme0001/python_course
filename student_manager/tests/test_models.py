# tests/test_models.py
from lab.models import Student

def test_student_creation():
    s = Student(1, "Тестов Тест", [80, 90])
    assert s.id == 1
    assert s.name == "Тестов Тест"
    assert s.grades == [80, 90]

def test_student_average():
    s1 = Student(1, "С оценками", [70, 80, 90])
    assert s1.average == 80.0

    s2 = Student(2, "Без оценок", [])
    assert s2.average == 0.0

def test_student_str_representation(capsys):
    s = Student(5, "Анна Котова", [100, 95])
    print(s)
    captured = capsys.readouterr()
    assert "ID: 5" in captured.out
    assert "Анна Котова" in captured.out
    assert "97.50" in captured.out
    assert "[100, 95]" in captured.out
