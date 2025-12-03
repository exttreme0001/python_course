# tests/conftest.py
import pytest
from typing import List
from lab.models import Student

@pytest.fixture
def sample_students() -> List[Student]:
    """Фикстура, предоставляющая тестовый набор студентов."""
    return [
        Student(1, "Иванов Иван", [78, 85, 90]),
        Student(3, "Петров Петр", [92, 88, 95]),
        Student(2, "Сидорова Анна", [65, 70]),
    ]
