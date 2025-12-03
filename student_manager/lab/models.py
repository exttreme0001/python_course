# lab/models.py
"""Модуль, определяющий основные модели данных, такие как Student."""
from typing import List

class Student:
    """Представляет студента с его ID, именем и оценками."""
    def __init__(self, student_id: int, name: str, grades: List[int]):
        if not isinstance(student_id, int) or student_id <= 0:
            raise ValueError("ID студента должен быть положительным целым числом.")
        if not name or not isinstance(name, str) or not name.strip():
            raise ValueError("Имя студента не может быть пустым.")

        # Мы не фильтруем оценки, а проверяем их. Если хоть одна плохая - ошибка.
        self.grades = []
        for grade in grades:
            if not isinstance(grade, int):
                raise ValueError(f"Оценка '{grade}' должна быть целым числом.")
            if grade < 0 or grade > 100:
                raise ValueError(f"Оценка {grade} недопустима. Разрешен диапазон 0-100.")
            self.grades.append(grade)
        # ---------------------------

        self.id = student_id
        self.name = name

    @property
    def average(self) -> float:
        """Рассчитывает средний балл студента. Возвращает 0.0, если оценок нет."""
        if not self.grades:
            return 0.0
        return sum(self.grades) / len(self.grades)

    def __repr__(self) -> str:
        """Возвращает строковое представление объекта для отладки."""
        return f"Student(id={self.id}, name='{self.name}', average={self.average:.2f})"

    def __str__(self) -> str:
        """Возвращает удобное для пользователя строковое представление объекта."""
        grades_str = ", ".join(map(str, self.grades)) if self.grades else "Нет оценок"
        return f"ID: {self.id:<3} | Имя: {self.name:<20} | Средний балл: {self.average:<6.2f} | Оценки: [{grades_str}]"
