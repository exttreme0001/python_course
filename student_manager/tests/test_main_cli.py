# tests/test_main_cli.py
import pytest
from unittest.mock import patch
from lab.main import main_cli
from lab.models import Student

def test_cli_show_students(monkeypatch, capsys, sample_students):
    """Тестирует базовый сценарий: загрузка и отображение студентов."""

    # Сценарий ввода:
    # '1' -> пункт "Загрузить"
    # 'data/test.csv' -> путь к файлу (имя не важно, т.к. мы мокаем чтение)
    # '3' -> пункт "Показать всех"
    # '0' -> пункт "Выход"
    input_sequence = iter(['1', 'data/test.csv', '3', '0'])

    def mock_input(prompt=""):
        """Имитация пользовательского ввода."""
        try:
            return next(input_sequence)
        except StopIteration:
            # Если ввод закончился раньше, чем программа завершилась,
            # возвращаем '0' (выход), чтобы не зависнуть.
            return "0"

    # Подменяем встроенную функцию input
    monkeypatch.setattr('builtins.input', mock_input)

    # Патчим функцию чтения в lab.io_utils.
    # return_value=sample_students заставит программу "думать",
    # что она прочитала список студентов из файла.
    with patch('lab.io_utils.read_students_from_csv', return_value=sample_students) as mock_read:
        main_cli()

        # Проверка: функция чтения действительно вызвалась?
        mock_read.assert_called_once()

    # Перехватываем все, что программа вывела в консоль
    captured = capsys.readouterr()
    output = captured.out

    # --- Проверки ---

    # 1. Сообщение об успешной загрузке (len(sample_students) == 3)
    assert "Успешно загружено 3 студентов" in output, "Не найдено сообщение об успешной загрузке"

    # 2. Проверка отображения имен студентов (пункт меню 3)
    # Ищем имена из тестовых данных в выводе консоли
    assert "Иванов Иван" in output
    assert "Сидорова Анна" in output
    assert "Петров Петр" in output

    # 3. Проверка, что программа корректно попрощалась
    assert "До свидания!" in output
