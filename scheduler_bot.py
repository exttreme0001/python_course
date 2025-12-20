import asyncio
import logging
import pandas as pd
import io
import requests
import re
import warnings
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, StateFilter
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

# Игнорируем предупреждения pandas о форматах
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- КОНФИГУРАЦИЯ ---
BOT_TOKEN = "8382626077:AAG44Shz2g3DDEM8c2iahBX0eUiiyQvr_IY"

# Хранилище учебных планов (аналог COURSES)
ACADEMIC_DATA = {
    "edu_1": {
        "label": "Факультет ФПМИ (3 курс)",
        "sheet_id": "14-YxxIaNrIohX5QwtQRgPARvj0LbMHLQ",
        "gid": "1243294014"
    }
}

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Глобальный кэш данных
LOCAL_STORAGE = {}

# Маппинг временных интервалов
WEEK_DAYS = {
    "mon": "Понедельник", "tue": "Вторник", "wed": "Среда",
    "thu": "Четверг", "fri": "Пятница", "sat": "Суббота","sun": "Воскресенье"
}
SEARCH_DAYS_LOW = {k: v.lower() for k, v in WEEK_DAYS.items()}

# Храним: user_id -> {'hid': ..., 'fid': ..., 'gnum': ..., 'col': ...}
USER_PREFS = {}

# --- СОСТОЯНИЯ ---
class FormStates(StatesGroup):
    input_lecturer_name = State()
    input_room_number = State()
    input_hub_title = State()
    input_hub_link = State()
    waiting_teacher_track = State()

# --- ЛОГИКА ОБРАБОТКИ ТЕКСТА ---
def scrub_content(raw_val):
    """Базовая очистка ячейки"""
    if not raw_val or str(raw_val).lower() == "nan": return ""
    text = str(raw_val).strip()
    # Убираем даты и лишние приписки
    text = re.sub(r'\d{2}\.\d{2}', '', text).strip()
    garbage = ("по ", "с ", "занятия", "кураторский", "в т.ч.")
    for word in garbage:
        if text.lower().startswith(word): return ""
    return text

def extract_full_data(cell_text):
    """Разбивает ячейку на Предмет, Преподавателя и Кабинет"""
    clean_t = scrub_content(cell_text)
    if not clean_t: return {"s": "", "t": "", "r": ""}

    # Регулярка для кабинета: строго 2-4 цифры + возможная буква
    room_rx = re.compile(r'\b\d{2,4}[а-яА-Я]?\b')
    lines = [l.strip() for l in str(cell_text).split('\n') if l.strip()]

    res = {"s": "", "t": "", "r": ""}
    for line in lines:
        line_clean = scrub_content(line)
        if not line_clean: continue

        # 1. Если это кабинет (короткое число)
        rm = room_rx.search(line_clean)
        # Кабинет обычно очень короткий. Кафедры типа "ФМО" или "ФПМИ" длиннее или не содержат цифр.
        if rm and len(line_clean) <= 6:
            res["r"] = rm.group()
        # 2. Если это препод (есть слова Доцент, Профессор или И.О.)
        elif any(rank in line_clean.lower() for rank in ["доцент", "проф", "преп", "ассист"]) or re.search(r'[А-Я]\.[А-Я]\.', line_clean):
            res["t"] = line_clean
        else:
            # 3. Предмет (если это не техническая пометка кафедры типа "МСС", "ТП")
            if len(line_clean) > 2:
                res["s"] = (res["s"] + " " + line_clean).strip()
    return res
def validate_subject(content):
    """Отличаем название предмета от аудитории"""
    if not content or not any(c.isalpha() for c in content):
        return False
    # Короткие строки с цифрами обычно аудитории (напр. "402 ГК")
    if any(c.isdigit() for c in content) and len(content) < 10:
        return False
    return True

# --- ЯДРО ПАРСЕРА ---
def map_sheet_layout(df):
    """
    Сканирует таблицу, находит потоки, группы и подгруппы.
    Использует ручное заполнение (manual ffill) для корректной работы
    с объединенными ячейками после df.fillna("").
    """
    # 1. Ищем строку с группами
    group_row = -1
    for r in range(30):
        row_str = [str(cell).lower() for cell in df.iloc[r].values]
        if "1 группа" in row_str or ("1" in row_str and "группа" in row_str):
            group_row = r
            break

    if group_row == -1: return {}

    # 2. Ищем строку потоков (выше групп)
    flow_row = -1
    for r in range(group_row - 1, -1, -1):
        line = " ".join([str(x).lower() for x in df.iloc[r].values])
        if "поток" in line:
            flow_row = r
            break
    if flow_row == -1: flow_row = max(0, group_row - 3)

    sub_header = group_row - 1
    layout = {}
    flow_tracker = {}

    # --- ИСПРАВЛЕНИЕ: РУЧНОЕ ЗАПОЛНЕНИЕ ОБЪЕДИНЕННЫХ ЯЧЕЕК ---
    # Получаем строку групп как список строк
    raw_groups = [str(val).strip() for val in df.iloc[group_row].values]
    filled_groups = []
    last_valid = ""

    for val in raw_groups:
        # Если ячейка не пустая — запоминаем её как актуальную
        if val:
            last_valid = val
        # Записываем либо текущее значение, либо последнее запомненное
        filled_groups.append(last_valid)
    # ---------------------------------------------------------

    # 3. Проходим по колонкам
    for c in range(2, len(df.columns)):
        f_val = str(df.iloc[flow_row, c]).strip()

        # БЕРЕМ ЗНАЧЕНИЕ ИЗ НАШЕГО СПИСКА (c - индекс колонки)
        if c < len(filled_groups):
            g_val = filled_groups[c]
        else:
            g_val = ""

        s_val = str(df.iloc[sub_header, c]).strip()

        # Определяем имя потока
        current_flow = "Общий поток"
        if f_val and f_val.lower() != "nan":
            current_flow = f_val.replace("\n", " ")

        # Ищем номер группы
        g_match = re.search(r"(\d+)", g_val)
        if not g_match: continue
        g_num = int(g_match.group(1))

        # Формируем имя подгруппы (кафедры)
        sub_name = s_val.replace("\n", " ").strip()
        if not sub_name or sub_name.lower() == "nan" or sub_name == current_flow:
            col_label = "Общая"
        else:
            col_label = sub_name

        # Регистрируем поток
        if current_flow not in flow_tracker:
            fid = f"f_{len(layout)}"
            flow_tracker[current_flow] = fid
            layout[fid] = {
                "title": current_flow,
                "anchor_col": c,
                "map": {},
                "labels": {}
            }

        fid = flow_tracker[current_flow]

        if g_num not in layout[fid]["map"]:
            layout[fid]["map"][g_num] = {}

        # Проверка дубликатов (КТС, КТС -> КТС, КТС (2))
        original_label = col_label
        counter = 2
        while col_label in layout[fid]["map"][g_num]:
            col_label = f"{original_label} ({counter})"
            counter += 1

        layout[fid]["map"][g_num][col_label] = c
        layout[fid]["labels"][c] = f"Гр. {g_num} ({col_label})"

    return layout

def sync_data(hub_id):
    """Загрузка и кэширование данных из Google Sheets"""
    if hub_id in LOCAL_STORAGE:
        return LOCAL_STORAGE[hub_id]["df"], LOCAL_STORAGE[hub_id]["layout"]

    conf = ACADEMIC_DATA.get(hub_id)
    if not conf: return None, None

    path = f"https://docs.google.com/spreadsheets/d/{conf['sheet_id']}/export?format=xlsx&gid={conf['gid']}"

    try:
        resp = requests.get(path, timeout=12)
        df = pd.read_excel(io.BytesIO(resp.content), header=None)

        # Предварительная обработка (заполнение пустот)
        df.iloc[:15] = df.iloc[:15].ffill(axis=1)
        df[0] = df[0].ffill()
        df[1] = df[1].ffill(limit=2)
        df = df.fillna("")

        struct = map_sheet_layout(df)
        LOCAL_STORAGE[hub_id] = {"df": df, "layout": struct}
        return df, struct
    except Exception as e:
        logging.error(f"Sync error: {e}")
        return None, None

# --- ИНТЕРФЕЙС (КЛАВИАТУРЫ) ---
def ui_main_menu():
    kb = []
    for hid, info in ACADEMIC_DATA.items():
        kb.append([InlineKeyboardButton(text=f"📘 {info['label']}", callback_data=f"hub:{hid}")])
    kb.append([InlineKeyboardButton(text="🔍 Поиск преподавателя", callback_data="find_proff")])
    kb.append([InlineKeyboardButton(text="🏢 Поиск по аудитории", callback_data="find_room")])
    kb.append([InlineKeyboardButton(text="⚡️ Что сейчас идет?", callback_data="near_event")])
    kb.append([InlineKeyboardButton(text="📅 Расписание на СЕГОДНЯ", callback_data="today_sch")])
    kb.append([InlineKeyboardButton(text="⏩ Расписание на ЗАВТРА", callback_data="tomorrow_sch")])
    kb.append([InlineKeyboardButton(text="🟢 Свободные кабинеты", callback_data="free_rooms")])
    kb.append([InlineKeyboardButton(text="📍 Где препод сейчас?", callback_data="track_teacher_now")])
    kb.append([InlineKeyboardButton(text="⚙️ Настроить расписание", callback_data="setup_hub")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def ui_post_control(mode="general"):
    """
    Генерирует кнопки навигации в зависимости от контекста.
    mode: "proff", "room", "track", "free", "general"
    """
    kb = [[InlineKeyboardButton(text="🏠 В меню", callback_data="home")]]

    if mode == "room":
        kb[0].append(InlineKeyboardButton(text="🔎 Другая аудитория", callback_data="find_room"))
    elif mode == "proff":
        kb[0].append(InlineKeyboardButton(text="🔎 Другой преподаватель", callback_data="find_proff"))
    elif mode == "track":
        kb[0].append(InlineKeyboardButton(text="🔎 Искать другого", callback_data="track_teacher_now"))
    elif mode == "free":
        kb[0].append(InlineKeyboardButton(text="🔄 Обновить", callback_data="free_rooms"))
    else:
        # Для обычного расписания (студента) оставляем поиск препода как самую частую функцию
        kb[0].append(InlineKeyboardButton(text="🔎 Поиск препода", callback_data="find_proff"))

    return InlineKeyboardMarkup(inline_keyboard=kb)
def ui_flow_select(hid, struct):
    kb = []
    for fid in sorted(struct.keys()):
        t = struct[fid]["title"][:25]
        kb.append([InlineKeyboardButton(text=f"📍 {t}", callback_data=f"flow:{hid}:{fid}")])
    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="home")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def ui_cluster_select(hid, fid, struct):
    nums = sorted(struct[fid]["map"].keys())

    kb, row = [], []
    for n in nums:
        row.append(InlineKeyboardButton(text=f"Группа {n}", callback_data=f"cls:{hid}:{fid}:{n}"))
        if len(row) == 2:
            kb.append(row); row = []
    if row: kb.append(row)
    kb.append([InlineKeyboardButton(text="⬅️ К потокам", callback_data=f"hub:{hid}")])
    return InlineKeyboardMarkup(inline_keyboard=kb)
def ui_day_select(hid, fid, cluster, col):
    kb, row = [], []
    for code, name in WEEK_DAYS.items():
        row.append(InlineKeyboardButton(text=name[:3], callback_data=f"get:{code}:{hid}:{fid}:{cluster}:{col}"))
        if len(row) == 3:
            kb.append(row); row = []
    kb.append([InlineKeyboardButton(text="🗓 Вся неделя", callback_data=f"get:all:{hid}:{fid}:{cluster}:{col}")])
    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"flow:{hid}:{fid}")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def parse_time_range(time_str):
    """Превращает '10:45 - 12:20' в объекты time"""
    try:
        times = re.findall(r"(\d{1,2}[:.]\d{2})", time_str)
        if len(times) >= 2:
            start = datetime.strptime(times[0].replace('.', ':'), "%H:%M").time()
            end = datetime.strptime(times[1].replace('.', ':'), "%H:%M").time()
            return start, end
    except:
        pass
    return None, None

async def render_schedule_output(message: Message, day_code: str, hid: str, fid: str, gnum: str, col: str):
    if hid not in LOCAL_STORAGE:
        await asyncio.get_event_loop().run_in_executor(None, sync_data, hid)

    df, layout = LOCAL_STORAGE[hid]["df"], LOCAL_STORAGE[hid]["layout"]

    # Карты колонок
    group_columns_map = layout[fid]["map"][int(gnum)]
    anchor_col = layout[fid]["anchor_col"]

    # Собираем все колонки потока
    all_flow_columns = []
    for grp_data in layout[fid]["map"].values():
        all_flow_columns.extend(grp_data.values())

    if day_code not in SEARCH_DAYS_LOW:
        await message.answer("🗓 <b>Сегодня воскресенье!</b>\nЗанятий нет, отдыхайте.", parse_mode="HTML", reply_markup=ui_post_control())
        return

    day_query = SEARCH_DAYS_LOW if day_code == "all" else {day_code: SEARCH_DAYS_LOW[day_code]}
    output = [f"🏛 <b>ГРУППА {gnum}</b>\n"]
    has_data = False

    # Структура daily_data теперь хранит сырые наборы данных (sets), а не готовые строки
    # daily_data[day][time] = { "is_flow": False, "groups": { "МСС": {s:set, t:set, r:set}, ... } }
    daily_data = {}

    for idx, row in df.iterrows():
        if idx < 15: continue

        # --- 1. День и Время ---
        d_val = str(row[0]).strip().lower()
        target_day_name = ""
        for k, v in day_query.items():
            if v in d_val:
                target_day_name = v.upper()
                break
        if not target_day_name: continue

        time_str = str(row[1]).replace("\n", " ").strip()
        if not re.search(r'\d{1,2}[:.]\d{2}', time_str): continue

        if target_day_name not in daily_data: daily_data[target_day_name] = {}
        if time_str not in daily_data[target_day_name]:
            daily_data[target_day_name][time_str] = {"groups": {}, "is_flow": False}

        slot = daily_data[target_day_name][time_str]

        # --- 2. Проверка ПОТОКА ---
        cell_flow = row[anchor_col]
        flow_text = scrub_content(cell_flow)
        data_flow = extract_full_data(cell_flow)

        is_global_flow = False
        if data_flow["s"] or data_flow["t"]:
            is_conflict = False
            for check_col in all_flow_columns:
                if check_col == anchor_col: continue
                other_text = scrub_content(row[check_col])
                if other_text and other_text != flow_text:
                    is_conflict = True
                    break
            if not is_conflict:
                is_global_flow = True

        # --- 3. Сбор данных в словарь (Агрегация) ---

        if is_global_flow:
            slot["is_flow"] = True
            # Для потока используем пустой ключ ""
            if "" not in slot["groups"]: slot["groups"][""] = {"s": set(), "t": set(), "r": set()}

            if data_flow["s"]: slot["groups"][""]["s"].add(data_flow["s"])
            if data_flow["t"]: slot["groups"][""]["t"].add(data_flow["t"])
            if data_flow["r"]: slot["groups"][""]["r"].add(data_flow["r"])
        else:
            # Проходим по всем подгруппам (КТС, МСС, ФМиИС...)
            for sub_label, sub_col_idx in group_columns_map.items():
                cell_data = extract_full_data(row[sub_col_idx])

                if cell_data["s"] or cell_data["t"]:
                    # Очищаем метку от цифр дубликатов: "МСС (2)" -> "МСС"
                    # Благодаря этому данные из соседних колонок (Предмет и Препод) попадут в ОДИН ключ
                    real_label = re.sub(r'\s*\(\d+\)$', '', sub_label)

                    if real_label not in slot["groups"]:
                        slot["groups"][real_label] = {"s": set(), "t": set(), "r": set()}

                    if cell_data["s"]: slot["groups"][real_label]["s"].add(cell_data["s"])
                    if cell_data["t"]: slot["groups"][real_label]["t"].add(cell_data["t"])
                    if cell_data["r"]: slot["groups"][real_label]["r"].add(cell_data["r"])

    # --- 4. Генерация итогового текста ---
    for day, times in daily_data.items():
        output.append(f"\n📅 <b>{day}</b>")
        for time, data in times.items():
            if not data["groups"]: continue
            has_data = True

            lines = []

            # Сортируем группы, чтобы порядок был детерминированным (например, КТС, потом ФМиИС)
            sorted_groups = sorted(data["groups"].items())

            for label, content in sorted_groups:
                subj = " ".join(sorted(content["s"]))
                teach = " ".join(sorted(content["t"]))
                rooms = ", ".join(sorted(content["r"]))

                # Формируем метку (МСС, КТС). "Общая" и пустую (для потока) скрываем.
                display_label = ""
                if label and "общая" not in label.lower():
                     display_label = f" ({label})"

                room_part = f" [📍 <b>{rooms}</b>]" if rooms else ""
                full_line = f"{subj} {teach}{display_label}{room_part}".strip()

                if full_line:
                    lines.append(full_line)

            # Убираем полные дубликаты строк (на всякий случай)
            unique_lines = sorted(list(set(lines)))

            final_str = " / ".join(unique_lines) if len(unique_lines) < 3 else "\n   ".join(unique_lines)
            tag = " <i>(Поток)</i>" if data["is_flow"] else ""

            output.append(f"<code>{time:12}</code> | {final_str}{tag}")

    if not has_data:
        await message.answer("🏖 <b>Занятий не найдено.</b>", parse_mode="HTML", reply_markup=ui_post_control())
    else:
        await message.answer("\n".join(output)[:4000], parse_mode="HTML")
        await message.answer("⚙️ <b>Навигация:</b>", reply_markup=ui_post_control(), parse_mode="HTML")

async def run_proff_search(msg, scope, name, day_code):
    try:
        await msg.delete()
    except:
        pass
    loading = await msg.answer("🔍 _Сканирую базу данных..._", parse_mode="Markdown")

    target_days = SEARCH_DAYS_LOW if day_code == "all" else {day_code: SEARCH_DAYS_LOW[day_code]}
    targets = list(ACADEMIC_DATA.keys()) if scope == "global" else [scope]

    found_events = {}
    time_rx = re.compile(r"\d{1,2}[:.]\d{2}")

    for hid in targets:
        df, layout = await asyncio.get_event_loop().run_in_executor(None, sync_data, hid)
        if df is None: continue

        for idx, row in df.iterrows():
            if idx < 15: continue

            # Проверка дня
            d_val = str(row[0]).lower()
            if not any(v in d_val for v in target_days.values()): continue

            # Проверка времени
            t_val = str(row[1])
            if not time_rx.search(t_val): continue

            # Проход по колонкам
            for c_idx in range(2, len(row)):
                cell_raw = str(row[c_idx])

                # Если фамилия найдена
                if name.lower() in cell_raw.lower():

                    grp_tag = "Неизв."

                    # --- ИСПРАВЛЕННАЯ ЛОГИКА ОПРЕДЕЛЕНИЯ ГРУППЫ/ПОТОКА ---
                    for fid, flow_data in layout.items():

                        # 1. ПРИОРИТЕТ: Проверяем, не является ли это колонкой ПОТОКА
                        if c_idx == flow_data["anchor_col"]:
                            # Проверяем, действительно ли это поток (нет конфликтов в других колонках)
                            # Хотя для поиска можно упростить: если препод в главной колонке - считаем потоком
                            # или пишем "Гр. 1 (Поток)"
                            grp_tag = f"Поток ({flow_data['title']})"
                            break

                        # 2. Если не поток, ищем конкретную группу
                        found_group = False
                        for g_num, groups in flow_data["map"].items():
                            for sub_label, sub_col in groups.items():
                                if sub_col == c_idx:
                                    # Очищаем " (2)" из названия
                                    clean_label = re.sub(r'\s*\(\d+\)$', '', sub_label)
                                    label_str = f" ({clean_label})" if "общая" not in clean_label.lower() else ""
                                    grp_tag = f"Гр. {g_num}{label_str}"
                                    found_group = True
                                    break
                            if found_group: break

                        if found_group: break
                    # -----------------------------------------------------

                    # Формируем описание
                    content_data = extract_full_data(cell_raw)
                    # Если extract_full_data вернул пустоту (например, там только фамилия), берем сырой текст
                    subj_text = content_data["s"] if content_data["s"] else content_data["t"]

                    # Красивое форматирование строки
                    day_name = str(row[0]).strip().upper()
                    time_name = t_val.replace("\n", " ").strip()

                    # Собираем инфо: Предмет [Кабинет] (Группа)
                    room_part = f" [🚪 {content_data['r']}]" if content_data['r'] else ""
                    full_desc = f"▫️ {subj_text}{room_part} — *{grp_tag}*"

                    if day_name not in found_events: found_events[day_name] = {}
                    if time_name not in found_events[day_name]: found_events[day_name][time_name] = []

                    # Избегаем дубликатов (если препод записан и в Subject и в Teacher ячейках одной строки)
                    if full_desc not in found_events[day_name][time_name]:
                        found_events[day_name][time_name].append(full_desc)

    await loading.delete()

    if not found_events:
        await msg.answer(f"🤷‍♂️ *Ничего не найдено для:* {name}", reply_markup=ui_post_control("proff"), parse_mode="Markdown")
        return

    report = [f"👨‍🏫 *Результаты для:* {name}"]

    # Сортируем дни недели
    sorted_days = sorted(found_events.keys(), key=lambda x: list(WEEK_DAYS.values()).index(x.title()) if x.title() in WEEK_DAYS.values() else 99)

    for d in sorted_days:
        report.append(f"\n📅 *{d}*")
        times = found_events[d]
        for t in sorted(times.keys()):
            report.append(f"  🕒 {t}")
            for job in times[t]:
                report.append(f"    {job}")

    await msg.answer("\n".join(report)[:4000], parse_mode="Markdown", reply_markup=ui_post_control("proff"))
# --- ОСНОВНЫЕ ХЕНДЛЕРЫ ---
@dp.message(Command("start"))
async def start_cmd(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("👋 *Добро пожаловать в Scheduler BOT!*",
                         reply_markup=ui_main_menu(), parse_mode="Markdown")

@dp.callback_query(F.data == "home")
async def go_home(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text("🏠 *Главное меню*", reply_markup=ui_main_menu(), parse_mode="Markdown")

@dp.callback_query(F.data.startswith("hub:"))
async def hub_click(cb: CallbackQuery):
    hid = cb.data.split(":")[1]
    await cb.message.edit_text("⏳ _Загрузка расписания..._", parse_mode="Markdown")
    df, struct = await asyncio.get_event_loop().run_in_executor(None, sync_data, hid)
    if not struct:
        await cb.message.edit_text("❌ Ошибка загрузки данных.", reply_markup=ui_main_menu())
        return
    await cb.message.edit_text(f"📍 *{ACADEMIC_DATA[hid]['label']}*\nВыберите поток:",
                               reply_markup=ui_flow_select(hid, struct), parse_mode="Markdown")

@dp.callback_query(F.data.startswith("flow:"))
async def flow_click(cb: CallbackQuery):
    _, hid, fid = cb.data.split(":")
    struct = LOCAL_STORAGE[hid]["layout"]
    await cb.message.edit_text("👥 *Выберите вашу группу:*",
                               reply_markup=ui_cluster_select(hid, fid, struct), parse_mode="Markdown")

@dp.callback_query(F.data.startswith("cls:"))
async def cluster_click(cb: CallbackQuery):
    _, hid, fid, gnum = cb.data.split(":")
    struct = LOCAL_STORAGE[hid]["layout"]

    # По умолчанию берем первую подгруппу/колонку для этой группы
    subgroups = struct[fid]["map"][int(gnum)]
    first_col = list(subgroups.values())[0]

    await cb.message.edit_text("🗓 *На какой день нужно расписание?*",
                               reply_markup=ui_day_select(hid, fid, gnum, first_col), parse_mode="Markdown")

@dp.callback_query(F.data.startswith("get:"))
async def get_schedule(cb: CallbackQuery):
    _, d, h, f, g, c = cb.data.split(":")
    USER_PREFS[cb.from_user.id] = {'hid': h, 'fid': f, 'gnum': g, 'col': c}
    await cb.answer()
    await render_schedule_output(cb.message, d, h, f, g, c)

# --- ДОБАВЛЕНИЕ КУРСОВ ---
@dp.callback_query(F.data == "today_sch")
async def cb_today(cb: CallbackQuery):
    p = USER_PREFS.get(cb.from_user.id)
    if not p:
        await cb.answer("❌ Выберите группу в меню!", show_alert=True)
        return
    await cb.answer()
    # Сегодня — это всегда текущая дата
    d = datetime.now().strftime('%a').lower()[:3]
    await render_schedule_output(cb.message, d, p['hid'], p['fid'], p['gnum'], p['col'])

@dp.callback_query(F.data == "tomorrow_sch")
async def cb_tomorrow(cb: CallbackQuery):
    p = USER_PREFS.get(cb.from_user.id)
    if not p:
        await cb.answer("❌ Выберите группу!", show_alert=True)
        return
    await cb.answer()

    now = datetime.now()
    # Если запрашиваем ночью, завтра — это следующий календарный день
    # (т.е. если сейчас 01:00 понедельника, "Завтра" = Вторник)
    tomorrow = now + timedelta(days=1)
    d = tomorrow.strftime('%a').lower()[:3]
    await render_schedule_output(cb.message, d, p['hid'], p['fid'], p['gnum'], p['col'])
async def add_hub_start(cb: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="home")]
    ])
    await cb.message.edit_text("📝 Введите короткое название для расписания (напр. 'Матфак 1 курс'):")
    await state.set_state(FormStates.input_hub_title)

@dp.message(FormStates.input_hub_title)
async def add_hub_name(msg: Message, state: FSMContext):
    await state.update_data(title=msg.text)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Отмена / Назад", callback_data="home")]
    ])
    await msg.answer("🔗 Пришлите ссылку на Google Таблицу:")
    await state.set_state(FormStates.input_hub_link)

@dp.message(FormStates.input_hub_link)
async def add_hub_final(msg: Message, state: FSMContext):
    url = msg.text
    sid = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
    gid = re.search(r"gid=([0-9]+)", url)

    if not sid:
        await msg.answer("❌ Не удалось распознать ссылку.")
        return

    data = await state.get_data()
    new_id = f"edu_{len(ACADEMIC_DATA)+1}"
    ACADEMIC_DATA[new_id] = {
        "label": data['title'],
        "sheet_id": sid.group(1),
        "gid": gid.group(1) if gid else "0"
    }
    await state.clear()
    await msg.answer(f"✅ Расписание *{data['title']}* успешно добавлено!",
                     reply_markup=ui_main_menu(), parse_mode="Markdown")

# --- ПОИСК ПРЕПОДАВАТЕЛЯ ---
@dp.callback_query(F.data == "find_proff")
async def proff_search_start(cb: CallbackQuery, state: FSMContext):
    await cb.message.edit_text("👤 Введите фамилию преподавателя:")
    await state.set_state(FormStates.input_lecturer_name)

@dp.callback_query(F.data == "track_teacher_now")
async def cb_track_teacher_start(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("👤 Введите фамилию преподавателя для live-поиска:")
    await state.set_state(FormStates.waiting_teacher_track) # Используем существующее или создай новое

# В обработчик поиска (где ты ищешь преподавателя), добавь логику проверки времени:
@dp.message(FormStates.waiting_teacher_track)
async def process_teacher_tracking(msg: Message, state: FSMContext):
    name_query = msg.text.strip().lower()
    await state.clear()
    now = datetime.now()
    curr_time = now.time()
    curr_day = WEEK_DAYS.get(now.strftime('%a').lower(), "").lower()

    found = False
    for hid in ACADEMIC_DATA:
        df, _ = await asyncio.get_event_loop().run_in_executor(None, sync_data, hid)
        for idx, row in df.iterrows():
            if idx < 10 or curr_day not in str(row[0]).lower(): continue

            start, end = parse_time_range(str(row[1]))
            if start and end and start <= curr_time <= end:
                # Проверяем всю строку
                row_str = " ".join([str(v) for v in row.values]).lower()
                if name_query in row_str:
                    found = True
                    # Пытаемся найти кабинет в этой строке
                    room_m = re.search(r'\b\d{2,4}[а-яА-Я]?\b', " ".join([str(v) for v in row.values]))
                    room = room_m.group() if room_m else "не указана"

                    await msg.answer(
                        f"📍 <b>{msg.text}</b> сейчас на паре.\n"
                        f"🚪 Аудитория: <b>{room}</b>\n"
                        f"🕒 До конца: {end.strftime('%H:%M')}",
                        parse_mode="HTML", reply_markup=ui_post_control("track")
                    )
                    return

    await msg.answer("😴 У этого преподавателя сейчас нет пар.", reply_markup=ui_post_control("track"))

@dp.callback_query(F.data == "free_rooms")
async def cb_free_rooms(cb: CallbackQuery):
    # 1. Мгновенно «отпускаем» кнопку в интерфейсе
    await cb.answer()

    now = datetime.now()
    curr_time = now.time()
    curr_day = WEEK_DAYS.get(now.strftime('%a').lower(), "").lower()

    # 2. Указываем рабочие часы (с 8 утра до 9 вечера)
    work_start = datetime.strptime("08:00", "%H:%M").time()
    work_end = datetime.strptime("21:00", "%H:%M").time()

    # 3. Проверка: если сейчас ночь или воскресенье
    if curr_time < work_start or curr_time > work_end or now.weekday() == 6:
        await cb.message.answer("🌙 <b>Университет сейчас закрыт.</b>\nВне учебного времени (08:00 - 21:00) все кабинеты свободны.", parse_mode="HTML", reply_markup=ui_post_control("free"))
        return

    # Отправляем временное сообщение, чтобы пользователь видел прогресс
    status_msg = await cb.message.answer("🔍 _Сканирую все расписания, секунду..._", parse_mode="Markdown")

    all_rooms = set()
    occupied_rooms = set()

    for hid in ACADEMIC_DATA:
        # Используем run_in_executor, чтобы не фризить бота
        df, _ = await asyncio.get_event_loop().run_in_executor(None, sync_data, hid)
        for idx, row in df.iterrows():
            if idx < 10: continue

            # Улучшенный поиск комнат (теперь видит и 521а, и 105)
            row_text = " ".join([str(v) for v in row.values])
            rooms = re.findall(r'\b\d{2,4}[а-яА-Я]?\b', row_text)
            all_rooms.update(rooms)

            if curr_day in str(row[0]).lower():
                start, end = parse_time_range(str(row[1]))
                if start and end and start <= curr_time <= end:
                    occupied_rooms.update(rooms)

    free_rooms = sorted(list(all_rooms - occupied_rooms))
    await status_msg.delete() # Удаляем «загрузку»

    if not free_rooms:
        await cb.message.answer("😱 Свободных кабинетов не найдено!", reply_markup=ui_post_control("free"))
    else:
        # Выводим первые 50 комнат
        text = "🟢 <b>Свободные кабинеты сейчас:</b>\n\n" + ", ".join(free_rooms[:50])
        await cb.message.answer(text, parse_mode="HTML",  reply_markup=ui_post_control("free"))
@dp.message(FormStates.input_lecturer_name)

async def proff_search_name(msg: Message, state: FSMContext):
    name = msg.text.strip()
    await state.clear()
    kb = [
        [InlineKeyboardButton(text="🌍 Искать везде", callback_data=f"p_scope:global:{name}")],
        [InlineKeyboardButton(text="🏠 В главном меню", callback_data="home")]
    ]
    await msg.answer(f"Где будем искать *{name}*?",
                     reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), parse_mode="Markdown")

@dp.callback_query(F.data.startswith("p_scope:"))
async def proff_scope_select(cb: CallbackQuery):
    _, scope, name = cb.data.split(":")
    # Сразу запускаем поиск на всю неделю для простоты
    await run_proff_search(cb.message, scope, name, "all")

@dp.callback_query(F.data == "find_room")
async def find_room_start(cb: CallbackQuery, state: FSMContext):
    try:
        await cb.message.edit_text("🔢 Введите номер аудитории (например, <b>402</b>):", parse_mode="HTML")
    except Exception: # Если текст уже такой же, просто игнорируем ошибку
        pass
    await state.set_state(FormStates.input_room_number)

@dp.message(StateFilter(FormStates.input_room_number))
async def process_room_search(message: Message, state: FSMContext):
    query = message.text.strip().lower()
    await state.clear()

    wait_msg = await message.answer(f"🔍 Ищу занятия в аудитории <b>{query}</b>...", parse_mode="HTML")

    found_schedule = {}
    time_pattern = re.compile(r'\d{1,2}[:.]\d{2}')

    for hid in ACADEMIC_DATA:
        df, layout = await asyncio.get_event_loop().run_in_executor(None, sync_data, hid)
        if df is None: continue

        for idx, row in df.iterrows():
            if idx < 15: continue

            # Быстрая проверка строки
            row_str = " ".join([str(x) for x in row.values]).lower()
            if query not in row_str: continue

            # Проходим по колонкам
            for col_idx in range(2, len(row)):
                cell_raw = str(row[col_idx]).strip()

                # Если нашли кабинет в ячейке
                if query in cell_raw.lower():

                    # 1. ОПРЕДЕЛЯЕМ ВЛАДЕЛЬЦА И ЯКОРНУЮ КОЛОНКУ (Anchor)
                    owner_name = "Неизвестно"
                    current_anchor_col = -1

                    for fid, flow_data in layout.items():
                        # Проверяем, входит ли эта колонка в этот поток
                        # (либо как anchor, либо как одна из групп)
                        if col_idx == flow_data["anchor_col"]:
                            owner_name = f"{flow_data['title']} (Поток)"
                            current_anchor_col = flow_data["anchor_col"]
                            break

                        found_grp = False
                        for g_num, groups in flow_data["map"].items():
                            if col_idx in groups.values():
                                # Нашли группу
                                for sub_label, sub_col in groups.items():
                                    if sub_col == col_idx:
                                        clean_label = re.sub(r'\s*\(\d+\)$', '', sub_label)
                                        label_part = f" ({clean_label})" if clean_label and "общая" not in clean_label.lower() else ""
                                        owner_name = f"Гр. {g_num}{label_part}"
                                        break
                                # Запоминаем якорь этого потока, чтобы искать там название предмета
                                current_anchor_col = flow_data["anchor_col"]
                                found_grp = True
                                break
                        if found_grp: break

                    # 2. СОБИРАЕМ ТЕКСТ (Subject/Teacher)
                    # Смотрим:
                    # А) В текущей колонке (вверх на 2 строки)
                    # Б) В ЯКОРНОЙ колонке (вверх на 2 строки) - потому что название лекции часто там!

                    context_parts = []
                    rows_to_check = [idx]
                    if idx > 15: rows_to_check.insert(0, idx - 1)
                    if idx > 16: rows_to_check.insert(0, idx - 2)

                    # Колонки для сканирования: текущая + якорная (если она отличается)
                    cols_to_scan = {col_idx}
                    if current_anchor_col != -1:
                        cols_to_scan.add(current_anchor_col)

                    for r_i in rows_to_check:
                        for c_i in cols_to_scan:
                            val = str(df.iloc[r_i, c_i]).strip()
                            if val and val.lower() != "nan":
                                context_parts.append(val)

                    full_context_text = "\n".join(context_parts)
                    data = extract_full_data(full_context_text)

                    if not data["s"] and not data["t"]:
                        continue

                    # 3. ИЩЕМ ВРЕМЯ
                    time_s = ""
                    day = ""
                    for r_i in reversed(rows_to_check):
                        t_candidate = str(df.iloc[r_i, 1]).replace('\n', ' ').strip()
                        if time_pattern.search(t_candidate):
                            time_s = t_candidate
                            day = str(df.iloc[r_i, 0]).strip().upper()
                            break

                    if not time_s or not day: continue

                    # 4. СОХРАНЯЕМ
                    subj_teach = f"{data['s']} {data['t']}".strip()

                    if day not in found_schedule: found_schedule[day] = {}
                    if time_s not in found_schedule[day]: found_schedule[day][time_s] = []

                    entry = f"{subj_teach} — <b>{owner_name}</b>"
                    if entry not in found_schedule[day][time_s]:
                        found_schedule[day][time_s].append(entry)

    await wait_msg.delete()

    if not found_schedule:
        await message.answer(f"🤷‍♂️ В ауд. <b>{query}</b> занятий не найдено.",
                            reply_markup=ui_post_control("room"), parse_mode="HTML")
        return

    # ВЫВОД
    report = [f"🏢 <b>Занятия в аудитории {query}:</b>"]
    sorted_days = sorted(found_schedule.keys(), key=lambda x: list(WEEK_DAYS.values()).index(x.title()) if x.title() in WEEK_DAYS.values() else 99)

    for d in sorted_days:
        report.append(f"\n📅 <b>{d}</b>")
        times = sorted(found_schedule[d].keys())
        for t in times:
            report.append(f"🕒 <code>{t}</code>")
            for item in found_schedule[d][t]:
                report.append(f"└ {item}")

    full_text = "\n".join(report)
    if len(full_text) > 4000:
        for x in range(0, len(full_text), 4000):
            await message.answer(full_text[x:x+4000], parse_mode="HTML")
        await message.answer("⚙️ <b>Навигация:</b>", reply_markup=ui_post_control("room"), parse_mode="HTML")
    else:
        await message.answer(full_text, parse_mode="HTML", reply_markup=ui_post_control("room"))

@dp.callback_query(F.data == "near_event")
async def cb_near_event(cb: CallbackQuery):
    now = datetime.now()
    current_time = now.time()
    current_day = WEEK_DAYS.get(now.strftime('%a').lower(), "").lower()

    found = False
    report = ["⚡️ <b>Сейчас или скоро по расписанию:</b>\n"]

    for hid in ACADEMIC_DATA:
        df, layout = await asyncio.get_event_loop().run_in_executor(None, sync_data, hid)
        if df is None: continue

        for idx, row in df.iterrows():
            if idx < 10: continue
            day_cell = str(row[0]).lower()
            if current_day not in day_cell: continue

            time_str = str(row[1])
            start, end = parse_time_range(time_str)

            if start and end:
                # Если пара идет ПРЯМО СЕЙЧАС
                if start <= current_time <= end:
                    # Считаем разницу
                    end_dt = datetime.combine(now.date(), end)
                    remains = end_dt - now
                    minutes_left = int(remains.total_seconds() // 60)

                    for col_idx in range(2, len(row)):
                        cell = scrub_content(row[col_idx])
                        if validate_subject(cell):
                            found = True
                            report.append(f"<b>СЕЙЧАС:</b>\n🕒 <code>{time_str}</code> | {cell}")
                            report.append(f"⏳ <i>До конца осталось: {minutes_left} мин.</i>\n")
    if not found:
        await cb.message.answer("🏖 Сейчас по расписанию пар нет.")
    else:
        await cb.message.answer("\n".join(report), parse_mode="HTML", reply_markup=ui_post_control())

# --- ЗАПУСК ---
async def main():
    print("🚀 Бот запущен и готов к работе!")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
