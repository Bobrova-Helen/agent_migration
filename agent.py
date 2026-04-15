# agent.py — Parquet → Iceberg Migration Agent (Together AI)
import os, json, re, sys
from pathlib import Path
from typing import Dict, List, Any, Union
from openai import OpenAI
from dotenv import load_dotenv
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
import argparse

# ==================== КОНФИГУРАЦИЯ ====================
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

API_KEY = os.getenv("TOGETHER_API_KEY")
if not API_KEY or "your" in API_KEY.lower():
    print("❌ TOGETHER_API_KEY не настроен!")
    print("1. Получите ключ: https://api.together.xyz/settings/api-keys")
    sys.exit(1)

client = OpenAI(api_key=API_KEY, base_url="https://api.together.xyz/v1")
MODEL = "meta-llama/Llama-3.3-70B-Instruct-Turbo"
MAX_ITERATIONS = 8

logger.remove()
logger.add(sys.stderr, format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>", level="INFO")


# ==================== ИНСТРУМЕНТЫ ====================
def analyze_code(input_path: str) -> Dict[str, Any]:
    """Сканирует файлы, находит паттерны Parquet."""
    path = Path(input_path)
    if not path.exists():
        return {"error": f"Path not found: {input_path}"}

    files = [path] if path.is_file() else list(path.rglob("*.py")) + list(path.rglob("*.sql")) + list(path.rglob("*.scala"))
    findings = {"files_analyzed": 0, "parquet_patterns": []}

    re_read = re.compile(r'(\w+)\.read\.(?:parquet|format\s*\(\s*["\']parquet["\']\s*\))', re.I)
    re_write = re.compile(r'\.format\s*\(\s*["\']parquet["\']\s*\)', re.I)
    re_sql = re.compile(r'USING\s+PARQUET', re.I)

    for f in files:
        try:
            lines = f.read_text(encoding="utf-8", errors="ignore").splitlines()
            findings["files_analyzed"] += 1
            for i, line in enumerate(lines, 1):
                pats = []
                if re_read.search(line): pats.append("read")
                if re_write.search(line): pats.append("write")
                if re_sql.search(line): pats.append("sql")
                if pats:
                    findings["parquet_patterns"].append({
                        "file": str(f), "line": i, "code": line.strip(), "patterns": pats
                    })
        except Exception:
            continue
    return findings


def apply_iceberg_migration(findings_json: Union[str, Dict, List]) -> Dict[str, Any]:
    """Преобразует находки в код Iceberg."""
    if isinstance(findings_json, str):
        try:
            findings = json.loads(findings_json)
        except:
            findings = {"parquet_patterns": findings_json if isinstance(findings_json, list) else []}
    elif isinstance(findings_json, dict):
        findings = findings_json
    else:
        findings = {"parquet_patterns": findings_json}

    patterns = findings.get("parquet_patterns", [])
    if isinstance(patterns, dict):
        patterns = [patterns]

    migrated = {"blocks": [], "notes": []}

    rules = [
        (r'(\w+)\.read\.(parquet|format\s*\(\s*["\']parquet["\']\s*\))\((.*?)\)',
         r'\1.read.format("iceberg").load(\3) # Migrated to Iceberg'),
        (r'(\w+)\.write\.(parquet|format\s*\(\s*["\']parquet["\']\s*\))\((.*?)\)',
         r'\1.writeTo("catalog.db.table").using("iceberg").createOrReplace() # Migrated to Iceberg'),
        (r'USING\s+PARQUET', 'USING ICEBERG'),
        (r'partitionBy\s*\((.*?)\)', r'partitionedWith(\1) # Iceberg hidden partitioning'),
        (r'\.format\s*\(\s*["\']parquet["\']\s*\)', '.format("iceberg")'),
    ]

    for p in patterns:
        if not isinstance(p, dict): continue
        code = p.get("code", "")
        original = code
        for pat, repl in rules:
            code = re.sub(pat, repl, code, flags=re.I)
        migrated["blocks"].append({
            "file": p.get("file", "unknown"),
            "line": p.get("line", 0),
            "original": original,
            "migrated": code
        })
        migrated["notes"].append(f"Мигрировано: {p.get('file')}:{p.get('line')}")
    return migrated


def validate_and_assess(migrated_json: Union[str, Dict], original_json: Union[str, Dict]) -> Dict[str, Any]:
    """Проверяет миграцию и даёт рекомендации по data velocity."""

    # 1. Безопасный парсинг аргументов
    def parse_arg(arg):
        if isinstance(arg, str):
            try:
                return json.loads(arg)
            except json.JSONDecodeError as e:
                # Если JSON кривой, логируем ошибку (в консоль), но не ломаем всё
                print(f"⚠️ Ошибка парсинга JSON: {e}")
                return {}
        return arg if isinstance(arg, dict) else {}

    mig = parse_arg(migrated_json)
    orig = parse_arg(original_json)

    # 2. Гибкий поиск блоков (поддержка разных форматов ответа LLM)
    blocks = []

    # Вариант А: mig — это сразу список блоков
    if isinstance(mig, list):
        blocks = mig

    # Вариант Б: mig — это словарь
    elif isinstance(mig, dict):
        # 1. Прямой ключ "blocks"
        if "blocks" in mig:
            blocks = mig["blocks"]
        # 2. Вложенный ключ "result": {"blocks": [...]}
        elif "result" in mig:
            res = mig["result"]
            if isinstance(res, list):
                blocks = res
            elif isinstance(res, dict) and "blocks" in res:
                blocks = res["blocks"]
            elif isinstance(res, dict) and "original" in res:
                blocks = [res]  # Один объект вместо списка
        # 3. Ключ "migrated_code_blocks"
        elif "migrated_code_blocks" in mig:
            blocks = mig["migrated_code_blocks"]

    if isinstance(blocks, dict) and "original" in blocks:
        blocks = [blocks]

    if not isinstance(blocks, list):
        blocks = []

    leftovers = []
    for b in blocks:
        if not isinstance(b, dict): continue
        migrated_code = b.get("migrated", "").lower()
        if "parquet" in migrated_code and "iceberg" not in migrated_code:
            leftovers.append(b)

    assessment = (
        "🔹 Data Velocity & Architecture Assessment:\n"
        "• Parquet: обычно daily-batch (cron/airflow), задержка 1-24ч.\n"
        "• Iceberg: поддерживает ACID, upserts, time-travel → можно перейти на micro-batch (5-15 мин).\n"
        "• Рекомендации:\n"
        "  - Замените явное partitionBy на Iceberg hidden partitioning + auto-compaction\n"
        "  - Настройте snapshot expiration: spark.sql('CALL catalog.system.expire_snapshots(...)')\n"
        "  - Для streaming: используйте Structured Streaming + Iceberg sink\n"
        "• Ожидаемый выигрыш: latency ↓ с 24ч до <15мин, устранение overwrite-гонок, точные upserts."
    )

    return {
        "validation_passed": len(leftovers) == 0,
        "leftover_count": len(leftovers),
        "leftovers": leftovers[:3],
        "architecture_recommendations": assessment,
        "summary": f"✅ Проанализировано: {orig.get('files_analyzed', 0)} файлов. "
                   f"🔄 Мигрировано: {len(blocks)} блоков. "
                   f"{'🎉 Чисто!' if not leftovers else f'⚠️ Осталось упоминаний Parquet: {len(leftovers)}'}"
    }


# ==================== СХЕМЫ ИНСТРУМЕНТОВ ====================
TOOLS_SCHEMA = [
    {
        "type": "function",
        "function": {
            "name": "analyze_code",
            "description": "Сканирует путь, находит все упоминания Parquet. Возвращает JSON с находками.",
            "parameters": {
                "type": "object",
                "properties": {
                    "input_path": {"type": "string", "description": "Путь к файлу или директории"}
                },
                "required": ["input_path"],
                "additionalProperties": False
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "apply_iceberg_migration",
            "description": "Принимает JSON-результат от analyze_code и возвращает мигрированный код Iceberg.",
            "parameters": {
                "type": "object",
                "properties": {
                    "findings_json": {
                        "type": "string",
                        "description": "JSON-строка с результатом analyze_code. Пример: '{\"parquet_patterns\": [...]}'"
                    }
                },
                "required": ["findings_json"],
                "additionalProperties": False
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "validate_and_assess",
            "description": "Проверяет миграцию и даёт архитектурные рекомендации по data velocity.",
            "parameters": {
                "type": "object",
                "properties": {
                    "migrated_code_json": {"type": "string", "description": "JSON от apply_iceberg_migration"},
                    "original_findings_json": {"type": "string", "description": "JSON от analyze_code"}
                },
                "required": ["migrated_code_json", "original_findings_json"],
                "additionalProperties": False
            }
        }
    }
]

# ==================== SYSTEM PROMPT ====================
SYSTEM_PROMPT = """Ты — старший Data Engineer AI-агент для миграции Parquet → Apache Iceberg.

🔹 ПРАВИЛА ВЫЗОВА ИНСТРУМЕНТОВ (СТРОГО):
1. analyze_code: {"input_path": "./src"}
2. apply_iceberg_migration: {"findings_json": "<JSON-строка от analyze_code>"} — НЕ путь, НЕ сырой код!
3. validate_and_assess: {"migrated_code_json": "...", "original_findings_json": "..."}

🔹 ПОРЯДОК РАБОТЫ:
1. Вызови analyze_code для входного пути
2. Передай результат (как JSON-строку!) в apply_iceberg_migration
3. Передай оба результата в validate_and_assess
4. Верни финальный отчёт пользователю

🔹 ЕСЛИ ОШИБКА:
- Если инструмент вернул error — исправь аргументы и повтори вызов
- Не выдумывай аргументы. Не пропускай шаги.

🔹 ФИНАЛЬНЫЙ ОТВЕТ:
Должен содержать: (1) мигрированные блоки кода, (2) статус валидации, (3) рекомендации по data velocity."""


# ==================== АГЕНТНЫЙ ЦИКЛ ====================
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8))
def call_llm(messages: List[Dict]) -> Any:
    return client.chat.completions.create(
        model=MODEL, messages=messages, tools=TOOLS_SCHEMA,
        tool_choice="auto", temperature=0.2
    )


def extract_arg(raw_args: Dict, possible_names: List[str], require_json_string: bool = False) -> Any:
    """Гибко извлекает аргумент по любому из возможных имён."""
    for name in possible_names:
        if name in raw_args:
            val = raw_args[name]
            if require_json_string and isinstance(val, (dict, list)):
                return json.dumps(val, ensure_ascii=False)
            return val
    return None


def run_agent(input_path: str) -> str:
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    messages.append({"role": "user", "content": f"Выполни миграцию для: {input_path}"})

    for step in range(1, MAX_ITERATIONS + 1):
        logger.info(f"Шаг {step}. Запрос к LLM...")
        response = call_llm(messages)
        msg = response.choices[0].message

        if not msg.tool_calls:
            return msg.content or "✅ Миграция завершена."

        messages.append(msg)

        for tc in msg.tool_calls:
            func_name = tc.function.name
            try:
                raw_args = json.loads(tc.function.arguments)
                logger.info(f"🔧 Вызов: {func_name} | args: {list(raw_args.keys())}")

                if func_name == "analyze_code":
                    arg = extract_arg(raw_args, ["input_path", "path", "file", "dir"])
                    if not arg: raise ValueError("Нет аргумента input_path")
                    result = analyze_code(arg)

                elif func_name == "apply_iceberg_migration":
                    arg = extract_arg(raw_args, ["findings_json", "findings", "parquet_patterns", "data", "result"],
                                      require_json_string=False)
                    if arg is None: raise ValueError("Нет аргумента с данными для миграции")
                    result = apply_iceberg_migration(arg)

                elif func_name == "validate_and_assess":
                    mig = extract_arg(raw_args, ["migrated_code_json", "migrated", "result", "output"],
                                      require_json_string=False)
                    orig = extract_arg(raw_args, ["original_findings_json", "original", "findings", "input"],
                                       require_json_string=False)
                    if mig is None or orig is None:
                        raise ValueError(f"Нет аргументов для валидации. Получено: {list(raw_args.keys())}")
                    result = validate_and_assess(mig, orig)
                else:
                    raise ValueError(f"Неизвестный инструмент: {func_name}")

                result_str = json.dumps(result, ensure_ascii=False, default=str)

            except Exception as e:
                logger.error(f"❌ Ошибка {func_name}: {e} | args: {raw_args}")
                result_str = json.dumps({"error": str(e), "received_args": raw_args}, ensure_ascii=False)

            messages.append({"role": "tool", "tool_call_id": tc.id, "content": result_str})
            logger.info(f"✅ Инструмент {func_name} выполнен")

    return "⚠️ Достигнут лимит шагов. Проверьте логи."


# ==================== 📄 ФУНКЦИЯ СОХРАНЕНИЯ ОТЧЁТА ====================
def save_report(result: str, output_path: str = "migration_report.md"):
    """Сохраняет отчёт агента в Markdown-файл."""
    from datetime import datetime
    report = f"""# 📊 Parquet → Iceberg Migration Report
**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

{result}

---
*Report generated by Parquet2Iceberg Agent*
"""
    Path(output_path).write_text(report, encoding="utf-8")
    logger.info(f"📄 Отчёт сохранён: {output_path}")


# ==================== CLI ENTRY POINT ====================
def main():
    """
    Запуск агента из командной строки.

    Использование:
        python agent.py                          # демо-режим с папкой sample/
        python agent.py ./my_project             # сканировать конкретную папку
        python agent.py ./src/etl.py             # сканировать один файл
    """

    parser = argparse.ArgumentParser(
        description="🧊 Parquet → Iceberg Migration Agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  %(prog)s                          # Запустить демо с тестовыми файлами
  %(prog)s ./my_project             # Мигрировать код в папке ./my_project
  %(prog)s ./src/etl.py             # Мигрировать один файл
  %(prog)s ./src --output report.md # Сохранить отчёт в указанный файл
        """
    )
    parser.add_argument(
        "input_path",
        nargs="?",
        default=None,
        help="Путь к файлу или директории с кодом (по умолчанию: демо-режим)"
    )
    parser.add_argument(
        "--output", "-o",
        default="migration_report.md",
        help="Путь для сохранения отчёта (по умолчанию: migration_report.md)"
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Создать демо-файлы и запустить на них (игнорирует input_path)"
    )

    args = parser.parse_args()

    # 🎯 Режим 1: Демо с созданием тестовых файлов
    if args.demo or args.input_path is None:
        sample_dir = Path("sample")
        if not sample_dir.exists():
            sample_dir.mkdir(exist_ok=True)
            (sample_dir / "etl.py").write_text(
                'df = spark.read.parquet("s3://raw/")\n'
                'df.write.partitionBy("date").parquet("s3://processed/")\n'
            )
            (sample_dir / "schema.sql").write_text(
                "CREATE TABLE events USING PARQUET PARTITIONED BY (event_date);"
            )
            logger.info("📁 Демо-файлы созданы в папке 'sample/'")
        input_path = "sample"
    else:
        # 🎯 Режим 2: Пользовательский путь
        input_path = args.input_path
        if not Path(input_path).exists():
            logger.error(f"❌ Путь не найден: {input_path}")
            sys.exit(1)

    print(f"🚀 Запуск агента для: {input_path}")
    result = run_agent(input_path)

    print("\n" + "=" * 70)
    print(result)
    print("=" * 70)

    save_report(result, output_path=args.output)


if __name__ == "__main__":
    main()