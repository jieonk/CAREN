
import os
import json
import re
import email
import pandas as pd
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil.parser import parse as date_parse
import xml.etree.ElementTree as ET
from openai import OpenAI
from email.header import decode_header


client  = OpenAI(api_key = "")

def extract_shard_from_gpt(text):
    prompt = f"""
Extract exactly ONE behavioral action in detail that best represents the following content.
Return a JSON with these fields only:

  "A": action description,
  "T_A": action time (ISO 8601 preferred),
  "C": contextual message content
  
Content:
{text}
"""
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You extract human actions from unstructured text."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.4,
            max_tokens=500
        )

        gpt_output = response.choices[0].message.content.strip()

        # 디버깅용 원본 출력
        print("🧠 GPT RAW OUTPUT:")
        print(gpt_output)
        print("-" * 50)

        # 코드 블록 제거
        if gpt_output.startswith("```json"):
            gpt_output = gpt_output.replace("```json", "").replace("```", "").strip()

        # ✅ JSON 바로 파싱 시도 (리스트든 객체든 대응)
        try:
            result = json.loads(gpt_output)
            print(f"✅ Parsed shard(s): {result}")
            return result
        except Exception as e:
            print("❌ Failed to parse GPT output:", e)
            return None

    except Exception as e:
        print("❌ GPT call failed:", e)
        return None


def get_file_modified_time(file_path):
    ts = os.path.getmtime(file_path)
    return datetime.fromtimestamp(ts).isoformat()

def decode_mime_header(value):
    if not value:
        return ""
    parts = decode_header(value)
    decoded = ""
    for part, encoding in parts:
        try:
            decoded += part.decode(encoding or 'utf-8') if isinstance(part, bytes) else part
        except:
            decoded += str(part)
    return decoded

def extract_eml_received_date(msg):
    received_headers = msg.get_all("Received", [])
    for header in received_headers:
        parts = header.split(";")
        if len(parts) > 1:
            try:
                return date_parse(parts[-1].strip())
            except:
                continue
    return None
'''
def extract_actions_from_shard(shard):
    if isinstance(shard, list):  # ✅ GPT가 여러 개 A/T_A를 리스트로 반환한 경우
        return [
            {"action": entry.get("A"), "timestamp": entry.get("T_A"),    "context": shard.get("C")}
            for entry in shard if isinstance(entry, dict) and entry.get("A")
        ]
    elif isinstance(shard, dict):  # ✅ 예전처럼 단일 딕셔너리로 반환된 경우
        actions = shard.get("A", [])
        if not isinstance(actions, list):
            actions = [actions]
        return [{"action": a.strip(), "timestamp": shard.get("T_A")} for a in actions]
    else:
        return []
'''
def extract_actions_from_shard(shard):
    seen = set()

    def is_new(action_str):
        if not action_str:
            return False
        key = action_str.strip()
        if key in seen:
            return False
        seen.add(key)
        return True

    actions = []
    if isinstance(shard, list):
        for entry in shard:
            if isinstance(entry, dict) and entry.get("A") and is_new(entry.get("A")):
                actions.append({
                    "action": entry.get("A"),
                    "timestamp": entry.get("T_A"),
                    "context": entry.get("C")
                })
    elif isinstance(shard, dict):
        a = shard.get("A")
        if isinstance(a, list):
            for x in a:
                if is_new(x):
                    actions.append({
                        "action": x.strip(),
                        "timestamp": shard.get("T_A"),
                        "context": shard.get("C")
                    })
        elif isinstance(a, str) and is_new(a):
            actions.append({
                "action": a.strip(),
                "timestamp": shard.get("T_A"),
                "context": shard.get("C")
            })
    return actions

def parse_text_file(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            text = f.read()
    except UnicodeDecodeError:
        with open(file_path, "r", encoding="iso-8859-1") as f:
            text = f.read()
    shard = extract_shard_from_gpt(text)
    return extract_actions_from_shard(shard) if shard else []

def parse_json_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    text = str(data)
    shard = extract_shard_from_gpt(text)
    return extract_actions_from_shard(shard) if shard else []

def parse_xml_file(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    text = ET.tostring(root, encoding='unicode')
    shard = extract_shard_from_gpt(text)
    return extract_actions_from_shard(shard) if shard else []

def parse_html_file(file_path):
    for enc in ['utf-8', 'euc-kr', 'cp949']:
        try:
            with open(file_path, "r", encoding=enc) as f:
                content = f.read()
            break
        except UnicodeDecodeError:
            continue
    else:
        return []
    soup = BeautifulSoup(content, "html.parser")
    text = soup.get_text(separator=' ')
    shard = extract_shard_from_gpt(text.strip())
    return extract_actions_from_shard(shard) if shard else []

def parse_eml_file(file_path):
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        msg = email.message_from_file(f)

    subject = decode_mime_header(msg.get("Subject"))
    sender = decode_mime_header(msg.get("From"))
    date = decode_mime_header(msg.get("Date"))

    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                try:
                    body += part.get_payload(decode=True).decode(part.get_content_charset() or "utf-8", errors="ignore")
                except:
                    continue
    else:
        try:
            body = msg.get_payload(decode=True).decode(msg.get_content_charset() or "utf-8", errors="ignore")
        except:
            body = ""

    text = f"Subject: {subject}\nFrom: {sender}\nDate: {date}\n\n{body}"
    shard = extract_shard_from_gpt(text)
    return extract_actions_from_shard(shard) if shard else []

def parse_csv_file(file_path):
    df = pd.read_csv(file_path)
    actions = []
    for _, row in df.iterrows():
        content = "\n".join([f"{col}: {val}" for col, val in row.items() if pd.notna(val)])
        shard = extract_shard_from_gpt(content)
        if shard:
            actions.extend(extract_actions_from_shard(shard))
    return actions

def parse_excel_file(file_path):
    df = pd.read_excel(file_path)
    actions = []
    for _, row in df.iterrows():
        content = "\n".join([f"{col}: {val}" for col, val in row.items() if pd.notna(val)])
        shard = extract_shard_from_gpt(content)
        if shard:
            actions.extend(extract_actions_from_shard(shard))
    return actions


def parse_sqlite_file(file_path):
    actions = []
    db_name = os.path.basename(file_path)

    try:
        conn = sqlite3.connect(file_path)

        if db_name == "Favicons.db":
            query = """
            SELECT im.page_url, im.icon_id, fb.image_data, fb.last_updated, fb.width, fb.height
            FROM icon_mapping im
            JOIN favicon_bitmaps fb ON im.icon_id = fb.icon_id
            """
            df = pd.read_sql_query(query, conn)

            for idx, row in df.iterrows():
                text_content = "\n".join([f"{col}: {val}" for col, val in row.items() if pd.notna(val)])
                print(text_content)

                shard = extract_shard_from_gpt(text_content)
                print(f"🧠 GPT SHARD: {shard}")

                if shard and isinstance(shard, dict) and shard.get("A"):
                    shard["source_table"] = "icon_mapping + favicon_bitmaps"

                    last_updated = row.get("last_updated")
                    if last_updated:
                        try:
                            parsed = pd.to_datetime(str(last_updated), errors='coerce')
                            if pd.notna(parsed):
                                shard["T_A"] = parsed.isoformat()
                        except Exception as e:
                            print(f"⚠️ last_updated 변환 실패: {last_updated} → {e}")

                    try:
                        shard["T_S"] = datetime.strptime("2020-12-29 17:30:22", "%Y-%m-%d %H:%M:%S").isoformat()
                    except Exception as e:
                        print(f"⚠️ T_S 고정값 설정 실패: {e}")

                    actions_before = len(actions)
                    actions.extend(extract_actions_from_shard(shard))
                    print(f"➕ Actions added: {len(actions) - actions_before}")
                else:
                    print(f"⚠️ Skipped invalid shard (no action): {shard}")

        conn = sqlite3.connect(file_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        for table_name in tables:
            table = table_name[0]
            try:
                df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                for _, row in df.iterrows():
                    text = "\n".join([f"{col}: {val}" for col, val in row.items() if pd.notna(val)])
                    shard = extract_shard_from_gpt(text)

                    if shard:
                        if isinstance(shard, dict) and shard.get("A"):
                            shard["source_table"] = table
                            actions.extend(extract_actions_from_shard(shard))

                        elif isinstance(shard, list):
                            for s in shard:
                                if isinstance(s, dict) and s.get("A"):
                                    s["source_table"] = table
                                    actions.append(s)

                        if db_name == "tms_1.0.db" and table.strip().upper() == "TBL_MSG":
                            reg_date = row.get("REG_DATE")
                            if reg_date and pd.notna(reg_date):
                                try:
                                    parsed = pd.to_datetime(str(reg_date), errors='coerce')
                                    if pd.notna(parsed):
                                        for s in extract_actions_from_shard(shard):
                                            s["T_A"] = parsed.isoformat()
                                except Exception as e:
                                    print(f"⚠️ REG_DATE 변환 실패: {reg_date} → {e}")
                    else:
                        print(f"⚠️ Skipped shard (no action): {shard}")
            except Exception as e:
                print(f"⚠️ Failed to read table {table}: {e}")
    finally:
        conn.close()

    return actions


def parse_file_by_extension(file_path):
    ext = os.path.splitext(file_path)[1].lower()

    # 기본 파일별 action 추출
    if ext == ".txt":
        content = parse_text_file(file_path)
    elif ext == ".json":
        content = parse_json_file(file_path)
    elif ext == ".xml":
        content = parse_xml_file(file_path)
    elif ext in [".html", ".htm"]:
        content = parse_html_file(file_path)
    elif ext == ".eml":
        content = parse_eml_file(file_path)
    elif ext == ".csv":
        content = parse_csv_file(file_path)
    elif ext in [".xls", ".xlsx"]:
        content = parse_excel_file(file_path)
    elif ext in [".db", ".sqlite"]:
        content = parse_sqlite_file(file_path)
    else:
        content = []

    # 메타데이터 추출
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            raw_text = f.read()
    except:
        raw_text = ""

    metadata = extract_metadata_fields_from_gpt(raw_text) if raw_text else {}

    return {
        "filePath": file_path,
        "actions": content,
        "metadata": metadata
    }


def extract_metadata_fields_from_gpt(text):
    keys = ["device_id", "user_id", "address", "card_number", "ip_address"]
    prompt = f"""
You are an information extractor. Extract values for the following keys from the content below:
{', '.join(keys)}

If the content includes any value that looks like a device ID, user ID, IP address, etc., extract it.

Return result in JSON:
{{
  "device_id": ...,
  "user_id": ...,
  "address": ...,
  "card_number": ...,
  "ip_address": ...
}}

Content:
{text}
"""
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You extract specific field values from unstructured text."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=300
        )

        output = response.choices[0].message.content.strip()
        if output.startswith("```json"):
            output = output.replace("```json", "").replace("```", "").strip()
            print("🧠 GPT METADATA RAW OUTPUT:")
            print(output)
        return json.loads(output)

    except Exception as e:
        print("❌ Metadata extraction failed:", e)
        print("🔍 Raw GPT metadata output was:")
        print(output)
        return {}