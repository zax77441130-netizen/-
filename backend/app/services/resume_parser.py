import re


SKILL_KEYWORDS = ["forklift", "warehouse", "assembly", "packing", "operator", "QC", "堆高機", "倉管", "組裝", "包裝", "作業員", "品檢"]
LICENSE_KEYWORDS = ["forklift license", "堆高機證照", "職安卡", "駕照"]
AREA_KEYWORDS = ["台北", "桃園", "新竹", "台中", "台南", "高雄", "Taipei", "Taoyuan", "Hsinchu", "Taichung", "Tainan", "Kaohsiung"]
SHIFT_KEYWORDS = ["日班", "夜班", "中班", "輪班", "Day", "Night", "Rotating"]


def parse_resume_text(raw_text: str) -> dict:
    text = raw_text.strip()
    email = first_match(r"[\w.+-]+@[\w-]+\.[\w.-]+", text)
    phone = first_match(r"09\d{8}", normalize_phone_text(text))
    name = parse_name(text)
    skills = [keyword for keyword in SKILL_KEYWORDS if keyword.lower() in text.lower()]
    licenses = [keyword for keyword in LICENSE_KEYWORDS if keyword.lower() in text.lower()]
    areas = [keyword for keyword in AREA_KEYWORDS if keyword.lower() in text.lower()]
    shifts = [keyword for keyword in SHIFT_KEYWORDS if keyword.lower() in text.lower()]
    availability = first_match(r"(?:最快到職|available from|start date)[:：\s]*(.{1,30})", text)
    return {
        "name": name,
        "phone": phone,
        "email": email,
        "region": areas[0] if areas else None,
        "preferred_area": areas[0] if areas else None,
        "preferred_shift": shifts[0] if shifts else None,
        "skills": skills,
        "licenses": licenses,
        "availability": availability,
        "missing_info": missing_info(phone, email, areas, shifts),
        "summary": build_summary(name, areas, shifts, skills, licenses),
    }


def normalize_phone_text(text: str) -> str:
    return text.replace("-", "").replace(" ", "")


def first_match(pattern: str, text: str) -> str | None:
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return None
    if match.lastindex:
        return match.group(match.lastindex).strip()
    return match.group(0).strip()


def parse_name(text: str) -> str | None:
    explicit = first_match(r"(?:姓名|Name)[:：\s]*([\u4e00-\u9fffA-Za-z ]{2,40})", text)
    if explicit:
        return explicit
    first_line = text.splitlines()[0].strip() if text.splitlines() else ""
    if 2 <= len(first_line) <= 40 and "@" not in first_line and not re.search(r"\d", first_line):
        return first_line
    return None


def missing_info(phone: str | None, email: str | None, areas: list[str], shifts: list[str]) -> list[str]:
    missing = []
    if not phone and not email:
        missing.append("contact")
    if not areas:
        missing.append("preferred_area")
    if not shifts:
        missing.append("preferred_shift")
    return missing


def build_summary(name: str | None, areas: list[str], shifts: list[str], skills: list[str], licenses: list[str]) -> str:
    parts = []
    if name:
        parts.append(f"Candidate {name}")
    if areas:
        parts.append(f"areas: {', '.join(areas)}")
    if shifts:
        parts.append(f"shifts: {', '.join(shifts)}")
    if skills:
        parts.append(f"skills: {', '.join(skills)}")
    if licenses:
        parts.append(f"licenses: {', '.join(licenses)}")
    return "; ".join(parts) if parts else "Resume parsed; HR review required."
