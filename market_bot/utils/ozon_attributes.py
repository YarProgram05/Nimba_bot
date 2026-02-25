import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OzonCategoryAttribute:
    id: int
    name: str
    dictionary_id: int


def flatten_description_category_tree(tree_result: list[dict]) -> dict[int, str]:
    """Convert /v1/description-category/tree result into map {description_category_id: category_name}."""

    mapping: dict[int, str] = {}

    def walk(nodes: list[dict]):
        for node in nodes or []:
            dcid = node.get("description_category_id")
            cname = node.get("category_name")
            if dcid is not None and cname:
                try:
                    mapping[int(dcid)] = str(cname)
                except Exception:
                    pass
            children = node.get("children")
            if children:
                walk(children)

    walk(tree_result or [])
    return mapping


def pick_attribute_id_by_keywords(attributes: list[dict], keywords: list[str]) -> int | None:
    """Try to find attribute id by name keywords (case-insensitive)."""
    if not attributes:
        return None

    kw = [k.strip().lower() for k in keywords if k and k.strip()]
    if not kw:
        return None

    for attr in attributes:
        name = str(attr.get("name") or "").strip().lower()
        if not name:
            continue
        if any(k in name for k in kw):
            try:
                return int(attr.get("id"))
            except Exception:
                continue

    return None


def extract_attribute_values_from_product_attributes(product_attributes_item: dict, attribute_id: int) -> tuple[list[str], list[int]]:
    """From /v4/product/info/attributes item extract string values and dictionary ids for a given attribute_id."""
    str_values: list[str] = []
    dict_ids: list[int] = []

    for attr in product_attributes_item.get("attributes") or []:
        if attr.get("id") != attribute_id:
            continue

        for v in attr.get("values") or []:
            value = v.get("value")
            if value is not None and str(value).strip() != "":
                str_values.append(str(value).strip())

            dvid = v.get("dictionary_value_id")
            if dvid not in (None, 0, "0"):
                try:
                    dict_ids.append(int(dvid))
                except Exception:
                    pass

    # de-dup preserve order
    seen_s: set[str] = set()
    uniq_s: list[str] = []
    for s in str_values:
        if s not in seen_s:
            uniq_s.append(s)
            seen_s.add(s)

    seen_i: set[int] = set()
    uniq_i: list[int] = []
    for i in dict_ids:
        if i not in seen_i:
            uniq_i.append(i)
            seen_i.add(i)

    return uniq_s, uniq_i


def build_category_full_paths(tree_result: list[dict], sep: str = " / ") -> dict[int, str]:
    """Build map {description_category_id: full_path_name} walking /v1/description-category/tree.

    Example: 'Одежда / Женщинам / Домашняя одежда / Туники'.
    """

    mapping: dict[int, str] = {}

    def walk(nodes: list[dict], path: list[str]):
        for node in nodes or []:
            name = node.get("category_name")
            dcid = node.get("description_category_id")

            next_path = path
            if name:
                next_path = path + [str(name)]

            if dcid is not None and name:
                try:
                    mapping[int(dcid)] = sep.join(next_path)
                except Exception:
                    pass

            children = node.get("children")
            if children:
                walk(children, next_path)

    walk(tree_result or [], [])
    return mapping


def looks_like_material_value(value: str) -> bool:
    """Heuristic filter: reject obviously non-material phrases."""
    v = (value or "").strip().lower()
    if not v:
        return False

    # Явный мусор/свойства, а не материалы
    banned_substrings = [
        "без подклада",
        "без подклад",
        "подклад",
        "утепл",
        "мембран",
        "водооттал",
        "ветрозащит",
        "антистат",
        "дышащ",
        "непромока",
        "смесовая ткань",
        "смесов",
        "стрейч",
        "эласт",
        "облегчен",
    ]
    if any(b in v for b in banned_substrings):
        return False

    # Слишком общие формулировки
    too_generic = ["ткань", "материал", "текстиль"]
    if v in too_generic:
        return False

    return True


def pick_best_attribute_id(attributes: list[dict], preferred_names: list[str], fallback_keywords: list[str]) -> int | None:
    """Pick attribute id: first by exact preferred names, then by keywords."""
    if not attributes:
        return None

    preferred = {p.strip().lower() for p in preferred_names if p and p.strip()}
    if preferred:
        for attr in attributes:
            name = str(attr.get("name") or "").strip().lower()
            if name in preferred:
                try:
                    return int(attr.get("id"))
                except Exception:
                    pass

    return pick_attribute_id_by_keywords(attributes, fallback_keywords)


def normalize_material_text(values: list[str]) -> str:
    cleaned: list[str] = []
    for v in values:
        s = str(v).strip()
        if not s:
            continue
        # первая буква заглавная, остальное как есть
        cleaned.append(s)

    # de-dup preserve order
    seen: set[str] = set()
    uniq: list[str] = []
    for s in cleaned:
        key = s.lower()
        if key not in seen:
            uniq.append(s)
            seen.add(key)

    return ", ".join(uniq) if uniq else "—"

