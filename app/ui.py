# app/ui.py
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Tuple, Iterable

from .adb import AdbClient


_BOUNDS_RE = re.compile(r'bounds="\[(\d+),(\d+)\]\[(\d+),(\d+)\]"')
_ID_RE = re.compile(r'resource-id="([^"]+)"')
_PKG_RE = re.compile(r'package="([^"]+)"')
_CLASS_RE = re.compile(r'class="([^"]+)"')


@dataclass(frozen=True)
class NodeMatch:
    resource_id: Optional[str]
    package: Optional[str]
    class_name: Optional[str]
    bounds: Tuple[int, int, int, int]


def _iter_xml_nodes(xml: str) -> Iterable[str]:
    """
    Very light-weight "node splitter": we only need node-level attribute lines.
    uiautomator dump produces a flat XML where <node .../> has all attrs.
    """
    # Ensure tags are on separate lines like your sed did
    # by splitting on '><' boundaries.
    s = xml.replace("><", ">\n<")
    for line in s.splitlines():
        line = line.strip()
        if line.startswith("<node "):
            yield line


def parse_bounds(node_line: str) -> Optional[Tuple[int, int, int, int]]:
    m = _BOUNDS_RE.search(node_line)
    if not m:
        return None
    x1, y1, x2, y2 = (int(m.group(i)) for i in range(1, 5))
    return (x1, y1, x2, y2)


def center(bounds: Tuple[int, int, int, int]) -> Tuple[int, int]:
    x1, y1, x2, y2 = bounds
    return ((x1 + x2) // 2, (y1 + y2) // 2)


def dump_ui(adb: AdbClient, remote_path: str = "/sdcard/ui.xml") -> str:
    """
    Runs uiautomator dump and returns the XML as a string.
    """
    adb.shell(f"uiautomator dump {remote_path} >/dev/null")
    # Use cat to return content; robust enough for our usage.
    return adb.shell(f"cat {remote_path}", check=True).stdout


def find_first_by_resource_id(
    xml: str,
    resource_id_substring: str,
) -> Optional[NodeMatch]:
    """
    Find first node line containing given substring in resource-id.
    We match by substring to keep it tolerant (exact ID is often long).
    """
    for line in _iter_xml_nodes(xml):
        rid_m = _ID_RE.search(line)
        if not rid_m:
            continue
        rid = rid_m.group(1)
        if resource_id_substring not in rid:
            continue
        b = parse_bounds(line)
        if not b:
            continue
        pkg = _PKG_RE.search(line)
        cls = _CLASS_RE.search(line)
        return NodeMatch(
            resource_id=rid,
            package=pkg.group(1) if pkg else None,
            class_name=cls.group(1) if cls else None,
            bounds=b,
        )
    return None


def find_first_edittext_in_package(xml: str, package: str) -> Optional[NodeMatch]:
    """
    Equivalent to your grep:
      package="com.garmin..." AND class="android.widget.EditText"
    """
    for line in _iter_xml_nodes(xml):
        pkg_m = _PKG_RE.search(line)
        cls_m = _CLASS_RE.search(line)
        if not pkg_m or not cls_m:
            continue
        if pkg_m.group(1) != package:
            continue
        if cls_m.group(1) != "android.widget.EditText":
            continue
        b = parse_bounds(line)
        if not b:
            continue
        rid_m = _ID_RE.search(line)
        return NodeMatch(
            resource_id=rid_m.group(1) if rid_m else None,
            package=package,
            class_name="android.widget.EditText",
            bounds=b,
        )
    return None


def tap_node(adb: AdbClient, node: NodeMatch) -> None:
    x, y = center(node.bounds)
    adb.input_tap(x, y)
