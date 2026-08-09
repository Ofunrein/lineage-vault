from __future__ import annotations

from ..models.events import CompatLevel, SchemaVersion


def classify_compat(old: SchemaVersion, new: SchemaVersion) -> CompatLevel:
    old_f, new_f = old.fields, new.fields
    removed = set(old_f) - set(new_f)
    added = set(new_f) - set(old_f)
    type_changes = [k for k in old_f if k in new_f and old_f[k] != new_f[k]]
    if removed or type_changes:
        return CompatLevel.BREAKING
    if added:
        return CompatLevel.BACKWARD
    return CompatLevel.FORWARD

class SchemaEngine:
    def evaluate(self, old: dict[str, str], new: dict[str, str]) -> CompatLevel:
        return classify_compat(SchemaVersion(version=1, fields=old), SchemaVersion(version=2, fields=new))
