def excluded_dids(summary: dict) -> set[str]:
    authors = summary.get("authors", [])
    labeled = [author for author in authors if author.get("state") == "labeled"]
    expected = summary.get("counts", {}).get("labeled")
    if not isinstance(expected, int) or len(labeled) != expected:
        raise ValueError("labeler summary does not contain every labeled author")
    return {
        author["did"]
        for author in labeled
        if not author.get("kept", False)
        and isinstance(author.get("did"), str)
    }


def filter_labeled_rows(rows: list[dict], excluded: set[str]) -> list[dict]:
    return [row for row in rows if row.get("did") not in excluded]
