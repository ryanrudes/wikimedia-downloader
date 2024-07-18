def category2dirname(category: str) -> str:
    """Converts a MediaWiki category title to a directory name."""
    dirname = category.replace("Category:", "").replace("/", ":")
    return dirname