import re
from typing import List, Dict, Optional


class CodeOwnersParser:
    def __init__(self, codeowners_content: str):
        self.patterns = self._parse_codeowners(codeowners_content)

    def _parse_codeowners(self, content: str) -> Dict[str, List[str]]:
        patterns = {}
        for line in content.splitlines():
            line = line.strip()

            # Ignore empty lines and comments
            if not line or line.startswith("#"):
                continue

            # Split the line into pattern and owners, ignore inline comments
            main_part = line.split("#", 1)[0].strip()
            parts = re.split(r"\s+", main_part)

            if len(parts) < 2:
                continue  # Skip invalid lines

            pattern, owners = parts[0], parts[1:]
            patterns[pattern] = owners
        return patterns

    def get_owners(self, filepath: str) -> Optional[List[str]]:
        matching_patterns = []
        for pattern in self.patterns:
            if self._matches_pattern(filepath, pattern):
                matching_patterns.append(pattern)

        if not matching_patterns:
            return None

        # Return the owners from the most specific (last) matching pattern
        return self.patterns[matching_patterns[-1]]

    def _matches_pattern(self, filepath: str, pattern: str) -> bool:
        if pattern.startswith("/"):
            return filepath.startswith(pattern[1:])
        if pattern.startswith("**/"):
            return f"/{pattern[3:]}" in filepath or filepath.endswith(pattern[3:])
        if "*" in pattern:
            # Convert wildcard patterns to regex
            regex_pattern = re.escape(pattern).replace(r"\*", ".*")
            return re.match(f"^{regex_pattern}$", filepath) is not None
        return filepath == pattern

    def is_valid_syntax(self, content: str) -> bool:
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # Split the line into pattern and owners, ignore inline comments
            main_part = line.split("#", 1)[0].strip()
            parts = re.split(r"\s+", main_part)

            if len(parts) < 2:
                return False  # Invalid line found
        return True
