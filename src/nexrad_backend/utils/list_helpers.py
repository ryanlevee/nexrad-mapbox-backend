import logging
from typing import Any, List, Optional, TypeVar

log = logging.getLogger(__name__)
T = TypeVar("T")  # Generic type variable for list elements


class Utl:
    """Utility class containing static methods for common operations."""

    @staticmethod
    def to_list(v: Any) -> list:
        """
        Ensures the input value is a list. If not, wraps it in a list.

        Args:
            v: The input value.

        Returns:
            A list containing the input value(s).
        """
        return [v] if not isinstance(v, list) else v

    @staticmethod
    def flatten_list(
        nested: Any, flat: Optional[List[Any]] = None, remove_falsey: bool = False
    ) -> List[Any]:
        """
        Recursively flattens a nested list structure.

        Args:
            nested: The potentially nested list or item to flatten.
            flat: Internal accumulator for the flattened list (should usually be None initially).
            remove_falsey: If True, removes items that evaluate to False (e.g., None, False, 0, "").

        Returns:
            A single flat list containing all non-list elements from the nested structure.
        """
        # Handle mutable default argument properly
        if flat is None:
            flat = []

        if isinstance(nested, list):
            for item in nested:
                # Recursively call flatten_list for each item in the list
                Utl.flatten_list(item, flat, remove_falsey)
        else:
            # Base case: item is not a list
            if remove_falsey:
                if nested:  # Only append if item is truthy
                    flat.append(nested)
            else:
                # Always append if not removing falsey values
                flat.append(nested)
        return flat

    @staticmethod
    def split_list(list_to_split: List[T], chunk_size: int) -> List[List[T]]:
        """
        Splits a list into smaller sublists (chunks) of a specified maximum size.

        Args:
            list_to_split: The list to be split.
            chunk_size: The maximum size of each chunk.

        Returns:
            A list of lists, where each inner list is a chunk of the original list.
            Returns an empty list if chunk_size is not positive.
        """
        if chunk_size <= 0:
            log.warning("chunk_size must be positive for split_list.")
            return []
        if not list_to_split:
            return []  # Return empty list if input is empty

        chunked_list = []
        for i in range(0, len(list_to_split), chunk_size):
            chunk = list_to_split[i : i + chunk_size]
            chunked_list.append(chunk)

        return chunked_list


# Example usage (optional, for testing)
if __name__ == "__main__":
    nested = [1, [2, 3, None], [4, [5, False, 6]], 7, []]
    flat_all = Utl.flatten_list(nested)
    flat_truthy = Utl.flatten_list(nested, remove_falsey=True)
    print(f"Original: {nested}")
    print(f"Flattened (all): {flat_all}")  # Output: [1, 2, 3, None, 4, 5, False, 6, 7]
    print(f"Flattened (truthy): {flat_truthy}")  # Output: [1, 2, 3, 4, 5, 6, 7]

    my_list = list(range(15))
    split_5 = Utl.split_list(my_list, 5)
    split_4 = Utl.split_list(my_list, 4)
    split_0 = Utl.split_list(my_list, 0)
    print(f"\nOriginal List: {my_list}")
    print(
        f"Split by 5: {split_5}"
    )  # Output: [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9], [10, 11, 12, 13, 14]]
    print(
        f"Split by 4: {split_4}"
    )  # Output: [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11], [12, 13, 14]]
    print(f"Split by 0: {split_0}")  # Output: []
