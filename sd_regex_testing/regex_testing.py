"""Provides functions for testing regex against metasmoke JSON dumps, as well
as for filtering the results by whether or not they were successful.
"""

from io import IOBase
from pathlib import Path
import polars as pl

FIELDS = ('title', 'username', 'body', 'is_tp')
TEXT_FIELDS = ('title', 'username', 'body')

def read_json(source: str | Path | IOBase | bytes) -> pl.DataFrame:
    """Read a JSON file into a Polars dataframe for use in regex tests. Used
    near-identically to ``polars.read_json``, save for some extra filtering.
    Intended for use with metasmoke JSON exports; follows the convention of
    giving each post its own row and each field its own column.
    
    To save space, only loads the fields defined in the FIELDS constant. Also
    removes any posts with either no feedback or conflicting feedback to ensure
    integrity of regex tests.
    
    :param source: the JSON file to read.
    :type source: str | Path | IOBase | bytes
    :return: a DataFrame containing regex-relevant information about each post.
    :rtype: pl.DataFrame
    """
    
    posts = pl.read_json(source)
    # Disputed and unreviewed should be excluded
    posts = posts.filter(pl.col('is_tp').xor('is_fp'))
    return posts.select(FIELDS).with_columns(matched=False)

class SDRegexTestingFrame:
    """Defines methods useful for regex testing. All of these methods are
    registered in the ``df.sdrt`` namespace; do not try to access them by
    directly instantiating an object of this type.
    
    :param df: the underlying DataFrame for this class.
    :type df: pl.DataFrame
    """
    
    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df
    
    def test_title(self, regex: str, case_sensitive: bool = False) -> pl.DataFrame:
        """Test a regex against each post's title. Returns a new DataFrame with
        a "matched" column indicating whether or not a match was found.
        
        :param regex: the regex to test.
        :param case_sensitive: whether the regex should be case-sensitive.
        Default False.
        :type regex: str
        :return: a DataFrame with a "matched" column, where True indicates that
        a match was found.
        :rtype: pl.DataFrame
        """
        
        if not case_sensitive:
            regex = "(?i)" + regex
        return self._df.with_columns(
            matched=pl.col('title').str.contains(regex)
        )
    
    def test_username(self, regex: str, case_sensitive: bool = False) -> pl.DataFrame:
        """Test a regex against each post's username. Returns a new DataFrame
        with a "matched" column indicating whether or not a match was found.
        
        :param regex: the regex to test.
        :param case_sensitive: whether the regex should be case-sensitive.
        Default False.
        :type regex: str
        :return: a DataFrame with a "matched" column, where True indicates that
        a match was found.
        :rtype: pl.DataFrame
        """
        
        if not case_sensitive:
            regex = "(?i)" + regex
        return self._df.with_columns(
            matched=pl.col('username').str.contains(regex)
        )
    
    def test_keyword(self, regex: str, case_sensitive: bool = False) -> pl.DataFrame:
        """Test a regex as if it were part of the keyword blacklist. Returns a
        new DataFrame with a "matched" column indicating whether or not a match
        was found. Checks for matches in every field in the TEXT_FIELDS
        constant. Bookended by ``'\\b'`` on both sides.
        
        :param regex: the regex to test.
        :param case_sensitive: whether the regex should be case-sensitive.
        Default False.
        :type regex: str
        :return: a DataFrame with a "matched" column, where True indicates that
        a match was found.
        :rtype: pl.DataFrame
        """
    
        regex = r"\b" + regex + r"\b"
        if not case_sensitive:
            regex = r"(?i)" + regex
        return self._df.with_columns(
            matched=pl.any_horizontal(
                pl.col(field).str.contains(regex) for field in TEXT_FIELDS
            )
        )
    
    def test_website(self, regex: str, case_sensitive: bool = False) -> pl.DataFrame:
        """Test a regex as if it were part of the website blacklist. Returns a
        new DataFrame with a "matched" column indicating whether or not a match
        was found. Checks for matches in every field in the TEXT_FIELDS
        constant. Case-insensitive.
        
        :param regex: the regex to test.
        :param case_sensitive: whether the regex should be case-sensitive.
        Default False.
        :type regex: str
        :return: a DataFrame with a "matched" column, where True indicates that
        a match was found.
        :rtype: pl.DataFrame
        """
        if not case_sensitive:
            regex = r"(?i)" + regex
        return self._df.with_columns(
            matched=pl.any_horizontal(
                pl.col(field).str.contains(regex) for field in TEXT_FIELDS
            )
        )
    
    @property
    def tp(self) -> pl.DataFrame:
        """All of the posts that were true positives given the regex test. A
        post is true positive if it was TP on SmokeDetector and matched by the
        regex.
    
        :return: a DataFrame containing only the true-positive posts.
        :rtype: pl.DataFrame
        """
        return self._df.filter(pl.col('is_tp') & pl.col('matched'))
    
    @property
    def fp(self) -> pl.DataFrame:
        """All of the posts that were false positives given the regex test. A
        post is false positive if it was FP on SmokeDetector but matched by the
        regex.
    
        :return: a DataFrame containing only the false-positive posts.
        :rtype: pl.DataFrame
        """
        return self._df.filter(~pl.col('is_tp') & pl.col('matched'))
    
    @property
    def tn(self) -> pl.DataFrame:
        """All of the posts that were true negatives given the regex test. A
        post is true negative if it was FP on SmokeDetector and not matched by
        the regex.
    
        :return: a DataFrame containing only the true-negative posts.
        :rtype: pl.DataFrame
        """
        return self._df.filter(~pl.col('is_tp') & ~pl.col('matched'))
    
    @property
    def fn(self) -> pl.DataFrame:
        """All of the posts that were false negatives given the regex test. A
        post is false negative if it was TP on SmokeDetector but not matched by
        the regex.
    
        :return: a DataFrame containing only the false-negative posts.
        :rtype: pl.DataFrame
        """
        return self._df.filter(pl.col('is_tp') & ~pl.col('matched'))

pl.api.register_dataframe_namespace("sdrt")(SDRegexTestingFrame)
pl.api.register_lazyframe_namespace("sdrt")(SDRegexTestingFrame)
